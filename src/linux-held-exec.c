#define _GNU_SOURCE
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/ptrace.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/user.h>
#include <sys/wait.h>
#include <unistd.h>

static const long options = PTRACE_O_TRACEFORK | PTRACE_O_TRACEVFORK |
	PTRACE_O_TRACEVFORKDONE | PTRACE_O_TRACECLONE | PTRACE_O_TRACEEXEC;

#define MAX_LINE 1024
#define MAX_OUTPUT_EVENTS 65536
#define MAX_OUTPUT_BYTES (512UL * 1024 * 1024)

struct output_event {
	unsigned fd;
	size_t length;
	unsigned char *data;
};

struct completion_observer {
	pid_t pid;
	int fd;
	struct completion_observer *next;
};

static int replace_with_exit(pid_t pid, unsigned code) {
#if defined(__x86_64__)
	struct user_regs_struct registers;
	if (ptrace(PTRACE_GETREGS, pid, 0, &registers) < 0) return -1;
	unsigned char stub[16] = {
		0xbf, code, code >> 8, code >> 16, code >> 24,
		0xb8, 0xe7, 0, 0, 0, 0x0f, 0x05,
	};
	for (size_t offset = 0; offset < sizeof(stub); offset += sizeof(long)) {
		long word;
		memcpy(&word, stub + offset, sizeof(word));
		if (ptrace(PTRACE_POKETEXT, pid, registers.rip + offset, word) < 0) return -1;
	}
	return 0;
#else
	(void)pid; (void)code; errno = ENOTSUP; return -1;
#endif
}

static int transfer(int fd, void *buffer, size_t length, int writing) {
	unsigned char *cursor = buffer;
	while (length) {
		ssize_t moved = writing ? write(fd, cursor, length) : read(fd, cursor, length);
		if (moved < 0 && errno == EINTR) continue;
		if (moved <= 0) return -1;
		cursor += moved; length -= (size_t)moved;
	}
	return 0;
}

static int read_line(int fd, char *buffer, size_t capacity) {
	size_t length = 0;
	while (length + 1 < capacity) {
		char byte;
		if (transfer(fd, &byte, 1, 0) < 0) return -1;
		if (byte == '\n') { buffer[length] = 0; return 0; }
		buffer[length++] = byte;
	}
	errno = EMSGSIZE;
	return -1;
}

static void free_events(struct output_event *events, unsigned count) {
	if (!events) return;
	for (unsigned index = 0; index < count; index++) free(events[index].data);
	free(events);
}

static char *take_env(const char *name) {
	char *value = getenv(name);
	char *copy = value ? strdup(value) : NULL;
	if (value) explicit_bzero(value, strlen(value));
	unsetenv(name);
	return copy;
}

static int has_extra_descriptors(int ignored) {
	DIR *directory = opendir("/proc/self/fd");
	if (!directory) return -1;
	int scan_fd = dirfd(directory), found = 0, saved = 0;
	struct dirent *entry;
	for (;;) {
		errno = 0;
		entry = readdir(directory);
		if (!entry) { saved = errno; break; }
		char *end;
		long fd = strtol(entry->d_name, &end, 10);
		if (!*entry->d_name || *end || fd <= 2 || fd == scan_fd || fd == ignored) continue;
		errno = 0;
		if (fcntl((int)fd, F_GETFD) >= 0) { found = 1; break; }
		if (errno != EBADF) { saved = errno; break; }
	}
	closedir(directory);
	if (saved) { errno = saved; return -1; }
	return found;
}

static int mapped_image(char *image, size_t capacity) {
	FILE *maps = fopen("/proc/self/maps", "re");
	char *line = NULL;
	size_t line_capacity = 0;
	int result = -1;
	while (maps && getline(&line, &line_capacity, maps) >= 0) {
		if (!strstr(line, " r-xp ")) continue;
		char *mapped = strchr(line, '/');
		if (!mapped) continue;
		mapped[strcspn(mapped, "\r\n")] = 0;
		char *deleted = strstr(mapped, " (deleted)");
		if (deleted) *deleted = 0;
		if (snprintf(image, capacity, "%s", mapped) >= (int)capacity) errno = ENAMETOOLONG;
		else result = 0;
		break;
	}
	if (maps) fclose(maps);
	free(line);
	return result;
}

/* An exec-only hardlink retains the target's argv[0]; its read-only sidecar supplies routing. */
static int image_dispatch(int argc, char **argv) {
	char image[PATH_MAX], sidecar[PATH_MAX], invoked[PATH_MAX], native[PATH_MAX];
	if (mapped_image(image, sizeof(image)) < 0) return -1;
	char *separator = strrchr(image, '/');
	if (!separator || !separator[1]) return -1;
	char *name = separator + 1;
	*separator = 0;
	if (snprintf(sidecar, sizeof(sidecar), "%s/.pi-spec-dispatch-v1", image) >= (int)sizeof(sidecar)) return 70;
	FILE *file = fopen(sidecar, "re");
	if (!file) return errno == ENOENT ? -1 : 70;
	struct stat state;
	char *line = NULL, *fields[5] = {0};
	size_t capacity = 0;
	int result = 70;
	if (fstat(fileno(file), &state) < 0 || !S_ISREG(state.st_mode) || state.st_uid != geteuid() || (state.st_mode & 022)) goto done;
	for (unsigned index = 0; index < 6; index++) {
		if (getline(&line, &capacity, file) < 0) goto done;
		line[strcspn(line, "\r\n")] = 0;
		if (index == 0) {
			if (strcmp(line, "PI_SPEC_DISPATCH_V1")) goto done;
		} else if (*line != '/' || !(fields[index - 1] = strdup(line))) goto done;
	}
	fclose(file); file = NULL;
	if (snprintf(invoked, sizeof(invoked), "%s/%s", fields[3], name) >= (int)sizeof(invoked) ||
		snprintf(native, sizeof(native), "%s/%s", fields[4], name) >= (int)sizeof(native)) goto done;
	int extra = has_extra_descriptors(-1);
	if (extra < 0) goto done;
	if (extra) {
		execv(native, argv);
		result = errno == ENOENT ? 127 : 126;
		goto done;
	}
	char **command = calloc((size_t)argc + 6, sizeof(*command));
	if (!command) goto done;
	command[0] = fields[0];
	command[1] = fields[1];
	command[2] = "--native-dispatch";
	command[3] = fields[2];
	command[4] = invoked;
	command[5] = argv[0];
	for (int index = 1; index < argc; index++) command[index + 5] = argv[index];
	execv(command[0], command);
	result = errno == ENOENT ? 127 : 126;
	free(command);
done:
	if (file) fclose(file);
	free(line);
	for (unsigned index = 0; index < 5; index++) free(fields[index]);
	return result;
}

static int open_tracee_output(pid_t pid, unsigned fd) {
	#if defined(SYS_pidfd_open) && defined(SYS_pidfd_getfd)
	int pidfd = (int)syscall(SYS_pidfd_open, pid, 0);
	if (pidfd >= 0) {
		int duplicate = (int)syscall(SYS_pidfd_getfd, pidfd, fd, 0);
		int saved = errno;
		close(pidfd);
		if (duplicate >= 0) {
			int flags = fcntl(duplicate, F_GETFD);
			if (flags >= 0 && fcntl(duplicate, F_SETFD, flags | FD_CLOEXEC) >= 0) return duplicate;
			close(duplicate);
		} else errno = saved;
	}
	#endif
	char path[64];
	if (snprintf(path, sizeof(path), "/proc/%ld/fd/%u", (long)pid, fd) >= (int)sizeof(path)) {
		errno = ENAMETOOLONG;
		return -1;
	}
	return open(path, O_WRONLY | O_CLOEXEC);
}

/* A nonnegative return keeps the connection until the continued tracee exits. */
static int actor_decision(pid_t pid, const char *socket_path, const char *token, const char *execution_id) {
	int connection = -1, outputs[3] = {-1, -1, -1}, observer = -1;
	struct output_event *events = NULL;
	unsigned count = 0, code = 125;
	size_t total = 0;
	char line[MAX_LINE];
	if (strlen(socket_path) >= sizeof(((struct sockaddr_un *)0)->sun_path)) return -1;
	connection = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
	if (connection < 0) return -1;
	struct sockaddr_un address = {.sun_family = AF_UNIX};
	strcpy(address.sun_path, socket_path);
	if (connect(connection, (struct sockaddr *)&address, sizeof(address)) < 0) goto done;
	int request_length = snprintf(line, sizeof(line),
		"{\"version\":1,\"token\":\"%s\",\"execution\":\"%s\",\"pid\":%ld,\"tracer\":%ld}\n",
		token, execution_id, (long)pid, (long)getpid());
	if (request_length < 0 || request_length >= (int)sizeof(line) ||
		transfer(connection, line, (size_t)request_length, 1) < 0 || read_line(connection, line, sizeof(line)) < 0) goto done;
	if (!strcmp(line, "C")) goto done;
	if (!strcmp(line, "O")) { observer = connection; connection = -1; goto done; }
	if (sscanf(line, "P %u %u %zu", &code, &count, &total) != 3 || code > 255 ||
		count > MAX_OUTPUT_EVENTS || total > MAX_OUTPUT_BYTES) goto done;
	events = calloc(count ? count : 1, sizeof(*events));
	if (!events) goto done;
	size_t received = 0;
	for (unsigned index = 0; index < count; index++) {
		size_t length;
		unsigned fd;
		if (read_line(connection, line, sizeof(line)) < 0 || sscanf(line, "O %u %zu", &fd, &length) != 2 ||
			(fd != 1 && fd != 2) || length > total - received) goto done;
		events[index].fd = fd;
		events[index].length = length;
		if (outputs[fd] < 0 && (outputs[fd] = open_tracee_output(pid, fd)) < 0) goto done;
		if (length && (!(events[index].data = malloc(length)) || transfer(connection, events[index].data, length, 0) < 0)) goto done;
		received += length;
	}
	if (received != total || replace_with_exit(pid, 125) < 0) goto done;
	if (transfer(connection, "A\n", 2, 1) < 0 || read_line(connection, line, sizeof(line)) < 0 || strcmp(line, "R")) goto done;
	for (unsigned index = 0; index < count; index++) {
		if (transfer(outputs[events[index].fd], events[index].data, events[index].length, 1) < 0) goto done;
	}
	if (replace_with_exit(pid, code) < 0) goto done;
done:
	if (connection >= 0) close(connection);
	for (unsigned fd = 1; fd <= 2; fd++) if (outputs[fd] >= 0) close(outputs[fd]);
	free_events(events, count);
	return observer;
}

static void observe_completion(struct completion_observer **observers, pid_t pid, int fd) {
	struct completion_observer *observer = malloc(sizeof(*observer));
	if (!observer) { close(fd); return; }
	*observer = (struct completion_observer){.pid = pid, .fd = fd, .next = *observers};
	*observers = observer;
}

static void complete_observers(struct completion_observer **observers, pid_t pid) {
	struct completion_observer **cursor = observers;
	while (*cursor) {
		struct completion_observer *observer = *cursor;
		if (observer->pid != pid) { cursor = &observer->next; continue; }
		*cursor = observer->next;
		size_t sent = 0;
		while (sent < 2) {
			ssize_t moved = send(observer->fd, "D\n" + sent, 2 - sent, MSG_NOSIGNAL);
			if (moved < 0 && errno == EINTR) continue;
			if (moved <= 0) break;
			sent += (size_t)moved;
		}
		close(observer->fd);
		free(observer);
	}
}

static int trace(char **command, const char *socket_path, const char *token, const char *execution_id,
	int skip, unsigned skip_code) {
	int gate[2];
	if (pipe2(gate, O_CLOEXEC) < 0) return 70;
	pid_t root = fork();
	if (root < 0) return 70;
	if (root == 0) {
		char ready;
		close(gate[1]);
		if (transfer(gate[0], &ready, 1, 0) < 0) _exit(71);
		close(gate[0]);
		execvp(command[0], command);
		_exit(errno == ENOENT ? 127 : 126);
	}
	close(gate[0]);
	if (ptrace(PTRACE_SEIZE, root, 0, options) < 0 || transfer(gate[1], "R", 1, 1) < 0) {
		close(gate[1]);
		kill(root, SIGKILL);
		waitpid(root, NULL, 0);
		return 72;
	}
	close(gate[1]);
	int status = 0;
	unsigned exec_events = 0;
	struct completion_observer *observers = NULL;
	for (;;) {
		pid_t pid = waitpid(-1, &status, __WALL);
		if (pid < 0) {
			if (errno == EINTR) continue;
			if (errno == ECHILD) return 125;
			return 74;
		}
		if (WIFEXITED(status) || WIFSIGNALED(status)) {
			complete_observers(&observers, pid);
			if (pid != root) continue;
			if (WIFEXITED(status)) return WEXITSTATUS(status);
			signal(WTERMSIG(status), SIG_DFL);
			raise(WTERMSIG(status));
			return 128 + WTERMSIG(status);
		}
		if (!WIFSTOPPED(status)) continue;
		unsigned event = (unsigned)status >> 16;
		int delivered = WSTOPSIG(status);
		if (event == PTRACE_EVENT_STOP && delivered != SIGTRAP) {
			if (ptrace(PTRACE_LISTEN, pid, 0, 0) < 0 && errno != ESRCH) return 75;
			continue;
		}
		if (event == PTRACE_EVENT_EXEC && ++exec_events > 1) {
			if (skip && replace_with_exit(pid, skip_code) < 0) return 76;
			if (socket_path) {
				int observer = actor_decision(pid, socket_path, token, execution_id);
				if (observer >= 0) observe_completion(&observers, pid, observer);
			}
		}
		if (event != 0) delivered = 0;
		if (ptrace(PTRACE_CONT, pid, 0, delivered) < 0 && errno != ESRCH) return 75;
	}
}

int main(int argc, char **argv) {
	int dispatched = image_dispatch(argc, argv);
	if (dispatched >= 0) return dispatched;
	if (argc == 2 && !strcmp(argv[1], "--protocol-version")) {
		puts("3");
		return 0;
	}
	if (argc == 2 && !strcmp(argv[1], "--probe-clean-fds")) {
		int extra = has_extra_descriptors(-1);
		return extra < 0 ? 70 : extra ? 65 : 0;
	}
	if (getenv("PI_SPEC_HELD_EXEC_SHELL")) {
		char *real_shell = take_env("PI_SPEC_HELD_EXEC_SHELL");
		char *socket_path = take_env("PI_SPEC_HELD_EXEC_SOCKET");
		char *token = take_env("PI_SPEC_HELD_EXEC_TOKEN");
		char *execution_id = take_env("PI_SPEC_HELD_EXEC_ID");
		if (!real_shell) return 70;
		char **command = calloc((size_t)argc + 1, sizeof(*command));
		if (!command) return 70;
		command[0] = real_shell;
		for (int index = 1; index < argc; index++) command[index] = argv[index];
		if (!socket_path || !token || !execution_id) {
			execvp(real_shell, command);
			return errno == ENOENT ? 127 : 126;
		}
		return trace(command, socket_path, token, execution_id, 0, 0);
	}
	if (argc < 2) return 64;
	int command = 1, skip = 0;
	unsigned skip_code = 0;
	if (argc >= 4 && !strcmp(argv[1], "--skip-code")) {
		skip = 1; skip_code = (unsigned)strtoul(argv[2], 0, 10); command = 3;
		if (skip_code > 255) return 64;
	}
	return trace(argv + command, NULL, NULL, NULL, skip, skip_code);
}
