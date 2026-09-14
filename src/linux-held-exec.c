#define _GNU_SOURCE
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/signalfd.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/ptrace.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/user.h>
#include <sys/wait.h>
#include <unistd.h>

static const long options = PTRACE_O_TRACEFORK | PTRACE_O_TRACEVFORK |
	PTRACE_O_TRACEVFORKDONE | PTRACE_O_TRACECLONE | PTRACE_O_TRACEEXEC | PTRACE_O_EXITKILL;

#define MAX_LINE 1024
#define MAX_OUTPUT_EVENTS 65536
#define MAX_OUTPUT_BYTES (512UL * 1024 * 1024)

struct output_event {
	unsigned fd;
	size_t length;
	unsigned char *data;
};

struct decision_job {
	pthread_t thread;
	pid_t pid;
	const char *socket_path, *token, *execution_id;
	int channel[2], connection, outputs[3], result;
	struct output_event *events;
	unsigned count;
};

struct traced_process {
	pid_t pid;
	int fd, armed;
	struct decision_job *job;
	struct traced_process *next;
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
	/* This fallback opens a new description; never change flags on a pidfd duplicate. */
	int output = open(path, O_WRONLY | O_CLOEXEC | O_NONBLOCK);
	if (output < 0) return -1;
	int flags = fcntl(output, F_GETFL);
	if (flags >= 0 && fcntl(output, F_SETFL, flags & ~O_NONBLOCK) >= 0) return output;
	close(output);
	return -1;
}

/* Only the tracer thread may mutate a held image. Each job owns its reply channel. */
static int request_exit(struct decision_job *job, unsigned code) {
	char reply;
	return transfer(job->channel[1], &code, sizeof(code), 1) < 0 ||
		transfer(job->channel[1], &reply, 1, 0) < 0 || reply != 'Y' ? -1 : 0;
}

/* A nonnegative return keeps the connection until the continued tracee exits. */
static int actor_decision(struct decision_job *job) {
	unsigned code = 125;
	size_t total = 0;
	char line[MAX_LINE];
	if (strlen(job->socket_path) >= sizeof(((struct sockaddr_un *)0)->sun_path)) return -1;
	int connection = job->connection = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
	if (connection < 0) return -1;
	struct sockaddr_un address = {.sun_family = AF_UNIX};
	strcpy(address.sun_path, job->socket_path);
	if (connect(connection, (struct sockaddr *)&address, sizeof(address)) < 0) return -1;
	int request_length = snprintf(line, sizeof(line),
		"{\"version\":1,\"token\":\"%s\",\"execution\":\"%s\",\"pid\":%ld,\"tracer\":%ld}\n",
		job->token, job->execution_id, (long)job->pid, (long)getpid());
	if (request_length < 0 || request_length >= (int)sizeof(line) ||
		transfer(connection, line, (size_t)request_length, 1) < 0 || read_line(connection, line, sizeof(line)) < 0) return -1;
	if (!strcmp(line, "C")) return -1;
	if (!strcmp(line, "F")) return -2;
	if (!strcmp(line, "O")) return connection;
	if (sscanf(line, "P %u %u %zu", &code, &job->count, &total) != 3 || code > 255 ||
		job->count > MAX_OUTPUT_EVENTS || total > MAX_OUTPUT_BYTES) return -1;
	job->events = calloc(job->count ? job->count : 1, sizeof(*job->events));
	if (!job->events) return -1;
	size_t received = 0;
	for (unsigned index = 0; index < job->count; index++) {
		size_t length;
		unsigned fd;
		if (read_line(connection, line, sizeof(line)) < 0 || sscanf(line, "O %u %zu", &fd, &length) != 2 ||
			(fd != 1 && fd != 2) || length > total - received) return -1;
		struct output_event *event = &job->events[index];
		event->fd = fd;
		event->length = length;
		if (job->outputs[fd] < 0) {
			/* Publish acquired descriptors before a cancellation point can retire the job. */
			pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
			job->outputs[fd] = open_tracee_output(job->pid, fd);
			pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
			if (job->outputs[fd] < 0) return -1;
		}
		if (length && (!(event->data = malloc(length)) || transfer(connection, event->data, length, 0) < 0)) return -1;
		received += length;
	}
	if (received != total) return -1;
	/* From the first text mutation onward, failure terminates the entire trace tree. */
	if (request_exit(job, 125) < 0) return -2;
	if (transfer(connection, "A\n", 2, 1) < 0 || read_line(connection, line, sizeof(line)) < 0 || strcmp(line, "R")) return -2;
	for (unsigned index = 0; index < job->count; index++) {
		struct output_event *event = &job->events[index];
		if (transfer(job->outputs[event->fd], event->data, event->length, 1) < 0) return -2;
	}
	if (request_exit(job, code) < 0 || transfer(connection, "D\n", 2, 1) < 0) return -2;
	return -1;
}

static void *decide_process(void *argument) {
	struct decision_job *job = argument;
	sigset_t blocked;
	sigemptyset(&blocked); sigaddset(&blocked, SIGPIPE);
	pthread_sigmask(SIG_BLOCK, &blocked, NULL);
	job->result = actor_decision(job);
	shutdown(job->channel[1], SHUT_WR);
	return NULL;
}

static void free_job(struct decision_job *job) {
	close(job->channel[0]); close(job->channel[1]); close(job->connection);
	for (unsigned fd = 1; fd <= 2; fd++) close(job->outputs[fd]);
	free_events(job->events, job->count);
	free(job);
}

static int start_decision(struct traced_process *process, const char *socket_path,
	const char *token, const char *execution_id) {
	struct decision_job *job = calloc(1, sizeof(*job));
	if (!job) return -1;
	*job = (struct decision_job){ .pid = process->pid, .socket_path = socket_path,
		.token = token, .execution_id = execution_id, .channel = {-1, -1},
		.connection = -1, .outputs = {-1, -1, -1}, .result = -1 };
	if (socketpair(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0, job->channel) < 0 ||
		pthread_create(&job->thread, NULL, decide_process, job) != 0) {
		free_job(job);
		return -1;
	}
	process->job = job;
	return 0;
}

static int track_process(struct traced_process **processes, pid_t pid) {
	struct traced_process *observer = malloc(sizeof(*observer));
	if (!observer) return -1;
	*observer = (struct traced_process){.pid = pid, .fd = -1, .next = *processes};
	*processes = observer;
	return 0;
}

static void release_process(struct traced_process **processes, pid_t pid) {
	struct traced_process **cursor = processes;
	while (*cursor) {
		struct traced_process *observer = *cursor;
		if (observer->pid != pid) { cursor = &observer->next; continue; }
		*cursor = observer->next;
		if (observer->job) {
			pthread_cancel(observer->job->thread);
			pthread_join(observer->job->thread, NULL);
			free_job(observer->job);
		}
		size_t sent = 0;
		while (observer->fd >= 0 && sent < 2) {
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
	sigset_t blocked, original;
	sigemptyset(&blocked); sigaddset(&blocked, SIGCHLD);
	if (pthread_sigmask(SIG_BLOCK, &blocked, &original) != 0) return 70;
	int signals = signalfd(-1, &blocked, SFD_CLOEXEC | SFD_NONBLOCK);
	if (signals < 0) return 70;
	int gate[2];
	if (pipe2(gate, O_CLOEXEC) < 0) return 70;
	pid_t root = fork();
	if (root < 0) return 70;
	if (root == 0) {
		char ready;
		close(gate[1]);
		if (transfer(gate[0], &ready, 1, 0) < 0) _exit(71);
		close(gate[0]);
		if (pthread_sigmask(SIG_SETMASK, &original, NULL) != 0) _exit(71);
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
	int status = 0, root_status = -1;
	unsigned exec_events = 0;
	struct traced_process *processes = NULL;
	struct pollfd *polling = NULL;
	size_t capacity = 0;
	if (track_process(&processes, root) < 0) { kill(root, SIGKILL); goto fatal; }
	for (;;) {
		struct signalfd_siginfo notification;
		while (read(signals, &notification, sizeof(notification)) > 0) {}
		size_t count = 1;
		for (struct traced_process *item = processes; item; item = item->next) {
			struct decision_job *job = item->job;
			if (!job) continue;
			unsigned code;
			ssize_t received = recv(job->channel[0], &code, sizeof(code), MSG_DONTWAIT);
			if (received == sizeof(code)) {
				item->armed = 1;
				char reply = replace_with_exit(item->pid, code) < 0 ? 'N' : 'Y';
				if (send(job->channel[0], &reply, 1, MSG_NOSIGNAL) != 1) goto fatal;
			} else if (received == 0) {
				pthread_join(job->thread, NULL);
				int result = job->result;
				if (result >= 0) { close(item->fd); item->fd = result; job->connection = -1; }
				free_job(job); item->job = NULL; item->armed = 0;
				if (result == -2 || (ptrace(PTRACE_CONT, item->pid, 0, 0) < 0 && errno != ESRCH)) goto fatal;
				continue;
			} else if (received > 0 || (errno != EAGAIN && errno != EINTR)) goto fatal;
			count++;
		}
		pid_t pid = waitpid(-1, &status, __WALL | WNOHANG);
		if (pid < 0) {
			if (errno == EINTR) continue;
			if (errno == ECHILD) break;
			goto fatal;
		}
		if (pid == 0) {
			if (count > capacity) {
				struct pollfd *grown = realloc(polling, count * sizeof(*polling));
				if (!grown) goto fatal;
				polling = grown; capacity = count;
			}
			polling[0] = (struct pollfd){.fd = signals, .events = POLLIN};
			count = 1;
			for (struct traced_process *item = processes; item; item = item->next)
				if (item->job) polling[count++] = (struct pollfd){.fd = item->job->channel[0], .events = POLLIN};
			if (poll(polling, count, -1) < 0 && errno != EINTR) goto fatal;
			continue;
		}
		if (WIFEXITED(status) || WIFSIGNALED(status)) {
			if (pid == root) root_status = status;
			for (struct traced_process *item = processes; item; item = item->next)
				if (item->pid == pid && item->armed) goto fatal;
			release_process(&processes, pid);
			continue;
		}
		if (!WIFSTOPPED(status)) continue;
		unsigned event = (unsigned)status >> 16;
		int delivered = WSTOPSIG(status);
		if (event == PTRACE_EVENT_FORK || event == PTRACE_EVENT_VFORK || event == PTRACE_EVENT_CLONE) {
			unsigned long child;
			if (ptrace(PTRACE_GETEVENTMSG, pid, 0, &child) < 0 || track_process(&processes, (pid_t)child) < 0) goto fatal;
		}
		if (event == PTRACE_EVENT_STOP && delivered != SIGTRAP) {
			if (ptrace(PTRACE_LISTEN, pid, 0, 0) < 0 && errno != ESRCH) goto fatal;
			continue;
		}
		if (event == PTRACE_EVENT_EXEC && ++exec_events > 1) {
			if (skip && replace_with_exit(pid, skip_code) < 0) goto fatal;
			if (socket_path) {
				for (struct traced_process *item = processes; item; item = item->next)
					if (item->pid == pid && start_decision(item, socket_path, token, execution_id) == 0) goto held;
			}
		}
		if (event != 0) delivered = 0;
		if (ptrace(PTRACE_CONT, pid, 0, delivered) < 0 && errno != ESRCH) goto fatal;
	held:;
	}
	while (processes) release_process(&processes, processes->pid);
	free(polling); close(signals);
	pthread_sigmask(SIG_SETMASK, &original, NULL);
	if (root_status < 0) return 125;
	if (WIFEXITED(root_status)) return WEXITSTATUS(root_status);
	signal(WTERMSIG(root_status), SIG_DFL);
	raise(WTERMSIG(root_status));
	return 128 + WTERMSIG(root_status);
fatal:
	for (struct traced_process *item = processes; item; item = item->next) kill(item->pid, SIGKILL);
	while (waitpid(-1, &status, __WALL) > 0 || errno == EINTR) {}
	while (processes) release_process(&processes, processes->pid);
	free(polling); close(signals);
	return 125;
}

int main(int argc, char **argv) {
	int dispatched = image_dispatch(argc, argv);
	if (dispatched >= 0) return dispatched;
	if (argc == 2 && !strcmp(argv[1], "--protocol-version")) {
		puts("5");
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
