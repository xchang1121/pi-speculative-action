#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
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

/* Return 1 only after the tracee has irreversibly become a replay stub. */
static int actor_decision(pid_t pid, const char *socket_path, const char *token, const char *execution_id) {
	int connection = -1, outputs[3] = {-1, -1, -1}, armed = 0;
	struct output_event *events = NULL;
	unsigned count = 0, code = 125;
	size_t total = 0;
	char line[MAX_LINE];
	if (strlen(socket_path) >= sizeof(((struct sockaddr_un *)0)->sun_path)) return 0;
	connection = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
	if (connection < 0) return 0;
	struct sockaddr_un address = {.sun_family = AF_UNIX};
	strcpy(address.sun_path, socket_path);
	if (connect(connection, (struct sockaddr *)&address, sizeof(address)) < 0) goto done;
	int request_length = snprintf(line, sizeof(line),
		"{\"version\":1,\"token\":\"%s\",\"execution\":\"%s\",\"pid\":%ld,\"tracer\":%ld}\n",
		token, execution_id, (long)pid, (long)getpid());
	if (request_length < 0 || request_length >= (int)sizeof(line) ||
		transfer(connection, line, (size_t)request_length, 1) < 0 || read_line(connection, line, sizeof(line)) < 0) goto done;
	if (!strcmp(line, "C")) goto done;
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
	armed = 1;
	if (transfer(connection, "A\n", 2, 1) < 0 || read_line(connection, line, sizeof(line)) < 0 || strcmp(line, "R")) goto done;
	for (unsigned index = 0; index < count; index++) {
		if (transfer(outputs[events[index].fd], events[index].data, events[index].length, 1) < 0) goto done;
	}
	if (replace_with_exit(pid, code) < 0) goto done;
done:
	if (connection >= 0) close(connection);
	for (unsigned fd = 1; fd <= 2; fd++) if (outputs[fd] >= 0) close(outputs[fd]);
	free_events(events, count);
	return armed;
}

static int trace(char **command, const char *socket_path, const char *token, const char *execution_id,
	int skip, unsigned skip_code, int broker) {
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
	for (;;) {
		pid_t pid = waitpid(-1, &status, __WALL);
		if (pid < 0) {
			if (errno == EINTR) continue;
			if (errno == ECHILD) return 125;
			return 74;
		}
		if (WIFEXITED(status) || WIFSIGNALED(status)) {
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
			if (broker) {
				unsigned char request = 'E', response[2];
				if (transfer(3, &request, 1, 1) < 0 || transfer(4, response, sizeof(response), 0) < 0) return 77;
				if (response[0] == 'S') {
					if (replace_with_exit(pid, response[1]) < 0) return 76;
				} else if (response[0] != 'C') return 78;
			}
			if (socket_path) (void)actor_decision(pid, socket_path, token, execution_id);
		}
		if (event != 0) delivered = 0;
		if (ptrace(PTRACE_CONT, pid, 0, delivered) < 0 && errno != ESRCH) return 75;
	}
}

int main(int argc, char **argv) {
	if (argc == 2 && !strcmp(argv[1], "--protocol-version")) {
		puts("1");
		return 0;
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
		return trace(command, socket_path, token, execution_id, 0, 0, 0);
	}
	if (argc < 2) return 64;
	int command = 1, skip = 0, broker = 0;
	unsigned skip_code = 0;
	if (argc >= 4 && !strcmp(argv[1], "--skip-code")) {
		skip = 1; skip_code = (unsigned)strtoul(argv[2], 0, 10); command = 3;
		if (skip_code > 255) return 64;
	} else if (argc >= 3 && !strcmp(argv[1], "--broker")) {
		broker = 1; command = 2;
		for (int fd = 3; fd <= 4; fd++) {
			int flags = fcntl(fd, F_GETFD);
			if (flags < 0 || fcntl(fd, F_SETFD, flags | FD_CLOEXEC) < 0) return 65;
		}
	}
	return trace(argv + command, NULL, NULL, NULL, skip, skip_code, broker);
}
