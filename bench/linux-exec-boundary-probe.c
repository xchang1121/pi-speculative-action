#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ptrace.h>
#include <sys/types.h>
#include <sys/user.h>
#include <sys/wait.h>
#include <unistd.h>

static const long options = PTRACE_O_TRACEFORK | PTRACE_O_TRACEVFORK |
	PTRACE_O_TRACECLONE | PTRACE_O_TRACEEXEC | PTRACE_O_EXITKILL;

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

int main(int argc, char **argv) {
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
	pid_t root = fork();
	if (root < 0) return 70;
	if (root == 0) {
		if (ptrace(PTRACE_TRACEME, 0, 0, 0) < 0) _exit(71);
		raise(SIGSTOP);
		execvp(argv[command], argv + command);
		_exit(errno == ENOENT ? 127 : 126);
	}
	int status = 0, root_status = 125 << 8;
	unsigned exec_events = 0;
	if (waitpid(root, &status, 0) != root || !WIFSTOPPED(status)) return 72;
	if (ptrace(PTRACE_SETOPTIONS, root, 0, options) < 0 || ptrace(PTRACE_CONT, root, 0, 0) < 0) return 73;
	for (;;) {
		pid_t pid = waitpid(-1, &status, __WALL);
		if (pid < 0) {
			if (errno == EINTR) continue;
			if (errno == ECHILD) break;
			return 74;
		}
		if (WIFEXITED(status) || WIFSIGNALED(status)) {
			if (pid == root) root_status = status;
			continue;
		}
		if (!WIFSTOPPED(status)) continue;
		unsigned event = (unsigned)status >> 16;
		int delivered = WSTOPSIG(status);
		if (event == PTRACE_EVENT_EXEC && ++exec_events > 1) {
			if (skip && replace_with_exit(pid, skip_code) < 0) return 76;
			if (broker) {
				unsigned char request = 'E', response[2];
				if (transfer(3, &request, 1, 1) < 0 || transfer(4, response, sizeof(response), 0) < 0)
					return 77;
				if (response[0] == 'S') {
					if (replace_with_exit(pid, response[1]) < 0) return 76;
				} else if (response[0] != 'C') return 78;
			}
		}
		if (event != 0 || delivered == SIGSTOP) delivered = 0;
		if (ptrace(PTRACE_CONT, pid, 0, delivered) < 0 && errno != ESRCH) return 75;
	}
	if (WIFEXITED(root_status)) return WEXITSTATUS(root_status);
	if (WIFSIGNALED(root_status)) {
		signal(WTERMSIG(root_status), SIG_DFL);
		raise(WTERMSIG(root_status));
	}
	return 125;
}
