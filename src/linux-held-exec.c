#define _GNU_SOURCE
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <inttypes.h>
#include <linux/audit.h>
#include <linux/kcmp.h>
#include <sched.h>
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
#include <sys/sysmacros.h>
#include <sys/ptrace.h>
#include <sys/prctl.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/user.h>
#include <sys/wait.h>
#include <unistd.h>

static const long options = PTRACE_O_TRACEFORK | PTRACE_O_TRACEVFORK |
	PTRACE_O_TRACEVFORKDONE | PTRACE_O_TRACECLONE | PTRACE_O_TRACEEXEC | PTRACE_O_EXITKILL;

#define MAX_LINE 32768
#define MAX_OUTPUT_EVENTS 65536
#define MAX_OUTPUT_BYTES (512UL * 1024 * 1024)
#define MAX_POSITIONS 64

struct file_position {
	int descriptor, duplicate, writer, flags, after_flags, alias;
	uintmax_t device, inode;
	int64_t before, after;
	int64_t content_length;
	unsigned char *content;
};

struct descriptor_origin { int fd, cloexec; unsigned long id; };
struct descriptor_table {
	unsigned references, count, active;
	unsigned long epoch, generation;
	struct descriptor_origin entries[256];
};

/* Track provenance without keeping kernel handles alive beyond their native lifetime. */
struct descriptor_domain {
	int enabled, escaped;
	unsigned uncertain;
	unsigned long next;
};

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
	struct file_position *positions;
	unsigned position_count;
	struct descriptor_domain *domain;
	struct traced_process *process;
};

struct traced_process {
	pid_t pid;
	int fd, armed, stopped, pending, delivered, listening, historical, awaiting_parent;
	struct decision_job *job;
	struct traced_process *next;
	struct descriptor_table *table;
	unsigned long call_epoch, mutation_generation;
	int mutation, uncertain;
	long syscall;
	unsigned long arguments[3];
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

static int has_unmodeled_descriptors(void) {
	/* Node fills closed standard streams; bypass before it changes the inherited table. */
	for (int fd = 0; fd < 3; fd++) {
		if (fcntl(fd, F_GETFD) < 0) return errno == EBADF ? 1 : -1;
	}
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
		if (!*entry->d_name || *end || fd <= 2 || fd == scan_fd) continue;
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
	if (snprintf(sidecar, sizeof(sidecar), "%s/.pi-spec-dispatch", image) >= (int)sizeof(sidecar)) return 70;
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
			if (strcmp(line, "PI_SPEC_DISPATCH")) goto done;
		} else if (*line != '/' || !(fields[index - 1] = strdup(line))) goto done;
	}
	fclose(file); file = NULL;
	if (snprintf(invoked, sizeof(invoked), "%s/%s", fields[3], name) >= (int)sizeof(invoked) ||
		snprintf(native, sizeof(native), "%s/%s", fields[4], name) >= (int)sizeof(native)) goto done;
	int extra = has_unmodeled_descriptors();
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

static int duplicate_tracee_fd(pid_t pid, unsigned fd) {
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
	return -1;
}

static struct descriptor_origin descriptor_origin(struct traced_process *process, int fd) {
	for (unsigned index = 0; process->table && index < process->table->count; index++)
		if (process->table->entries[index].fd == fd) return process->table->entries[index];
	return (struct descriptor_origin){.fd = fd};
}

static void set_descriptor_origin(struct traced_process *process, struct descriptor_domain *domain,
	int fd, unsigned long id, int cloexec) {
	struct descriptor_table *table = process->table;
	for (unsigned index = 0; index < table->count; index++) if (table->entries[index].fd == fd) {
		table->entries[index] = table->entries[--table->count]; break;
	}
	if (!id) return;
	if (table->count == sizeof(table->entries) / sizeof(table->entries[0])) { domain->escaped = 1; return; }
	table->entries[table->count++] = (struct descriptor_origin){fd, cloexec, id};
}

static struct descriptor_table *copy_descriptor_table(struct traced_process *source) {
	struct descriptor_table *table = calloc(1, sizeof(*table));
	if (!table) return NULL;
	if (source && source->table && !source->table->active && source->call_epoch == source->table->epoch) {
		table->count = source->table->count;
		memcpy(table->entries, source->table->entries, table->count * sizeof(*table->entries));
	}
	table->references = 1; table->generation = 1;
	return table;
}

static void drop_descriptor_table(struct traced_process *process, struct descriptor_domain *domain) {
	if (process->uncertain) { domain->uncertain--; process->uncertain = 0; domain->escaped = 1; }
	struct descriptor_table *table = process->table;
	if (!table) return;
	if (process->mutation) { table->active--; table->count = 0; table->generation++; process->mutation = 0; }
	if (!--table->references) free(table);
	process->table = NULL;
}

static int detach_descriptor_table(struct traced_process *process, struct descriptor_domain *domain) {
	struct descriptor_table *old = process->table;
	if (!old || (old->references == 1 && !old->active && process->call_epoch == old->epoch)) return 0;
	struct descriptor_table *table = copy_descriptor_table(process);
	if (!table) return -1;
	drop_descriptor_table(process, domain); process->table = table;
	return 0;
}

/* Called at syscall stops, on the tracer thread. No tracing cost in the ordinary mode. */
static int observe_descriptor_syscall(struct traced_process *process, struct descriptor_domain *domain) {
	if (domain->escaped) return 0;
	pid_t pid = process->pid;
	struct __ptrace_syscall_info info;
	if (ptrace(PTRACE_GET_SYSCALL_INFO, pid, sizeof(info), &info) < 0) return -1;
	if (info.arch != AUDIT_ARCH_X86_64) { domain->escaped = 1; return 0; }
	if (!process->table && !(process->table = copy_descriptor_table(NULL))) return -1;
	struct descriptor_table *table = process->table;
	if (info.op == PTRACE_SYSCALL_INFO_ENTRY) {
		process->syscall = (long)info.entry.nr;
		for (unsigned index = 0; index < 3; index++) process->arguments[index] = info.entry.args[index];
	}
	long number = process->syscall;
	unsigned long first = process->arguments[0], second = process->arguments[1], third = process->arguments[2];
	int detached = (number == SYS_unshare && first == CLONE_FILES) || (number == SYS_close_range && (third & CLOSE_RANGE_UNSHARE));
	if (info.op == PTRACE_SYSCALL_INFO_ENTRY) {
		/* These can publish handles outside the traced tree or bypass its syscall stops. */
		int escapes = number == SYS_sendmsg || number == SYS_sendmmsg || number == SYS_io_uring_setup ||
			number == SYS_io_uring_enter || number == SYS_io_uring_register || number == SYS_ptrace ||
			number == SYS_process_vm_writev || (number == SYS_unshare && (first & ~(unsigned long)CLONE_FILES)) || number == SYS_setns;
		if (number == SYS_clone) escapes |= (first & 0x00800000) != 0; /* CLONE_UNTRACED */
		/* clone3 flags live in mutable shared memory: prove attachment from the kernel event instead. */
		if (number == SYS_clone3 || number == SYS_ioctl) { process->uncertain = 1; domain->uncertain++; }
		if (escapes) domain->escaped = 1;
		process->call_epoch = table->epoch;
		process->mutation = number == SYS_close || (number == SYS_close_range && !detached) || number == SYS_dup ||
			number == SYS_dup2 || number == SYS_dup3 || number == SYS_open || number == SYS_openat ||
			number == SYS_openat2 || number == SYS_creat || number == SYS_memfd_create ||
			(number == SYS_fcntl && (second == F_SETFD || second == F_DUPFD || second == F_DUPFD_CLOEXEC));
		if (process->mutation) {
			table->epoch++;
			/* Exit-stop order cannot prove overlapping mutations' kernel order.
			 * Later uncontended calls can establish fresh origins without disabling the domain. */
			if (table->active++) { table->count = 0; table->generation++; process->mutation_generation = 0; }
			else process->mutation_generation = table->generation;
		}
		/* Dropping uncertain provenance before a failing close/dup is safe; retaining a stale slot is not. */
		if (number == SYS_close) set_descriptor_origin(process, domain, (int)first, 0, 0);
		if ((number == SYS_dup2 || number == SYS_dup3) && first != second)
			set_descriptor_origin(process, domain, (int)second, 0, 0);
	}
	if (info.op != PTRACE_SYSCALL_INFO_EXIT) return 0;
	if (process->uncertain) {
		domain->uncertain--; process->uncertain = 0;
		if (!info.exit.is_error) domain->escaped = 1;
	}
	if (process->mutation) {
		process->mutation = 0; table->active--;
		if (!process->mutation_generation || process->mutation_generation != table->generation) return 0;
	}
	if (info.exit.is_error || domain->escaped) return 0;
	/* A successful split changes only the caller's table; failure preserves sharing. */
	if (detached) {
		if (detach_descriptor_table(process, domain) < 0) return -1;
		table = process->table;
	}
	if (number == SYS_close_range) for (unsigned index = 0; index < table->count;) {
		struct descriptor_origin *entry = &table->entries[index];
		if ((unsigned)entry->fd < (unsigned)first || (unsigned)entry->fd > (unsigned)second) { index++; continue; }
		if (third & CLOSE_RANGE_CLOEXEC) { entry->cloexec = 1; index++; }
		else set_descriptor_origin(process, domain, entry->fd, 0, 0);
	}
	struct descriptor_origin source = descriptor_origin(process, (int)first);
	int fd = (int)info.exit.rval;
	if (number == SYS_fcntl && second == F_SETFD) {
		set_descriptor_origin(process, domain, source.fd, source.id, (third & FD_CLOEXEC) != 0);
	} else if (number == SYS_dup || number == SYS_dup2 || number == SYS_dup3 ||
		(number == SYS_fcntl && (second == F_DUPFD || second == F_DUPFD_CLOEXEC))) {
		set_descriptor_origin(process, domain, fd, source.id,
			(number == SYS_dup2 && first == second && source.cloexec) ||
			(number == SYS_dup3 && (third & O_CLOEXEC)) || (number == SYS_fcntl && second == F_DUPFD_CLOEXEC));
	} else if (number == SYS_open || number == SYS_openat || number == SYS_creat || number == SYS_memfd_create) {
		unsigned long flags = number == SYS_open ? second : number == SYS_openat ? third : 0;
		set_descriptor_origin(process, domain, fd, ++domain->next,
			(flags & O_CLOEXEC) || (number == SYS_memfd_create && (second & 1)));
	} else if (number == SYS_openat2) {
		/* Read the installed flag, rather than racing the tracee's open_how memory. */
		char name[64], line[256]; unsigned flags = 0; int found = 0;
		snprintf(name, sizeof(name), "/proc/%ld/fdinfo/%d", (long)pid, fd);
		FILE *file = fopen(name, "re");
		while (file && fgets(line, sizeof(line), file)) if (sscanf(line, "flags: %o", &flags) == 1) { found = 1; break; }
		if (file) fclose(file);
		set_descriptor_origin(process, domain, fd, found ? ++domain->next : 0, (flags & O_CLOEXEC) != 0);
	}
	return 0;
}

static int null_device(const struct stat *state) {
	return S_ISCHR(state->st_mode) && major(state->st_rdev) == 1 && minor(state->st_rdev) == 3;
}

/* The entire owned tree is stopped until this job retires. External OFDs remain unknown. */
static int descriptor_context(struct decision_job *job, char *line, size_t capacity) {
	char path[64];
	snprintf(path, sizeof(path), "/proc/%ld/fd", (long)job->pid);
	DIR *directory = opendir(path);
	if (!directory) return -1;
	int pins[MAX_POSITIONS], fds[MAX_POSITIONS], nulls[MAX_POSITIONS], count = 0, result = -1, null_input = -1;
	struct dirent *entry;
	size_t used = 0;
	for (;;) {
		errno = 0;
		entry = readdir(directory);
		if (!entry) { if (errno) goto done; break; }
		char *end;
		long descriptor = strtol(entry->d_name, &end, 10);
		if (!*entry->d_name || *end || descriptor < 0 || descriptor > INT_MAX) continue;
		int pin = duplicate_tracee_fd(job->pid, (unsigned)descriptor);
		struct stat state;
		if (pin < 0) goto done;
		if (fstat(pin, &state) < 0) { close(pin); goto done; }
		if (!S_ISREG(state.st_mode) && !(null_device(&state) && descriptor != 1 && descriptor != 2)) { close(pin); continue; }
		if (count == MAX_POSITIONS) { close(pin); goto done; }
		if (!descriptor && null_device(&state)) null_input = count;
		nulls[count] = null_device(&state); pins[count] = pin; fds[count++] = (int)descriptor;
	}
	/* Ordinary stdin keeps its existing profile; include it only when an extra FD shares its OFD. */
	if (null_input >= 0) {
		int shared = 0;
		for (int index = 0; index < count; index++) if (fds[index] > 2 && nulls[index]) {
			long same = syscall(SYS_kcmp, getpid(), getpid(), KCMP_FILE, pins[null_input], pins[index]);
			if (same < 0) goto done;
			if (!same) { shared = 1; break; }
		}
		if (!shared) { close(pins[null_input]); pins[null_input] = pins[--count]; fds[null_input] = fds[count]; }
	}
	/* Canonical alias representatives must not depend on procfs enumeration order. */
	for (int index = 0; index < count; index++) for (int previous = index; previous > 0 && fds[previous] < fds[previous - 1]; previous--) {
		int fd = fds[previous], pin = pins[previous];
		fds[previous] = fds[previous - 1]; pins[previous] = pins[previous - 1];
		fds[previous - 1] = fd; pins[previous - 1] = pin;
	}
	for (int index = 0; index < count; index++) {
		int pin = pins[index], alias = fds[index], owned = 0, flags = fcntl(pin, F_GETFL);
		struct stat state;
		off_t offset = lseek(pin, 0, SEEK_CUR);
		if (flags < 0 || offset < 0 || fstat(pin, &state) < 0) goto done;
		for (int previous = 0; previous < index; previous++) {
			long same = syscall(SYS_kcmp, getpid(), getpid(), KCMP_FILE, pin, pins[previous]);
			if (same < 0) goto done;
			unsigned long origin = descriptor_origin(job->process, fds[index]).id;
			unsigned long other = descriptor_origin(job->process, fds[previous]).id;
			if (origin && other && ((same == 0) != (origin == other))) goto done;
			if (!same) { alias = fds[previous]; break; }
		}
		owned = !job->domain->escaped && !job->domain->uncertain && job->process->table &&
			!job->process->table->active && descriptor_origin(job->process, fds[index]).id != 0;
		int length = snprintf(line + used, capacity - used,
			"%s{\"fd\":%d,\"alias\":%d,\"device\":\"%ju\",\"inode\":\"%ju\",\"flags\":%d,\"offset\":%jd,\"owned\":%s%s}",
			index ? "," : "", fds[index], alias, (uintmax_t)state.st_dev, (uintmax_t)state.st_ino, flags,
			(intmax_t)offset, owned ? "true" : "false", null_device(&state) ? ",\"type\":\"null\"" : "");
		if (length < 0 || (size_t)length >= capacity - used) goto done;
		used += (size_t)length;
	}
	line[used] = 0;
	result = 0;
done:
	while (count) close(pins[--count]);
	closedir(directory);
	return result;
}

static int open_tracee_output(pid_t pid, unsigned fd) {
	int duplicate = duplicate_tracee_fd(pid, fd);
	if (duplicate >= 0) return duplicate;
	char path[64];
	if (snprintf(path, sizeof(path), "/proc/%ld/fd/%u", (long)pid, fd) >= (int)sizeof(path)) {
		errno = ENAMETOOLONG;
		return -1;
	}
	/* This fallback opens a new description; never change flags on a pidfd duplicate. */
	int output = open(path, O_WRONLY | O_CLOEXEC | O_NONBLOCK);
	if (output < 0) return -1;
	struct stat state;
	int flags = fcntl(output, F_GETFL);
	if (fstat(output, &state) == 0 && S_ISFIFO(state.st_mode) && flags >= 0 &&
		fcntl(output, F_SETFL, flags & ~O_NONBLOCK) >= 0) return output;
	close(output);
	return -1;
}

static int position_matches(const struct file_position *position) {
	struct stat state;
	return fstat(position->duplicate, &state) == 0 && (S_ISREG(state.st_mode) ||
		(null_device(&state) && !position->before && !position->after && position->content_length == -1)) &&
		(uintmax_t)state.st_dev == position->device && (uintmax_t)state.st_ino == position->inode &&
		fcntl(position->duplicate, F_GETFL) == position->flags &&
		lseek(position->duplicate, 0, SEEK_CUR) == position->before;
}

/* Only the tracer thread may mutate a held image. Each job owns its reply channel. */
static int request_tracer(struct decision_job *job, unsigned code) {
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
	char descriptors[MAX_LINE];
	int captured = 0;
	if (job->domain) {
		/* Temporary snapshot pins must close before cancellation can retire their job. */
		pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
		captured = descriptor_context(job, descriptors, sizeof(descriptors)) == 0;
		pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
	}
	int request_length = snprintf(line, sizeof(line),
		"{\"version\":1,\"token\":\"%s\",\"execution\":\"%s\",\"pid\":%ld,\"tracer\":%ld%s%s%s}\n",
		job->token, job->execution_id, (long)job->pid, (long)getpid(),
		captured ? ",\"descriptors\":[" : "", captured ? descriptors : "", captured ? "]" : "");
	if (request_length < 0 || request_length >= (int)sizeof(line) ||
		transfer(connection, line, (size_t)request_length, 1) < 0 || read_line(connection, line, sizeof(line)) < 0) return -1;
	if (!strcmp(line, "C")) return -1;
	if (!strcmp(line, "F")) return -2;
	if (!strcmp(line, "O")) return connection;
	if (sscanf(line, "P %u %u %zu %u", &code, &job->count, &total, &job->position_count) != 4 || code > 255 ||
		job->count > MAX_OUTPUT_EVENTS || total > MAX_OUTPUT_BYTES || job->position_count > MAX_POSITIONS) return -1;
	if (job->position_count) {
		job->positions = calloc(job->position_count, sizeof(*job->positions));
		if (!job->positions) return -1;
		for (unsigned index = 0; index < job->position_count; index++) {
			job->positions[index].duplicate = -1; job->positions[index].writer = -1;
		}
	}
	size_t received = 0;
	for (unsigned index = 0; index < job->position_count; index++) {
		struct file_position *position = &job->positions[index];
		if (read_line(connection, line, sizeof(line)) < 0 ||
			sscanf(line, "S %d %ju %ju %d %" SCNd64 " %" SCNd64 " %" SCNd64 " %d", &position->descriptor,
				&position->device, &position->inode, &position->flags, &position->before, &position->after, &position->content_length, &position->after_flags) != 8 ||
			position->descriptor < 0 || position->before < 0 || position->after < 0 || position->content_length < -1 ||
			position->after_flags < 0 || ((position->flags ^ position->after_flags) & ~(O_APPEND | O_NONBLOCK)) ||
			(position->content_length >= 0 && (uint64_t)position->content_length > total - received)) goto decline;
		if (position->content_length > 0) {
			position->content = malloc((size_t)position->content_length);
			if (!position->content || transfer(connection, position->content, (size_t)position->content_length, 0) < 0) return -1;
		}
		if (position->content_length >= 0) received += (size_t)position->content_length;
	}
	job->events = calloc(job->count ? job->count : 1, sizeof(*job->events));
	if (!job->events) return -1;
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
	for (unsigned index = 0; index < job->position_count; index++) {
		struct file_position *position = &job->positions[index];
		pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
		position->duplicate = duplicate_tracee_fd(job->pid, (unsigned)position->descriptor);
		pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
		if (position->duplicate < 0 || !position_matches(position)) goto decline;
		if (job->domain && (job->domain->escaped || job->domain->uncertain || !job->process->table ||
			job->process->table->active || !descriptor_origin(job->process, position->descriptor).id)) goto decline;
		for (unsigned previous = 0; previous < index; previous++) {
			const struct file_position *other = &job->positions[previous];
			if (position->content_length >= 0 && other->content_length >= 0 &&
				position->device == other->device && position->inode == other->inode) goto decline;
			long same = syscall(SYS_kcmp, getpid(), getpid(), KCMP_FILE, position->duplicate, other->duplicate);
			if (same < 0) goto decline;
			if (same == 0) {
				if (position->before != other->before || position->after != other->after || position->after_flags != other->after_flags) goto decline;
				position->alias = 1;
			}
		}
		if (position->content_length >= 0) {
			char path[64]; struct stat state;
			snprintf(path, sizeof(path), "/proc/self/fd/%d", position->duplicate);
			pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
			position->writer = open(path, O_WRONLY | O_CLOEXEC);
			pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
			if (position->writer < 0 || fstat(position->writer, &state) < 0 ||
				(uintmax_t)state.st_dev != position->device || (uintmax_t)state.st_ino != position->inode) goto decline;
		}
	}
	/* From the first text mutation onward, failure terminates the entire trace tree. */
	if (request_tracer(job, 125) < 0) return -2;
	if (transfer(connection, "A\n", 2, 1) < 0 || read_line(connection, line, sizeof(line)) < 0 || strcmp(line, "R")) return -2;
	for (unsigned index = 0; index < job->position_count; index++)
		if (!position_matches(&job->positions[index])) return -2;
	for (unsigned index = 0; index < job->position_count; index++) {
		const struct file_position *position = &job->positions[index];
		if (position->writer >= 0 && (ftruncate(position->writer, 0) < 0 ||
			transfer(position->writer, position->content, (size_t)position->content_length, 1) < 0)) return -2;
	}
	for (unsigned index = 0; index < job->position_count; index++) {
		const struct file_position *position = &job->positions[index];
		if (position->alias) continue;
		if (position->after_flags != position->flags && (fcntl(position->duplicate, F_SETFL, position->after_flags) < 0 ||
			fcntl(position->duplicate, F_GETFL) != position->after_flags)) return -2;
		if (lseek(position->duplicate, position->after, SEEK_SET) != position->after) return -2;
	}
	/* Output may feed another tracee. Release the offset lease before a pipe write can block. */
	if (job->domain && job->domain->enabled && request_tracer(job, 256) < 0) return -2;
	for (unsigned index = 0; index < job->count; index++) {
		struct output_event *event = &job->events[index];
		if (transfer(job->outputs[event->fd], event->data, event->length, 1) < 0) return -2;
	}
	if (request_tracer(job, code) < 0 || transfer(connection, "D\n", 2, 1) < 0) return -2;
	return -1;
decline:
	/* No image or shared offset changed: explicitly acknowledge native fallback. */
	(void)transfer(connection, "N\n", 2, 1);
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
	if (job->positions) for (unsigned index = 0; index < job->position_count; index++) {
		close(job->positions[index].duplicate); close(job->positions[index].writer); free(job->positions[index].content);
	}
	free(job->positions);
	free_events(job->events, job->count);
	free(job);
}

static int start_decision(struct traced_process *process, const char *socket_path,
	const char *token, const char *execution_id, struct descriptor_domain *domain) {
	struct user_regs_struct registers;
	if (ptrace(PTRACE_GETREGS, process->pid, 0, &registers) < 0 || registers.cs != 0x33) return -1;
	struct decision_job *job = calloc(1, sizeof(*job));
	if (!job) return -1;
	*job = (struct decision_job){ .pid = process->pid, .socket_path = socket_path,
		.token = token, .execution_id = execution_id, .channel = {-1, -1},
		.connection = -1, .outputs = {-1, -1, -1}, .result = -1, .domain = domain, .process = process };
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

static void release_process(struct traced_process **processes, pid_t pid, struct descriptor_domain *domain) {
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
		drop_descriptor_table(observer, domain);
		free(observer);
	}
}

static int trace(char **command, const char *socket_path, const char *token, const char *execution_id,
	int skip, unsigned skip_code, int descriptors) {
	int inspect_descriptors = descriptors;
	descriptors = descriptors == 1;
	struct descriptor_domain domain = {.enabled = descriptors};
	pid_t barrier = 0;
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
	if (ptrace(PTRACE_SEIZE, root, 0, options | (descriptors ? PTRACE_O_TRACESYSGOOD : 0)) < 0 || transfer(gate[1], "R", 1, 1) < 0) {
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
				char reply;
				if (code == 256 && descriptors && item->armed) {
					item->pending = 0; if (barrier == item->pid) barrier = 0;
					reply = 'Y';
				} else {
					item->armed = 1;
					reply = code > 255 || replace_with_exit(item->pid, code) < 0 ? 'N' : 'Y';
				}
				if (send(job->channel[0], &reply, 1, MSG_NOSIGNAL) != 1) goto fatal;
			} else if (received == 0) {
				pthread_join(job->thread, NULL);
				int result = job->result;
				if (result >= 0) { item->fd = result; job->connection = -1; }
				free_job(job); item->job = NULL; item->armed = 0;
				if (result == -2) goto fatal;
				if (descriptors) { item->pending = 0; if (barrier == item->pid) barrier = 0; }
				else if (ptrace(PTRACE_CONT, item->pid, 0, 0) < 0 && errno != ESRCH) goto fatal;
				continue;
			} else if (received > 0 || (errno != EAGAIN && errno != EINTR)) goto fatal;
			count++;
		}
	manage_descriptors:
		if (descriptors) {
			if (!barrier) for (struct traced_process *item = processes; item; item = item->next)
				if (item->pending && !item->historical) { barrier = item->pid; break; }
			int ready = 1;
			for (struct traced_process *item = processes; item; item = item->next) {
				if (item->historical || item->stopped) continue;
				ready = 0;
				if (barrier && ptrace(PTRACE_INTERRUPT, item->pid, 0, 0) < 0 && errno != ESRCH && errno != EIO) goto fatal;
			}
			for (struct traced_process *item = processes; item; item = item->next) {
				if (item->historical || !item->stopped) continue;
				if (barrier) {
					if (ready && item->pid == barrier && !item->job &&
						start_decision(item, socket_path, token, execution_id, &domain) < 0) {
						item->pending = 0; barrier = 0; goto manage_descriptors;
					}
				} else if (!item->job && !item->awaiting_parent) {
					if (ptrace(item->listening ? PTRACE_LISTEN : domain.escaped ? PTRACE_CONT : PTRACE_SYSCALL, item->pid, 0, item->delivered) < 0 && errno != ESRCH) goto fatal;
					item->stopped = 0; item->delivered = 0; item->listening = 0;
				}
			}
		}
		pid_t pid = waitpid(-1, &status, __WALL | WNOHANG);
		if (pid < 0) {
			if (errno == EINTR) continue;
			if (errno == ECHILD) break;
			goto fatal;
		}
		if (pid == 0) {
			count = 1;
			for (struct traced_process *item = processes; item; item = item->next) if (item->job) count++;
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
			if (pid == barrier) barrier = 0;
			if (pid == root) root_status = status;
			for (struct traced_process *item = processes; item; item = item->next)
				if (item->pid == pid && item->armed) goto fatal;
			release_process(&processes, pid, &domain);
			continue;
		}
		if (!WIFSTOPPED(status)) continue;
		unsigned event = (unsigned)status >> 16;
		int delivered = WSTOPSIG(status);
		struct traced_process *current = processes;
		while (current && (current->pid != pid || current->historical)) current = current->next;
		/* waitpid may deliver an automatically attached child's stop before its parent's fork event. */
		if (!current) {
			if (track_process(&processes, pid) < 0) goto fatal;
			current = processes;
			current->awaiting_parent = descriptors && event == PTRACE_EVENT_STOP;
		}
		current->stopped = 1;
		if (descriptors && delivered == (SIGTRAP | 0x80)) {
			if (observe_descriptor_syscall(current, &domain) < 0) goto fatal;
			delivered = 0;
		}
		if (event == PTRACE_EVENT_FORK || event == PTRACE_EVENT_VFORK || event == PTRACE_EVENT_CLONE) {
			unsigned long child;
			if (ptrace(PTRACE_GETEVENTMSG, pid, 0, &child) < 0) goto fatal;
			struct traced_process *known = processes;
			while (known && known->pid != (pid_t)child) known = known->next;
			if (!known && track_process(&processes, (pid_t)child) < 0) goto fatal;
			if (!known) known = processes;
			known->awaiting_parent = 0;
			if (descriptors && !domain.escaped) {
				if (current->uncertain && current->syscall == SYS_clone3) { current->uncertain = 0; domain.uncertain--; }
				long shared = syscall(SYS_kcmp, pid, child, KCMP_FILES, 0, 0);
				if (shared < 0) domain.escaped = 1;
				else if (!shared && current->table) { known->table = current->table; known->table->references++; }
				else if (!(known->table = copy_descriptor_table(current))) goto fatal;
			}
		}
		if (event == PTRACE_EVENT_STOP && delivered != SIGTRAP) {
			if (descriptors) { current->listening = 1; continue; }
			if (ptrace(PTRACE_LISTEN, pid, 0, 0) < 0 && errno != ESRCH) goto fatal;
			continue;
		}
		if (event == PTRACE_EVENT_EXEC) {
			unsigned long previous;
			if (ptrace(PTRACE_GETEVENTMSG, pid, 0, &previous) < 0) goto fatal;
			/* A non-leader exec replaces its TID without a separate death notification. */
			if ((pid_t)previous != pid) {
				for (struct traced_process *item = processes; item; item = item->next) if (item->pid == (pid_t)previous && !item->historical) {
					drop_descriptor_table(current, &domain);
					current->table = item->table; item->table = NULL; current->call_epoch = item->call_epoch;
					break;
				}
				release_process(&processes, (pid_t)previous, &domain);
			}
			/* exec unshares CLONE_FILES before closing only this image's CLOEXEC slots. */
			if (detach_descriptor_table(current, &domain) < 0) goto fatal;
			for (unsigned index = 0; current->table && index < current->table->count;)
				if (current->table->entries[index].cloexec) set_descriptor_origin(current, &domain, current->table->entries[index].fd, 0, 0);
				else index++;
		}
		if (event == PTRACE_EVENT_EXEC && ++exec_events > 1) {
			if (skip && replace_with_exit(pid, skip_code) < 0) goto fatal;
			if (socket_path) {
				for (struct traced_process *item = processes; item; item = item->next) {
					if (item->pid != pid) continue;
					/* Earlier images still own completion of this process, including later execs. */
					if (item->fd >= 0) {
						if (track_process(&processes, pid) < 0) goto fatal;
						item->historical = 1;
						processes->table = item->table; item->table = NULL;
						item = processes;
						item->stopped = 1;
					}
					if (descriptors) { item->pending = 1; goto held; }
					if (start_decision(item, socket_path, token, execution_id, inspect_descriptors ? &domain : NULL) == 0) goto held;
					break;
				}
			}
		}
		if (event != 0) delivered = 0;
		if (descriptors) { current->delivered = delivered; continue; }
		if (ptrace(PTRACE_CONT, pid, 0, delivered) < 0 && errno != ESRCH) goto fatal;
	held:;
	}
	while (processes) release_process(&processes, processes->pid, &domain);
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
	while (processes) release_process(&processes, processes->pid, &domain);
	free(polling); close(signals);
	return 125;
}

/* Reproduce a complete inherited FD table at the existing sandboxed native outlet.
 * The supervisor alone keeps the pins, and drains descendants before reporting offsets. */
static int execute_descriptors(const char *manifest, const char *report, char *executable, char **command) {
	struct file_position positions[MAX_POSITIONS];
	char *paths[MAX_POSITIONS] = {0}, line[MAX_LINE];
	unsigned count = 0, close_input = 0, initialized = 0;
	int result = 70, minimum = 3, output = -1, root_status = -1;
	if (has_unmodeled_descriptors() != 0) return result;
	FILE *input = fopen(manifest, "re");
	if (!input) return result;
	if (!fgets(line, sizeof(line), input) || sscanf(line, "FD1 %u %u", &count, &close_input) != 2 ||
		count > MAX_POSITIONS || close_input > 1) goto done;
	for (unsigned index = 0; index < count; index++) {
		struct file_position *position = &positions[index];
		*position = (struct file_position){.duplicate = -1}; initialized++;
		unsigned length;
		int alias;
		if (!fgets(line, sizeof(line), input) || sscanf(line, "%d %d %d %" SCNd64 " %u",
			&position->descriptor, &alias, &position->flags, &position->before, &length) != 5 ||
			position->descriptor < 0 || position->descriptor == 1 || position->descriptor == 2 || position->descriptor == INT_MAX ||
			(close_input && position->descriptor == 0) || (index && position->descriptor <= positions[index - 1].descriptor) ||
			position->before < 0 || length >= PATH_MAX || alias > position->descriptor || alias < 0) goto done;
		position->alias = (int)index;
		if (alias != position->descriptor) {
			unsigned previous = 0;
			while (previous < index && positions[previous].descriptor != alias) previous++;
			if (previous == index || positions[previous].alias != (int)previous || length ||
				positions[previous].flags != position->flags || positions[previous].before != position->before) goto done;
			position->alias = (int)previous;
		} else if (!length) goto done;
		paths[index] = calloc((size_t)length + 1, 1);
		if (!paths[index] || fread(paths[index], 1, length, input) != length || fgetc(input) != '\n' ||
			strlen(paths[index]) != length || (length && paths[index][0] != '/')) goto done;
		if (position->descriptor >= minimum) minimum = position->descriptor + 1;
	}
	if (fgetc(input) != EOF || ferror(input)) goto done;
	fclose(input); input = NULL;
	for (unsigned index = 0; index < count; index++) {
		struct file_position *position = &positions[index];
		if (position->alias != (int)index) {
			position->duplicate = fcntl(positions[position->alias].duplicate, F_DUPFD_CLOEXEC, minimum);
		} else {
			const int allowed = O_ACCMODE | O_APPEND | O_NONBLOCK | O_DSYNC | O_SYNC | 0x8000 /* kernel O_LARGEFILE */ | O_NOATIME | O_NOFOLLOW | O_DIRECT;
			if ((position->flags & ~allowed) || (position->flags & O_ACCMODE) == O_ACCMODE) goto done;
			int fd = open(paths[index], position->flags | O_CLOEXEC);
			if (fd < 0) goto done;
			position->duplicate = fcntl(fd, F_DUPFD_CLOEXEC, minimum);
			close(fd);
		}
		struct stat state;
		if (position->duplicate < 0 || fstat(position->duplicate, &state) < 0 || !(S_ISREG(state.st_mode) ||
			(null_device(&state) && !position->before)) ||
			fcntl(position->duplicate, F_GETFL) != position->flags ||
			lseek(position->duplicate, position->before, SEEK_SET) != position->before) goto done;
	}
	output = open(report, O_WRONLY | O_CLOEXEC | O_NOFOLLOW);
	if (output < 0 || prctl(PR_SET_CHILD_SUBREAPER, 1) < 0) goto done;
	pid_t root = fork();
	if (root < 0) goto done;
	if (!root) {
		close(output);
		if (close_input && close(0) < 0 && errno != EBADF) _exit(70);
		for (unsigned index = 0; index < count; index++)
			if (dup2(positions[index].duplicate, positions[index].descriptor) < 0) _exit(70);
		for (unsigned index = 0; index < count; index++) close(positions[index].duplicate);
		execv(executable, command);
		_exit(errno == ENOENT ? 127 : 126);
	}
	for (;;) {
		int status;
		pid_t child = waitpid(-1, &status, 0);
		if (child == root) root_status = status;
		if (child >= 0 || errno == EINTR) continue;
		if (errno != ECHILD || root_status < 0) goto done;
		break;
	}
	if (ftruncate(output, 0) < 0 || lseek(output, 0, SEEK_SET) != 0 || dprintf(output, "FD1 %u\n", count) < 0) goto done;
	for (unsigned index = 0; index < count; index++) {
		struct file_position *position = &positions[index];
		off_t offset = lseek(position->duplicate, 0, SEEK_CUR);
		int flags = fcntl(position->duplicate, F_GETFL);
		struct stat state;
		if (offset < 0 || flags < 0 || fstat(position->duplicate, &state) < 0 || dprintf(output, "%d %d %jd %ju %ju\n",
			position->descriptor, flags, (intmax_t)offset, (uintmax_t)state.st_dev, (uintmax_t)state.st_ino) < 0) goto done;
	}
	result = WIFEXITED(root_status) ? WEXITSTATUS(root_status) : 128 + WTERMSIG(root_status);
done:
	if (input) fclose(input);
	if (output >= 0 && close(output) < 0) result = 70;
	for (unsigned index = 0; index < initialized; index++) { free(paths[index]); close(positions[index].duplicate); }
	if (root_status >= 0 && WIFSIGNALED(root_status)) { signal(WTERMSIG(root_status), SIG_DFL); raise(WTERMSIG(root_status)); }
	return result;
}

int main(int argc, char **argv) {
	int dispatched = image_dispatch(argc, argv);
	if (dispatched >= 0) return dispatched;
	if (argc == 2 && !strcmp(argv[1], "--protocol-version")) {
		puts("14");
		return 0;
	}
	if (argc >= 2 && (!strcmp(argv[1], "--exec") || !strcmp(argv[1], "--exec-closed-input") || !strcmp(argv[1], "--exec-fds"))) {
		int descriptors = !strcmp(argv[1], "--exec-fds");
		if (argc < (descriptors ? 7 : 5) || strlen(argv[2]) != 2 || strspn(argv[2], "12") != 2) return 64;
		/* Save stdout's source before changing either endpoint, including swapped routes. */
		int output = fcntl(argv[2][0] - '0', F_DUPFD_CLOEXEC, 3);
		if (output < 0) return 70;
		int routed = dup2(argv[2][1] - '0', 2) >= 0 && dup2(output, 1) >= 0;
		close(output);
		if (!routed) return 70;
		/* Close only at the native outlet: Node and the sandbox launcher may fill vacant stdio. */
		if (!strcmp(argv[1], "--exec-closed-input") && close(0) < 0 && errno != EBADF) return 70;
		/* Preserve the former libuv outlet's empty mask and default dispositions. */
		sigset_t empty;
		sigemptyset(&empty);
		if (sigprocmask(SIG_SETMASK, &empty, NULL) < 0) return 70;
		for (int number = 1; number < NSIG; number++) signal(number, SIG_DFL);
		if (descriptors) {
			char *executable = argv[6];
			argv[6] = argv[5];
			return execute_descriptors(argv[3], argv[4], executable, argv + 6);
		}
		char *executable = argv[4];
		argv[4] = argv[3];
		execv(executable, argv + 4);
		return errno == ENOENT ? 127 : 126;
	}
	if (argc == 2 && !strcmp(argv[1], "--probe-clean-fds")) {
		int extra = has_unmodeled_descriptors();
		return extra < 0 ? 70 : extra ? 65 : 0;
	}
	if (getenv("PI_SPEC_HELD_EXEC_SHELL")) {
		char *real_shell = take_env("PI_SPEC_HELD_EXEC_SHELL");
		char *socket_path = take_env("PI_SPEC_HELD_EXEC_SOCKET");
		char *token = take_env("PI_SPEC_HELD_EXEC_TOKEN");
		char *execution_id = take_env("PI_SPEC_HELD_EXEC_ID");
		char *descriptors = take_env("PI_SPEC_HELD_EXEC_DESCRIPTORS");
		int track_descriptors = descriptors && !strcmp(descriptors, "1") ? 1 : descriptors && !strcmp(descriptors, "2") ? 2 : 0;
		free(descriptors);
		if (!real_shell) return 70;
		char **command = calloc((size_t)argc + 1, sizeof(*command));
		if (!command) return 70;
		command[0] = real_shell;
		for (int index = 1; index < argc; index++) command[index] = argv[index];
		if (!socket_path || !token || !execution_id) {
			execvp(real_shell, command);
			return errno == ENOENT ? 127 : 126;
		}
		return trace(command, socket_path, token, execution_id, 0, 0, track_descriptors);
	}
	if (argc < 2) return 64;
	int command = 1, skip = 0;
	unsigned skip_code = 0;
	if (argc >= 4 && !strcmp(argv[1], "--skip-code")) {
		skip = 1; skip_code = (unsigned)strtoul(argv[2], 0, 10); command = 3;
		if (skip_code > 255) return 64;
	}
	return trace(argv + command, NULL, NULL, NULL, skip, skip_code, 0);
}
