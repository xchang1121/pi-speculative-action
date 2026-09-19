#define _GNU_SOURCE
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <inttypes.h>
#include <linux/audit.h>
#include <linux/kcmp.h>
#include <linux/netlink.h>
#include <linux/rtnetlink.h>
#include <linux/sock_diag.h>
#include <linux/unix_diag.h>
#include <sched.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
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
#define MAX_HANDLES 256
#define MAX_OBJECTS 1024
#define MAX_INPUT_BYTES (2UL * 1024 * 1024)
#define MAX_REQUEST_BYTES (MAX_INPUT_BYTES * 2 + MAX_LINE)

struct file_position {
	/* stream: 0=file, 1/2=pipe input (closed/live), 3=pipe output, 4/5=Unix endpoint (connected/closed). */
	int descriptor, duplicate, writer, flags, after_flags, alias, stream, queue_alias, eof, producer, object;
	int capacity, shutdown, peer_shutdown, peer_queued, allocated, peer_descriptor;
	unsigned peer_inode;
	uintmax_t device, inode;
	int64_t before, after;
	int64_t content_length;
	unsigned char *content;
	char *path;
};

struct descriptor_origin { int fd, cloexec; unsigned long id; unsigned object; };
struct resource_object {
	dev_t device; ino_t inode; mode_t type;
	unsigned internal, foreign, pipe;
	unsigned long channel;
};
struct descriptor_table {
	unsigned references, count, active;
	unsigned long epoch, generation;
	struct descriptor_origin entries[MAX_HANDLES];
};

/* Track provenance without keeping kernel handles alive beyond their native lifetime. */
struct descriptor_domain {
	int enabled, escaped;
	unsigned uncertain;
	unsigned long next;
	struct traced_process **processes;
	unsigned initialized, incomplete, object_count;
	struct resource_object objects[MAX_OBJECTS];
};

struct output_event {
	unsigned fd, kind;
	size_t length;
	unsigned char *data;
};

struct decision_job {
	pthread_t thread;
	pid_t pid;
	const char *socket_path, *token, *execution_id;
	int channel[2], connection, outputs[3], result, pidfd;
	struct output_event *events;
	unsigned count, stream_count;
	struct file_position *positions;
	unsigned position_count;
	struct descriptor_domain *domain;
	struct traced_process *process;
	char *context;
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
	unsigned long arguments[6];
	int descriptor_count, descriptors[MAX_HANDLES], internal_message;
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

static int has_unmodeled_descriptors(int minimum) {
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
		if (!*entry->d_name || *end || fd < minimum || fd == scan_fd) continue;
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
	int extra = has_unmodeled_descriptors(3);
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

static int duplicate_tracee_fd(struct decision_job *job, unsigned fd) {
	#if defined(SYS_pidfd_open) && defined(SYS_pidfd_getfd)
	if (job->pidfd < 0) job->pidfd = (int)syscall(SYS_pidfd_open, job->pid, 0);
	/* Both syscalls return CLOEXEC handles; one process pin serves the entire held decision. */
	if (job->pidfd >= 0) return (int)syscall(SYS_pidfd_getfd, job->pidfd, fd, 0);
	#endif
	return -1;
}

static struct descriptor_origin descriptor_origin(struct traced_process *process, int fd) {
	for (unsigned index = 0; process->table && index < process->table->count; index++)
		if (process->table->entries[index].fd == fd) return process->table->entries[index];
	return (struct descriptor_origin){.fd = fd};
}

static void set_descriptor_origin(struct traced_process *process, struct descriptor_domain *domain,
	struct descriptor_origin entry) {
	struct descriptor_table *table = process->table;
	for (unsigned index = 0; index < table->count; index++) if (table->entries[index].fd == entry.fd) {
		table->entries[index] = table->entries[--table->count]; break;
	}
	if (!entry.id) return;
	if (table->count == sizeof(table->entries) / sizeof(table->entries[0])) { domain->escaped = 1; return; }
	table->entries[table->count++] = entry;
}

static int descriptor_numbers(pid_t pid, int *fds, unsigned capacity) {
	char path[64]; snprintf(path, sizeof(path), "/proc/%ld/fd", (long)pid);
	DIR *directory = opendir(path);
	if (!directory) return -1;
	unsigned count = 0; int error = 0;
	for (;;) {
		errno = 0; struct dirent *entry = readdir(directory);
		if (!entry) { error = errno; break; }
		char *end; long fd = strtol(entry->d_name, &end, 10);
		if (!*entry->d_name || *end || fd < 0 || fd > INT_MAX) continue;
		if (count == capacity) { error = E2BIG; break; }
		fds[count++] = (int)fd;
	}
	closedir(directory);
	return error ? -1 : (int)count;
}

/* Read installed flags, never a tracee's mutable result/control buffer. */
static int descriptor_flags(pid_t pid, int fd) {
	char name[64], line[256]; unsigned flags; int result = -1;
	snprintf(name, sizeof(name), "/proc/%ld/fdinfo/%d", (long)pid, fd);
	FILE *file = fopen(name, "re");
	while (file && fgets(line, sizeof(line), file)) if (sscanf(line, "flags: %o", &flags) == 1) { result = (int)flags; break; }
	if (file) fclose(file);
	return result;
}

/* Object history grants no OFD identity. Foreign ancestry is sticky, including
 * inode recycling: it can only refuse a later proof, never manufacture one. */
static unsigned remember_object(struct descriptor_domain *domain, pid_t pid, int fd, int foreign) {
	char name[64]; struct stat state;
	snprintf(name, sizeof(name), "/proc/%ld/fd/%d", (long)pid, fd);
	if (stat(name, &state) < 0) { domain->incomplete = 1; return 0; }
	unsigned index = 0;
	while (index < domain->object_count && !(domain->objects[index].device == state.st_dev &&
		domain->objects[index].inode == state.st_ino && domain->objects[index].type == (state.st_mode & S_IFMT))) index++;
	if (index == MAX_OBJECTS) { domain->incomplete = 1; return 0; }
	if (index == domain->object_count) domain->objects[domain->object_count++] =
		(struct resource_object){.device = state.st_dev, .inode = state.st_ino, .type = state.st_mode & S_IFMT};
	if (foreign) domain->objects[index].foreign = 1;
	return index + 1;
}

static struct resource_object *origin_object(struct descriptor_domain *domain, struct descriptor_origin origin) {
	return origin.object ? &domain->objects[origin.object - 1] : NULL;
}

static struct descriptor_origin created_origin(struct traced_process *process, struct descriptor_domain *domain,
	int fd, int cloexec, int pipe, unsigned long channel) {
	struct descriptor_origin origin = {.fd = fd, .cloexec = cloexec, .id = ++domain->next,
		.object = remember_object(domain, process->pid, fd, 0)};
	struct resource_object *object = origin_object(domain, origin);
	if (object) { object->internal = 1; object->pipe |= pipe; if (channel) object->channel = channel; }
	return origin;
}

/* Recover aliases with KCMP_FILE first. An orphaned internal transfer can create
 * a fresh OFD node only when every external import is accounted for and the
 * object has exclusively internal ancestry. No kernel reference is retained. */
static struct descriptor_origin received_origin(struct traced_process *process, struct descriptor_domain *domain, int fd) {
	int flags = descriptor_flags(process->pid, fd);
	if (flags >= 0) for (struct traced_process *source = *domain->processes; source; source = source->next) {
		if (source->historical || !source->table || source->table->active) continue;
		for (unsigned index = 0; index < source->table->count; index++) {
			struct descriptor_origin entry = source->table->entries[index];
			if (source->table == process->table && entry.fd == fd) continue;
			if (syscall(SYS_kcmp, process->pid, source->pid, KCMP_FILE, fd, entry.fd) != 0) continue;
			entry.fd = fd; entry.cloexec = !!(flags & O_CLOEXEC); return entry;
		}
	}
	unsigned id = remember_object(domain, process->pid, fd, 0);
	struct resource_object *object = id ? &domain->objects[id - 1] : NULL;
	int owned = flags >= 0 && process->internal_message && !domain->incomplete && object && object->internal && !object->foreign;
	if (!owned && object) object->foreign = 1;
	return (struct descriptor_origin){.fd = fd, .cloexec = flags >= 0 && (flags & O_CLOEXEC),
		.id = owned ? ++domain->next : 0, .object = id};
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
	if (!domain->initialized) {
		int fds[MAX_HANDLES], count = descriptor_numbers(pid, fds, MAX_HANDLES);
		domain->initialized = 1;
		if (count < 0) domain->incomplete = 1;
		for (int index = 0; index < count; index++) (void)remember_object(domain, pid, fds[index], 1);
	}
	if (info.op == PTRACE_SYSCALL_INFO_ENTRY) {
		process->syscall = (long)info.entry.nr;
		for (unsigned index = 0; index < 6; index++) process->arguments[index] = info.entry.args[index];
	}
	long number = process->syscall;
	unsigned long first = process->arguments[0], second = process->arguments[1], third = process->arguments[2];
	int receiving = number == SYS_recvmsg || number == SYS_recvmmsg;
	int message = receiving || number == SYS_sendmsg || number == SYS_sendmmsg;
	int pair = number == SYS_pipe || number == SYS_pipe2 || number == SYS_socketpair;
	int detached = (number == SYS_unshare && first == CLONE_FILES) || (number == SYS_close_range && (third & CLOSE_RANGE_UNSHARE));
	if (info.op == PTRACE_SYSCALL_INFO_ENTRY) {
		/* These can publish handles outside the traced tree or bypass its syscall stops. */
		int escapes = number == SYS_io_uring_setup ||
			number == SYS_io_uring_enter || number == SYS_io_uring_register || number == SYS_ptrace ||
			number == SYS_process_vm_writev || number == SYS_splice || number == SYS_tee ||
			(number == SYS_unshare && (first & ~(unsigned long)CLONE_FILES)) || number == SYS_setns;
		if (number == SYS_clone) escapes |= (first & 0x00800000) != 0; /* CLONE_UNTRACED */
		/* Packet boundaries cannot be reconstructed from a byte snapshot. */
		escapes |= (number == SYS_pipe2 && (second & ~(unsigned long)(O_CLOEXEC | O_NONBLOCK))) ||
			(number == SYS_fcntl && second == F_SETFL && (third & O_DIRECT)) ||
			(number == SYS_sendmsg && (third & MSG_OOB)) ||
			((number == SYS_sendto || number == SYS_sendmmsg) && (process->arguments[3] & MSG_OOB));
		/* clone3 flags live in mutable shared memory: prove attachment from the kernel event instead. */
		struct resource_object *channel = origin_object(domain, descriptor_origin(process, (int)first));
		process->internal_message = message && !table->active && channel && channel->channel != 0;
		if (number == SYS_clone3 || number == SYS_ioctl || message) { process->uncertain = 1; domain->uncertain++; }
		if (escapes) domain->escaped = 1;
		process->call_epoch = table->epoch;
		process->mutation = number == SYS_close || (number == SYS_close_range && !detached) || number == SYS_dup ||
			number == SYS_dup2 || number == SYS_dup3 || number == SYS_open || number == SYS_openat ||
			number == SYS_openat2 || number == SYS_creat || number == SYS_memfd_create || pair || receiving || number == SYS_pidfd_getfd ||
			(number == SYS_fcntl && (second == F_SETFD || second == F_DUPFD || second == F_DUPFD_CLOEXEC));
		if (process->mutation) {
			table->epoch++;
			/* Exit-stop order cannot prove overlapping mutations' kernel order.
			 * Later uncontended calls can establish fresh origins without disabling the domain. */
			if (table->active++) { table->count = 0; table->generation++; process->mutation_generation = 0; }
			else process->mutation_generation = table->generation;
		}
		/* Dropping uncertain provenance before a failing close/dup is safe; retaining a stale slot is not. */
		if (number == SYS_close) set_descriptor_origin(process, domain, (struct descriptor_origin){.fd = (int)first});
		if ((number == SYS_dup2 || number == SYS_dup3) && first != second)
			set_descriptor_origin(process, domain, (struct descriptor_origin){.fd = (int)second});
		if (pair || receiving) process->descriptor_count = descriptor_numbers(pid, process->descriptors, MAX_HANDLES);
	}
	if (info.op != PTRACE_SYSCALL_INFO_EXIT) return 0;
	if (process->uncertain) {
		domain->uncertain--; process->uncertain = 0;
		/* A channel cannot change peers, but a shared table could replace its FD
		 * while the kernel reads it. An in-flight call never grants adoption. */
		if (!info.exit.is_error && !(message && process->internal_message &&
			table->epoch == process->call_epoch + (unsigned)receiving && table->active == (unsigned)receiving)) domain->escaped = 1;
	}
	if (process->mutation) {
		process->mutation = 0; table->active--;
		if (!process->mutation_generation || process->mutation_generation != table->generation) { domain->incomplete = 1; return 0; }
	}
	if ((info.exit.is_error && !receiving) || domain->escaped) return 0;
	/* A successful split changes only the caller's table; failure preserves sharing. */
	if (detached) {
		if (detach_descriptor_table(process, domain) < 0) return -1;
		table = process->table;
	}
	if (number == SYS_close_range) for (unsigned index = 0; index < table->count;) {
		struct descriptor_origin *entry = &table->entries[index];
		if ((unsigned)entry->fd < (unsigned)first || (unsigned)entry->fd > (unsigned)second) { index++; continue; }
		if (third & CLOSE_RANGE_CLOEXEC) { entry->cloexec = 1; index++; }
		else set_descriptor_origin(process, domain, (struct descriptor_origin){.fd = entry->fd});
	}
	struct descriptor_origin source = descriptor_origin(process, (int)first);
	int fd = (int)info.exit.rval;
	if (pair || receiving) {
		int installed[MAX_HANDLES], count = descriptor_numbers(pid, installed, MAX_HANDLES), added = 0;
		if (count < 0 || process->descriptor_count < 0) return 0;
		for (int index = 0; index < count; index++) {
			int previous = 0;
			while (previous < process->descriptor_count && installed[index] != process->descriptors[previous]) previous++;
			if (previous == process->descriptor_count) installed[added++] = installed[index];
		}
		if (pair && added != 2) return 0;
		/* Only stream/seqpacket socketpairs have immutable, tree-owned peers.
		 * Datagram destinations can be changed by sendmsg's address argument. */
		unsigned long type = second & ~(unsigned long)(SOCK_CLOEXEC | SOCK_NONBLOCK);
		unsigned long channel = number == SYS_socketpair && first == AF_UNIX && !third &&
			(type == SOCK_STREAM || type == SOCK_SEQPACKET) ? ++domain->next : 0;
		/* recvmsg may install rights before failing to write its userspace result. */
		if (receiving && !process->internal_message && added) { domain->escaped = 1; return 0; }
		for (int index = 0; index < added; index++) set_descriptor_origin(process, domain, receiving
			? received_origin(process, domain, installed[index])
			: created_origin(process, domain, installed[index], number != SYS_pipe && (second & O_CLOEXEC), number != SYS_socketpair, channel));
	} else if (number == SYS_pidfd_getfd) {
		set_descriptor_origin(process, domain, received_origin(process, domain, fd));
	} else if (number == SYS_fcntl && second == F_SETFD) {
		source.cloexec = (third & FD_CLOEXEC) != 0; set_descriptor_origin(process, domain, source);
	} else if (number == SYS_dup || number == SYS_dup2 || number == SYS_dup3 ||
		(number == SYS_fcntl && (second == F_DUPFD || second == F_DUPFD_CLOEXEC))) {
		source.fd = fd; source.cloexec = (number == SYS_dup2 && first == second && source.cloexec) ||
			(number == SYS_dup3 && (third & O_CLOEXEC)) || (number == SYS_fcntl && second == F_DUPFD_CLOEXEC);
		set_descriptor_origin(process, domain, source);
	} else if (number == SYS_open || number == SYS_openat || number == SYS_creat || number == SYS_memfd_create) {
		unsigned long flags = number == SYS_open ? second : number == SYS_openat ? third : 0;
		set_descriptor_origin(process, domain, created_origin(process, domain, fd,
			(flags & O_CLOEXEC) || (number == SYS_memfd_create && (second & 1)), 0, 0));
	} else if (number == SYS_openat2) {
		int flags = descriptor_flags(pid, fd);
		if (flags >= 0) set_descriptor_origin(process, domain, created_origin(process, domain, fd, (flags & O_CLOEXEC) != 0, 0, 0));
	}
	return 0;
}

static int null_device(const struct stat *state) {
	return S_ISCHR(state->st_mode) && major(state->st_rdev) == 1 && minor(state->st_rdev) == 3;
}

/* O_PATH has no seekable position; zero is the descriptor-report sentinel. */
static off_t descriptor_seek(int fd, int flags, off_t offset, int whence) {
	return flags & O_PATH ? (offset ? -1 : 0) : lseek(fd, offset, whence);
}

/* A queue snapshot keeps bytes and producer EOF separate. Repeating tee would copy
 * the same prefix again, so require one complete non-consuming transfer. */
static int pipe_bytes(int fd, unsigned char **bytes, int *eof) {
	struct pollfd ready = {.fd = fd, .events = POLLIN};
	int length, fds[2], result = -1;
	*bytes = NULL;
	if (poll(&ready, 1, 0) < 0 || ioctl(fd, FIONREAD, &length) < 0 ||
		length < 0 || (unsigned)length > MAX_INPUT_BYTES) return -1;
	*eof = !!(ready.revents & POLLHUP);
	if (!length) return 0;
	if (pipe2(fds, O_CLOEXEC | O_NONBLOCK) < 0) return -1;
	int capacity = fcntl(fd, F_GETPIPE_SZ);
	if (capacity > 0 && (fcntl(fds[1], F_GETPIPE_SZ) >= capacity || fcntl(fds[1], F_SETPIPE_SZ, capacity) >= capacity) &&
		(*bytes = malloc((size_t)length)) && tee(fd, fds[1], (size_t)length, SPLICE_F_NONBLOCK) == length &&
		transfer(fds[0], *bytes, (size_t)length, 0) == 0) result = length;
	close(fds[0]); close(fds[1]);
	if (result < 0) { free(*bytes); *bytes = NULL; }
	return result;
}

struct socket_state { unsigned peer; int shutdown, queued, allocated; };

/* Query a single kernel endpoint, not a /proc/net dump or a pathname guess. */
static int socket_state(ino_t inode, struct socket_state *state) {
	struct { struct nlmsghdr header; struct unix_diag_req request; } request = {
		.header = {.nlmsg_len = sizeof(request), .nlmsg_type = SOCK_DIAG_BY_FAMILY, .nlmsg_flags = NLM_F_REQUEST, .nlmsg_seq = 1},
		.request = {.sdiag_family = AF_UNIX, .udiag_states = ~0U, .udiag_ino = inode,
			.udiag_show = UDIAG_SHOW_PEER | UDIAG_SHOW_RQLEN, .udiag_cookie = {~0U, ~0U}},
	};
	int fd = socket(AF_NETLINK, SOCK_RAW | SOCK_CLOEXEC, NETLINK_SOCK_DIAG), result = -1;
	if (fd < 0 || inode > UINT_MAX) { close(fd); return -1; }
	struct sockaddr_nl address = {.nl_family = AF_NETLINK};
	unsigned char buffer[1024];
	*state = (struct socket_state){.shutdown = -1, .queued = -1};
	if (sendto(fd, &request, sizeof(request), 0, (struct sockaddr *)&address, sizeof(address)) != sizeof(request)) goto done;
	struct pollfd ready = {.fd = fd, .events = POLLIN};
	if (poll(&ready, 1, 1000) <= 0) goto done;
	ssize_t size = recv(fd, buffer, sizeof(buffer), 0);
	struct nlmsghdr *header = (void *)buffer;
	if (size < 0 || !NLMSG_OK(header, size) || header->nlmsg_type != SOCK_DIAG_BY_FAMILY || header->nlmsg_seq != 1 ||
		header->nlmsg_len < NLMSG_LENGTH(sizeof(struct unix_diag_msg))) goto done;
	struct unix_diag_msg *message = NLMSG_DATA(header);
	if (message->udiag_ino != inode || message->udiag_family != AF_UNIX || message->udiag_type != SOCK_STREAM) goto done;
	int remaining = (int)header->nlmsg_len - (int)NLMSG_LENGTH(sizeof(*message));
	for (struct rtattr *attribute = (void *)(message + 1); RTA_OK(attribute, remaining); attribute = RTA_NEXT(attribute, remaining)) {
		if (attribute->rta_type == UNIX_DIAG_PEER && RTA_PAYLOAD(attribute) == sizeof(unsigned)) memcpy(&state->peer, RTA_DATA(attribute), sizeof(unsigned));
		if (attribute->rta_type == UNIX_DIAG_SHUTDOWN && RTA_PAYLOAD(attribute) == 1) state->shutdown = *(unsigned char *)RTA_DATA(attribute);
		if (attribute->rta_type == UNIX_DIAG_RQLEN && RTA_PAYLOAD(attribute) == sizeof(struct unix_diag_rqlen)) {
			struct unix_diag_rqlen queues; memcpy(&queues, RTA_DATA(attribute), sizeof(queues)); state->queued = (int)queues.udiag_rqueue; state->allocated = (int)queues.udiag_wqueue;
		}
	}
	if (!remaining && state->shutdown >= 0 && state->shutdown <= 3 && state->queued >= 0) result = 0;
done:
	close(fd); return result;
}

static int socket_option(int fd, int option) {
	int value; socklen_t length = sizeof(value);
	return getsockopt(fd, SOL_SOCKET, option, &value, &length) == 0 && length == sizeof(value) ? value : -2;
}

/* Ancillary data and non-default peek cursors cannot be represented by byte queues. */
static int socket_bytes(int fd, unsigned char **bytes, int *eof) {
	int length; *bytes = NULL;
	struct pollfd ready = {.fd = fd, .events = POLLIN | POLLRDHUP};
	if (socket_option(fd, SO_TYPE) != SOCK_STREAM || socket_option(fd, SO_DOMAIN) != AF_UNIX ||
		socket_option(fd, SO_PEEK_OFF) != -1 || socket_option(fd, SO_PASSCRED) != 0 || socket_option(fd, SO_PASSSEC) != 0 ||
		socket_option(fd, SO_RCVLOWAT) != 1 || socket_option(fd, SO_OOBINLINE) != 0 ||
		poll(&ready, 1, 0) < 0 || ioctl(fd, FIONREAD, &length) < 0 || length < 0 || (unsigned)length > MAX_INPUT_BYTES) return -1;
	*eof = !!(ready.revents & (POLLRDHUP | POLLHUP));
	if (!length) return 0;
	*bytes = malloc((size_t)length);
	if (!*bytes) return -1;
	struct iovec vector = {*bytes, (size_t)length};
	struct msghdr message = {.msg_iov = &vector, .msg_iovlen = 1};
	/* Some kernels count the consumed prefix of a partially read skb in FIONREAD.
	 * recvmsg supplies the actual unread bytes and reports ancillary truncation. */
	ssize_t copied = recvmsg(fd, &message, MSG_PEEK | MSG_DONTWAIT);
	if (copied < 0 || copied > length || (message.msg_flags & (MSG_CTRUNC | MSG_TRUNC | MSG_OOB))) {
		free(*bytes); *bytes = NULL; return -1;
	}
	return (int)copied;
}

/* Both queues are named, even when only one endpoint is inherited. Pending peer input
 * is protected by the same tree lease; no unseen peer traffic is simulated. */
static int stream_state(int fd, struct file_position *state) {
	struct stat metadata;
	if (fstat(fd, &metadata) < 0) return -1;
	if (S_ISFIFO(metadata.st_mode)) {
		state->capacity = fcntl(fd, F_GETPIPE_SZ);
		state->stream = (fcntl(fd, F_GETFL) & O_ACCMODE) == O_WRONLY ? 3 : 1;
		if ((fcntl(fd, F_GETFL) & O_ACCMODE) != O_RDONLY) {
			int bytes; struct pollfd ready = {.fd = fd, .events = POLLOUT};
			if (ioctl(fd, FIONREAD, &bytes) < 0 || bytes || poll(&ready, 1, 0) < 0 || (ready.revents & POLLERR)) return -1;
		}
		return state->capacity >= 4096 ? 0 : -1;
	}
	struct socket_state endpoint, peer = {.shutdown = 3};
	if (!S_ISSOCK(metadata.st_mode) || socket_state(metadata.st_ino, &endpoint) < 0 ||
		(endpoint.peer && (socket_state(endpoint.peer, &peer) < 0 || peer.peer != metadata.st_ino))) return -1;
	state->stream = endpoint.peer ? 4 : 5; state->shutdown = endpoint.shutdown; state->peer_shutdown = peer.shutdown;
	state->peer_queued = peer.queued; state->allocated = endpoint.allocated;
	state->peer_inode = endpoint.peer; state->capacity = socket_option(fd, SO_SNDBUF);
	return state->capacity >= 8192 ? 0 : -1;
}

/* The entire owned tree is stopped until this job retires. External OFDs remain unknown. */
static int descriptor_context(struct decision_job *job, char *line, size_t capacity) {
	int installed[MAX_HANDLES], total = descriptor_numbers(job->pid, installed, MAX_HANDLES);
	if (total < 0) return -1;
	int pins[MAX_POSITIONS], fds[MAX_POSITIONS], nulls[MAX_POSITIONS], count = 0, result = -1, null_input = -1;
	size_t used = 0;
	for (int index = 0; index < total; index++) {
		int descriptor = installed[index];
		int pin = duplicate_tracee_fd(job, (unsigned)descriptor);
		struct stat state;
		if (pin < 0) goto done;
		if (fstat(pin, &state) < 0) { close(pin); goto done; }
		struct resource_object *object = origin_object(job->domain, descriptor_origin(job->process, descriptor));
		if (!S_ISREG(state.st_mode) && !S_ISDIR(state.st_mode) &&
			!((null_device(&state) || ((S_ISFIFO(state.st_mode) || S_ISSOCK(state.st_mode)) &&
				(!job->domain->enabled || (object && (object->pipe || object->channel))))) && descriptor != 1 && descriptor != 2)) { close(pin); continue; }
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
		if (flags < 0 || fstat(pin, &state) < 0) goto done;
		int stream = S_ISFIFO(state.st_mode) || S_ISSOCK(state.st_mode);
		struct file_position queue = {0};
		off_t offset = stream ? 0 : descriptor_seek(pin, flags, 0, SEEK_CUR);
		if (offset < 0 || (stream && ((flags & ~(O_ACCMODE | O_APPEND | O_NONBLOCK | 0x8000)) || stream_state(pin, &queue) < 0))) goto done;
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
			"%s{\"fd\":%d,\"alias\":%d,\"device\":\"%ju\",\"inode\":\"%ju\",\"flags\":%d,\"offset\":%jd,\"owned\":%s%s",
			index ? "," : "", fds[index], alias, (uintmax_t)state.st_dev, (uintmax_t)state.st_ino, flags,
			(intmax_t)offset, owned ? "true" : "false", stream ? (S_ISSOCK(state.st_mode) ? ",\"type\":\"socket\"" : ",\"type\":\"pipe\"") : null_device(&state) ? ",\"type\":\"null\"" : S_ISDIR(state.st_mode) ? ",\"type\":\"directory\"" : "");
		if (length < 0 || (size_t)length >= capacity - used) goto done;
		used += (size_t)length;
		if (stream) {
			unsigned char *bytes;
			int eof, size = queue.stream >= 4 ? socket_bytes(pin, &bytes, &eof) : pipe_bytes(pin, &bytes, &eof);
			if (size < 0) goto done;
			if ((size_t)size * 2 + 256 >= capacity - used) { free(bytes); goto done; }
			used += (size_t)sprintf(line + used, ",\"queueHex\":\"");
			for (int byte = 0; byte < size; byte++) {
				line[used++] = "0123456789abcdef"[bytes[byte] >> 4]; line[used++] = "0123456789abcdef"[bytes[byte] & 15];
			}
			free(bytes); used += (size_t)sprintf(line + used, "\",\"eof\":%s,\"capacity\":%d", eof ? "true" : "false", queue.capacity);
			if (queue.stream >= 4) used += (size_t)sprintf(line + used, ",\"socket\":{\"shutdown\":%d,\"peerShutdown\":%d,\"peerInode\":%u,\"peerQueued\":%d,\"allocated\":%d}", queue.shutdown, queue.peer_shutdown, queue.peer_inode, queue.peer_queued, queue.allocated);
		}
		if (capacity - used < 2) goto done;
		line[used++] = '}';
	}
	line[used] = 0;
	result = 0;
done:
	while (count) close(pins[--count]);
	return result;
}

static int open_tracee_output(struct decision_job *job, unsigned fd) {
	int duplicate = duplicate_tracee_fd(job, fd);
	if (duplicate >= 0) return duplicate;
	char path[64];
	if (snprintf(path, sizeof(path), "/proc/%ld/fd/%u", (long)job->pid, fd) >= (int)sizeof(path)) {
		errno = ENAMETOOLONG;
		return -1;
	}
	/* This fallback opens a new description; never change flags on a pidfd duplicate. */
	int output = open(path, O_WRONLY | O_CLOEXEC | O_NONBLOCK);
	if (output < 0) return -1;
	struct stat state;
	int flags = fcntl(output, F_GETFL);
	if (fstat(output, &state) == 0 && S_ISFIFO(state.st_mode) && flags >= 0 &&
		fcntl(output, F_SETFL, flags & ~(O_NONBLOCK | 0x8000)) >= 0) return output;
	close(output);
	return -1;
}

static int position_matches(const struct file_position *position) {
	struct stat state, named;
	if (fstat(position->duplicate, &state) < 0) return 0;
	if (position->stream) {
		unsigned char *bytes;
		struct file_position current = {0};
		int eof, size = position->stream >= 4 ? socket_bytes(position->duplicate, &bytes, &eof) : pipe_bytes(position->duplicate, &bytes, &eof);
		int matches = stream_state(position->duplicate, &current) == 0 && current.stream == position->stream &&
			current.capacity == position->capacity && current.shutdown == position->shutdown && current.peer_shutdown == position->peer_shutdown && current.peer_inode == position->peer_inode &&
			current.peer_queued == position->peer_queued && current.allocated == position->allocated &&
			!position->before && position->content_length >= 0 &&
			size == position->content_length && eof == position->eof && position->after <= size &&
			(!size || !memcmp(bytes, position->content, (size_t)size)) &&
			(uintmax_t)state.st_dev == position->device && (uintmax_t)state.st_ino == position->inode &&
			fcntl(position->duplicate, F_GETFL) == position->flags && !(position->flags & ~(O_ACCMODE | O_APPEND | O_NONBLOCK | 0x8000)) &&
			!(position->after_flags & ~(O_ACCMODE | O_APPEND | O_NONBLOCK | 0x8000));
		free(bytes); return matches;
	}
	if ((position->flags & O_PATH) && (position->before || position->after ||
		position->content_length != -1 || position->after_flags != position->flags)) return 0;
	if (S_ISDIR(state.st_mode)) {
		char link[64], endpoint[PATH_MAX];
		snprintf(link, sizeof(link), "/proc/self/fd/%d", position->duplicate);
		ssize_t length = readlink(link, endpoint, sizeof(endpoint));
		if (!position->path || length < 0 || strlen(position->path) != (size_t)length || memcmp(endpoint, position->path, (size_t)length) ||
			lstat(position->path, &named) < 0 || named.st_dev != state.st_dev || named.st_ino != state.st_ino) return 0;
	}
	return (S_ISREG(state.st_mode) || ((null_device(&state) || S_ISDIR(state.st_mode)) &&
		!position->before && !position->after && position->content_length == -1)) &&
		(uintmax_t)state.st_dev == position->device && (uintmax_t)state.st_ino == position->inode &&
		fcntl(position->duplicate, F_GETFL) == position->flags &&
		descriptor_seek(position->duplicate, position->flags, 0, SEEK_CUR) == position->before;
}

/* Only the tracer thread may mutate a held image. Each job owns its reply channel. */
static int request_tracer(struct decision_job *job, unsigned code) {
	char reply;
	return transfer(job->channel[1], &code, sizeof(code), 1) < 0 ||
		transfer(job->channel[1], &reply, 1, 0) < 0 || reply != 'Y' ? -1 : 0;
}

/* One ordered transition interpreter validates an in-memory queue graph, then applies
 * that same sequence under the tree lease. Aliases never own another copy of a queue. */
static int stream_events(struct decision_job *job, int apply) {
	if (!job->stream_count) return 0;
	struct { unsigned char *bytes; size_t length, produced; unsigned writes; int eof, shutdown, owner; } queues[MAX_POSITIONS] = {0};
	int result = -1;
	for (unsigned index = 0; index < job->position_count; index++) {
		const struct file_position *position = &job->positions[index];
		queues[index].owner = (int)index;
		if (!position->stream) continue;
		for (unsigned previous = 0; previous < index; previous++) if (position->device == job->positions[previous].device && position->inode == job->positions[previous].inode) {
			queues[index].owner = queues[previous].owner; break;
		}
		if (queues[index].owner != (int)index) continue;
		queues[index].length = (size_t)(position->content_length - position->after);
		queues[index].eof = position->eof; queues[index].shutdown = position->shutdown;
		queues[index].bytes = malloc(queues[index].length + 4096 + 1);
		if (!queues[index].bytes) goto done;
		if (queues[index].length) memcpy(queues[index].bytes, position->content + position->after, queues[index].length);
	}
	for (unsigned index = 0; index < job->stream_count; index++) {
		const struct output_event *event = &job->events[index];
		unsigned handle = 0;
		while (handle < job->position_count && job->positions[handle].descriptor != (int)event->fd) handle++;
		if (handle == job->position_count || !job->positions[handle].stream) goto done;
		const struct file_position *position = &job->positions[handle];
		int object = queues[handle].owner, peer = position->stream < 4 ? object : -1;
		if (position->peer_inode) for (unsigned other = 0; other < job->position_count; other++)
			if (job->positions[other].stream >= 4 && job->positions[other].inode == position->peer_inode) { peer = queues[other].owner; break; }
		if (event->kind < 2) {
			if ((position->flags & O_ACCMODE) == O_WRONLY || event->length > queues[object].length ||
				(event->length ? memcmp(event->data, queues[object].bytes, event->length) : !queues[object].eof)) goto done;
			if (event->kind == 0) {
				if (apply && event->length) {
					unsigned char bytes[65536]; size_t consumed = 0;
					while (consumed < event->length) {
						size_t length = event->length - consumed; if (length > sizeof(bytes)) length = sizeof(bytes);
						if (transfer(position->duplicate, bytes, length, 0) < 0 || memcmp(bytes, event->data + consumed, length)) goto done;
						consumed += length;
					}
				}
				queues[object].length -= event->length;
				memmove(queues[object].bytes, queues[object].bytes + event->length, queues[object].length);
			}
		} else if (event->kind == 2) {
			if ((position->flags & O_ACCMODE) == O_RDONLY || (queues[object].shutdown & 2) ||
				(position->stream >= 4 && (peer < 0 ? position->peer_shutdown & 1 : queues[peer].shutdown & 1)) ||
				(queues[object].produced += event->length) > 4096 || ++queues[object].writes > 16 ||
				(position->stream >= 4 && (uint64_t)position->allocated + queues[object].writes * 8192UL > (unsigned)position->capacity)) goto done;
			if (peer >= 0) {
				if (queues[peer].length + event->length > (size_t)job->positions[peer].content_length + 4096 ||
					(position->stream < 4 && queues[peer].length + event->length > (unsigned)position->capacity)) goto done;
				memcpy(queues[peer].bytes + queues[peer].length, event->data, event->length); queues[peer].length += event->length;
			}
			if (apply && (position->stream >= 4 ? send(position->duplicate, event->data, event->length, MSG_DONTWAIT | MSG_NOSIGNAL)
				: write(position->duplicate, event->data, event->length)) != (ssize_t)event->length) goto done;
		} else {
			if (position->stream < 4 || event->length != 1 || !event->data[0] || event->data[0] > 3) goto done;
			queues[object].shutdown |= event->data[0];
			if (event->data[0] & 1) queues[object].eof = 1;
			if ((event->data[0] & 2) && peer >= 0) queues[peer].eof = 1;
			if (apply && shutdown(position->duplicate, event->data[0] == 3 ? SHUT_RDWR : event->data[0] == 2 ? SHUT_WR : SHUT_RD) < 0) goto done;
		}
	}
	result = 0;
done:
	for (unsigned index = 0; index < job->position_count; index++) free(queues[index].bytes);
	return result;
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
	char *descriptors = job->context = job->domain ? malloc(MAX_REQUEST_BYTES) : NULL;
	int captured = 0;
	if (descriptors) {
		/* Temporary snapshot pins must close before cancellation can retire their job. */
		pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
		captured = descriptor_context(job, descriptors, MAX_REQUEST_BYTES) == 0;
		pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
	}
	int request_length = snprintf(line, sizeof(line),
		"{\"version\":1,\"token\":\"%s\",\"execution\":\"%s\",\"pid\":%ld,\"tracer\":%ld%s",
		job->token, job->execution_id, (long)job->pid, (long)getpid(),
		captured ? ",\"descriptors\":[" : "");
	int sent = request_length > 0 && request_length < (int)sizeof(line) && transfer(connection, line, (size_t)request_length, 1) == 0 &&
		(!captured || transfer(connection, descriptors, strlen(descriptors), 1) == 0) && transfer(connection, captured ? "]}\n" : "}\n", captured ? 3 : 2, 1) == 0;
	free(descriptors); job->context = NULL;
	if (!sent || read_line(connection, line, sizeof(line)) < 0) return -1;
	if (!strcmp(line, "C")) return -1;
	if (!strcmp(line, "F")) return -2;
	if (!strcmp(line, "O")) return connection;
	if (sscanf(line, "P %u %u %zu %u %u", &code, &job->count, &total, &job->position_count, &job->stream_count) != 5 || code > 255 ||
		job->count > MAX_OUTPUT_EVENTS || total > MAX_OUTPUT_BYTES || job->position_count > MAX_POSITIONS || job->stream_count > 1024) return -1;
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
		unsigned path_length;
		if (read_line(connection, line, sizeof(line)) < 0 ||
			sscanf(line, "S %d %ju %ju %d %" SCNd64 " %" SCNd64 " %" SCNd64 " %d %u %d %d %d %d %u %d %d", &position->descriptor,
				&position->device, &position->inode, &position->flags, &position->before, &position->after, &position->content_length, &position->after_flags, &path_length, &position->eof,
				&position->capacity, &position->shutdown, &position->peer_shutdown, &position->peer_inode, &position->peer_queued, &position->allocated) != 16 || position->eof < -1 || position->eof > 1 ||
			position->capacity < 0 || position->shutdown < 0 || position->shutdown > 3 || position->peer_shutdown < 0 || position->peer_shutdown > 3 ||
			position->peer_queued < 0 || position->allocated < 0 ||
			position->descriptor < 0 || position->before < 0 || position->after < 0 || position->content_length < -1 ||
			path_length >= PATH_MAX || path_length > total - received || position->after_flags < 0 || ((position->flags ^ position->after_flags) & ~(O_APPEND | O_NONBLOCK)) ||
			(position->content_length >= 0 && (uint64_t)position->content_length > total - received - path_length)) goto decline;
		if (path_length) {
			position->path = calloc((size_t)path_length + 1, 1);
			if (!position->path || transfer(connection, position->path, path_length, 0) < 0) return -1;
			if (*position->path != '/' || strlen(position->path) != path_length) goto decline;
			received += path_length;
		}
		if (position->content_length > 0) {
			position->content = malloc((size_t)position->content_length);
			if (!position->content || transfer(connection, position->content, (size_t)position->content_length, 0) < 0) return -1;
		}
		if (position->content_length >= 0) received += (size_t)position->content_length;
	}
	job->events = calloc(job->count + job->stream_count + 1, sizeof(*job->events));
	if (!job->events) return -1;
	for (unsigned index = 0; index < job->stream_count + job->count; index++) {
		size_t length;
		unsigned fd, kind = 0;
		if (read_line(connection, line, sizeof(line)) < 0 || (index < job->stream_count
			? sscanf(line, "Q %u %u %zu", &kind, &fd, &length) != 3 || kind > 3 || length > MAX_INPUT_BYTES
			: sscanf(line, "O %u %zu", &fd, &length) != 2 || (fd != 1 && fd != 2)) || length > total - received) return -1;
		struct output_event *event = &job->events[index];
		event->fd = fd; event->kind = kind;
		event->length = length;
		if (index >= job->stream_count && job->outputs[fd] < 0) {
			/* Publish acquired descriptors before a cancellation point can retire the job. */
			pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
			job->outputs[fd] = open_tracee_output(job, fd);
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
		position->duplicate = duplicate_tracee_fd(job, (unsigned)position->descriptor);
		pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
		struct stat state;
		if (position->duplicate >= 0 && fstat(position->duplicate, &state) == 0 && (S_ISFIFO(state.st_mode) || S_ISSOCK(state.st_mode))) {
			struct file_position current = {0};
			if (stream_state(position->duplicate, &current) < 0) goto decline;
			position->stream = current.stream;
		}
		if (position->stream && position->eof < 0) position->eof = 1;
		struct resource_object *object = job->domain ? origin_object(job->domain, descriptor_origin(job->process, position->descriptor)) : NULL;
		if (position->stream && (!object || (position->stream >= 4 ? !object->channel : !object->pipe))) goto decline;
		if (position->duplicate < 0 || !position_matches(position)) goto decline;
		if (job->domain && (job->domain->escaped || job->domain->uncertain || !job->process->table ||
			job->process->table->active || !descriptor_origin(job->process, position->descriptor).id)) goto decline;
		for (unsigned previous = 0; previous < index; previous++) {
			const struct file_position *other = &job->positions[previous];
			if (!position->stream && position->content_length >= 0 && other->content_length >= 0 &&
				position->device == other->device && position->inode == other->inode) goto decline;
			if (position->stream && position->device == other->device && position->inode == other->inode) {
				if (!other->stream || position->after != other->after) goto decline;
				position->queue_alias = 1;
			}
			long same = syscall(SYS_kcmp, getpid(), getpid(), KCMP_FILE, position->duplicate, other->duplicate);
			if (same < 0) goto decline;
			if (same == 0) {
				if (position->before != other->before || position->after != other->after || position->after_flags != other->after_flags) goto decline;
				position->alias = 1;
			}
		}
		if (!position->stream && position->content_length >= 0) {
			char path[64]; struct stat state;
			snprintf(path, sizeof(path), "/proc/self/fd/%d", position->duplicate);
			pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
			position->writer = open(path, O_WRONLY | O_CLOEXEC);
			pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
			if (position->writer < 0 || fstat(position->writer, &state) < 0 ||
				(uintmax_t)state.st_dev != position->device || (uintmax_t)state.st_ino != position->inode) goto decline;
		}
	}
	if (stream_events(job, 0) < 0) goto decline;
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
		if (position->stream && !position->queue_alias && transfer(position->duplicate, position->content, (size_t)position->after, 0) < 0) return -2;
		if (position->alias) continue;
		if (position->after_flags != position->flags && (fcntl(position->duplicate, F_SETFL, position->after_flags) < 0 ||
			fcntl(position->duplicate, F_GETFL) != position->after_flags)) return -2;
		if (!position->stream && descriptor_seek(position->duplicate, position->after_flags, position->after, SEEK_SET) != position->after) return -2;
	}
	if (stream_events(job, 1) < 0) return -2;
	/* Output may feed another tracee. Release the offset lease before a pipe write can block. */
	if (job->domain && job->domain->enabled && request_tracer(job, 256) < 0) return -2;
	for (unsigned index = job->stream_count; index < job->stream_count + job->count; index++) {
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
	close(job->channel[0]); close(job->channel[1]); close(job->connection); close(job->pidfd);
	for (unsigned fd = 1; fd <= 2; fd++) close(job->outputs[fd]);
	if (job->positions) for (unsigned index = 0; index < job->position_count; index++) {
		close(job->positions[index].duplicate); close(job->positions[index].writer); free(job->positions[index].content); free(job->positions[index].path);
	}
	free(job->positions);
	free(job->context);
	free_events(job->events, job->count + job->stream_count);
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
		.connection = -1, .pidfd = -1, .outputs = {-1, -1, -1}, .result = -1, .domain = domain, .process = process };
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
	domain.processes = &processes;
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
				if (current->table->entries[index].cloexec) set_descriptor_origin(current, &domain,
					(struct descriptor_origin){.fd = current->table->entries[index].fd});
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
static int load_queue_image(struct file_position *position) {
	if (position->content) return 0;
	struct stat state; int source = open(position->path, O_RDONLY | O_CLOEXEC);
	if (source < 0) return -1;
	int valid = fstat(source, &state) == 0 && S_ISREG(state.st_mode) && state.st_size >= 0 && (uint64_t)state.st_size <= MAX_INPUT_BYTES;
	if (valid) {
		position->content_length = state.st_size; position->content = malloc((size_t)state.st_size + 1);
		valid = position->content && transfer(source, position->content, (size_t)state.st_size, 0) == 0;
	}
	close(source); return valid ? 0 : -1;
}

static int execute_descriptors(const char *manifest, const char *report, char *executable, char **command) {
	struct file_position positions[MAX_POSITIONS];
	char line[MAX_LINE];
	unsigned count = 0, close_input = 0, journal = 0, initialized = 0, inherited = 3;
	int result = 70, minimum = 3, output = -1, root_status = -1;
	FILE *input = fopen(manifest, "re");
	if (!input) return result;
	if (!fgets(line, sizeof(line), input) || sscanf(line, "FD2 %u %u %u", &count, &close_input, &journal) != 3 ||
		count > MAX_POSITIONS || close_input > 1 || journal > 1) goto done;
	for (unsigned index = 0; index < count; index++) {
		struct file_position *position = &positions[index];
		*position = (struct file_position){.duplicate = -1, .writer = -1, .object = -1, .producer = -1}; initialized++;
		unsigned length;
		int alias;
		if (!fgets(line, sizeof(line), input) || sscanf(line, "%d %d %d %" SCNd64 " %u %d %d %d %d %d",
			&position->descriptor, &alias, &position->flags, &position->before, &length, &position->stream, &position->capacity, &position->shutdown, &position->peer_shutdown, &position->peer_descriptor) != 10 ||
			position->descriptor < 0 || position->descriptor == 1 || position->descriptor == 2 || position->descriptor == INT_MAX ||
			(close_input && position->descriptor == 0) || (index && position->descriptor <= positions[index - 1].descriptor) ||
			position->before < 0 || length >= PATH_MAX || alias > position->descriptor || alias < 0 || (position->stream < 0 || position->stream > 5) ||
			position->capacity < 0 || position->shutdown < 0 || position->shutdown > 3 || position->peer_shutdown < 0 || position->peer_shutdown > 3 || position->peer_descriptor < -1) goto done;
		position->alias = (int)index;
		if (alias != position->descriptor) {
			unsigned previous = 0;
			while (previous < index && positions[previous].descriptor != alias) previous++;
			if (previous == index || positions[previous].alias != (int)previous || length ||
				positions[previous].flags != position->flags || positions[previous].before != position->before || positions[previous].stream != position->stream) goto done;
			position->alias = (int)previous;
		} else if (!length) goto done;
		position->path = calloc((size_t)length + 1, 1);
		if (!position->path || fread(position->path, 1, length, input) != length || fgetc(input) != '\n' ||
			strlen(position->path) != length || (length && *position->path != '/')) goto done;
		if (position->descriptor >= minimum) minimum = position->descriptor + 1;
		if (alias == position->descriptor && (position->flags & O_PATH)) inherited++;
	}
	if (fgetc(input) != EOF || ferror(input)) goto done;
	fclose(input); input = NULL;
	if (has_unmodeled_descriptors((int)inherited) != 0) goto done;
	inherited = 3;
	/* ADDFD cannot inject O_PATH; the sandbox preserves these caller-owned images. */
	for (unsigned index = 0; index < count; index++) {
		struct file_position *position = &positions[index];
		if (position->alias == (int)index && (position->flags & O_PATH)) {
			position->duplicate = fcntl((int)inherited++, F_DUPFD_CLOEXEC, minimum);
			if (position->duplicate < 0) goto done;
		}
	}
	for (unsigned fd = 3; fd < inherited; fd++) close((int)fd);
	for (unsigned index = 0; index < count; index++) {
		struct file_position *position = &positions[index];
		if (position->alias != (int)index) {
			position->duplicate = fcntl(positions[position->alias].duplicate, F_DUPFD_CLOEXEC, minimum);
			position->object = positions[position->alias].object;
		} else if (position->duplicate >= 0) {
			/* O_PATH images and the other end of an already reconstructed socketpair. */
		} else if (position->stream) {
			if (position->before || (position->flags & ~(O_ACCMODE | O_APPEND | O_NONBLOCK | 0x8000))) goto done;
			for (unsigned previous = 0; previous < index; previous++) if (positions[previous].stream && !strcmp(position->path, positions[previous].path)) {
				position->object = positions[previous].object; break;
			}
			int fd;
			if (position->object >= 0) {
				if (position->stream >= 4) goto done;
				snprintf(line, sizeof(line), "/proc/self/fd/%d", positions[position->object].duplicate);
				fd = open(line, position->flags | O_CLOEXEC);
			} else {
				int fds[2], valid = 1; struct file_position *peer = NULL;
				if (position->stream >= 4 && position->peer_descriptor >= 0) {
					for (unsigned other = index + 1; other < count; other++) if (positions[other].descriptor == position->peer_descriptor) { peer = &positions[other]; break; }
					if (!peer || peer->stream != 4 || peer->peer_descriptor != position->descriptor || peer->alias != peer - positions || load_queue_image(peer) < 0) goto done;
				}
				if (load_queue_image(position) < 0 || (position->stream >= 4 ? socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0, fds) : pipe2(fds, O_CLOEXEC | O_NONBLOCK)) < 0) goto done;
				if (position->stream >= 4) {
					int capacity = position->capacity / 2;
					valid = setsockopt(fds[0], SOL_SOCKET, SO_SNDBUF, &capacity, sizeof(capacity)) == 0 && socket_option(fds[0], SO_SNDBUF) == position->capacity;
					if (peer && valid) {
						capacity = peer->capacity / 2;
						valid = setsockopt(fds[1], SOL_SOCKET, SO_SNDBUF, &capacity, sizeof(capacity)) == 0 && socket_option(fds[1], SO_SNDBUF) == peer->capacity &&
							transfer(fds[0], peer->content, (size_t)peer->content_length, 1) == 0 && fcntl(fds[1], F_SETFL, peer->flags) == 0;
					}
				} else if (fcntl(fds[1], F_GETPIPE_SZ) != position->capacity && fcntl(fds[1], F_SETPIPE_SZ, position->capacity) != position->capacity) valid = 0;
				if (valid) valid = transfer(fds[1], position->content, (size_t)position->content_length, 1) == 0;
				if (position->stream >= 4) {
					if (valid && position->shutdown) valid = shutdown(fds[0], position->shutdown == 3 ? SHUT_RDWR : position->shutdown == 2 ? SHUT_WR : SHUT_RD) == 0;
					if (valid && position->peer_shutdown) valid = shutdown(fds[1], position->peer_shutdown == 3 ? SHUT_RDWR : position->peer_shutdown == 2 ? SHUT_WR : SHUT_RD) == 0;
				}
				if (position->stream == 3) { int swap = fds[0]; fds[0] = fds[1]; fds[1] = swap; if (position->content_length) valid = 0; }
				if (valid) valid = fcntl(fds[0], F_SETFL, position->flags) == 0;
				if (position->stream != 1 && position->stream != 5 && valid) { position->producer = fcntl(fds[1], F_DUPFD_CLOEXEC, minimum); valid = position->producer >= 0; }
				if (peer && valid) {
					peer->object = (int)(peer - positions); peer->duplicate = fcntl(fds[1], F_DUPFD_CLOEXEC, minimum);
					peer->producer = fcntl(fds[0], F_DUPFD_CLOEXEC, minimum); valid = peer->duplicate >= 0 && peer->producer >= 0;
				}
				close(fds[1]); fd = fds[0]; position->object = (int)index;
				if (!valid) { close(fd); goto done; }
				if (position->stream < 4 && (position->flags & 0x8000)) {
					snprintf(line, sizeof(line), "/proc/self/fd/%d", fd);
					int reopened = open(line, position->flags | O_CLOEXEC); close(fd); fd = reopened;
				}
			}
			if (fd < 0) goto done;
			position->duplicate = fcntl(fd, F_DUPFD_CLOEXEC, minimum); close(fd);
		} else if (!(position->flags & O_PATH)) {
			const int allowed = O_ACCMODE | O_APPEND | O_NONBLOCK | O_DSYNC | O_SYNC | 0x8000 /* kernel O_LARGEFILE */ | O_NOATIME | O_NOFOLLOW | O_DIRECT | O_DIRECTORY;
			if ((position->flags & ~allowed) || (position->flags & O_ACCMODE) == O_ACCMODE) goto done;
			int fd = open(position->path, position->flags | O_CLOEXEC);
			if (fd < 0) goto done;
			position->duplicate = fcntl(fd, F_DUPFD_CLOEXEC, minimum);
			close(fd);
		}
		struct stat state;
		if (position->duplicate < 0 || fstat(position->duplicate, &state) < 0 || !(S_ISREG(state.st_mode) || (position->stream && (S_ISFIFO(state.st_mode) || S_ISSOCK(state.st_mode))) ||
			((null_device(&state) || S_ISDIR(state.st_mode)) && !position->before)) ||
			fcntl(position->duplicate, F_GETFL) != position->flags ||
			(!position->stream && descriptor_seek(position->duplicate, position->flags, position->before, SEEK_SET) != position->before)) goto done;
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
		for (unsigned index = 0; index < count; index++) { close(positions[index].duplicate); close(positions[index].producer); }
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
	if (ftruncate(output, 0) < 0 || lseek(output, 0, SEEK_SET) != 0 || dprintf(output, "FD2 %u\n", count) < 0) goto done;
	for (unsigned index = 0; index < count; index++) {
		struct file_position *position = &positions[index];
		int flags = fcntl(position->duplicate, F_GETFL);
		off_t offset;
		if (position->stream && !journal) {
			unsigned char *bytes;
			int eof, remaining = pipe_bytes(position->duplicate, &bytes, &eof);
			const struct file_position *image = &positions[position->object];
			offset = image->content_length - remaining;
			int valid = remaining >= 0 && (position->stream >= 4 || eof == (position->stream == 1)) && offset >= 0 && (!remaining || !memcmp(bytes, image->content + offset, (size_t)remaining));
			free(bytes); if (!valid) goto done;
		} else offset = position->stream ? 0 : descriptor_seek(position->duplicate, flags, 0, SEEK_CUR);
		struct stat state;
		if (offset < 0 || flags < 0 || fstat(position->duplicate, &state) < 0 || dprintf(output, "%d %d %jd %ju %ju\n",
			position->descriptor, flags, (intmax_t)offset, (uintmax_t)state.st_dev, (uintmax_t)state.st_ino) < 0) goto done;
	}
	result = WIFEXITED(root_status) ? WEXITSTATUS(root_status) : 128 + WTERMSIG(root_status);
done:
	if (input) fclose(input);
	if (output >= 0 && close(output) < 0) result = 70;
	for (unsigned index = 0; index < initialized; index++) { free(positions[index].path); free(positions[index].content); close(positions[index].duplicate); close(positions[index].producer); }
	if (root_status >= 0 && WIFSIGNALED(root_status)) { signal(WTERMSIG(root_status), SIG_DFL); raise(WTERMSIG(root_status)); }
	return result;
}

int main(int argc, char **argv) {
	int dispatched = image_dispatch(argc, argv);
	if (dispatched >= 0) return dispatched;
	if (argc == 2 && !strcmp(argv[1], "--protocol-version")) {
		puts("19");
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
		int extra = has_unmodeled_descriptors(3);
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
