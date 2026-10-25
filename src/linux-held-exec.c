#define _GNU_SOURCE
#include <dirent.h>
#include <elf.h>
#include <linux/prctl.h>
#include <linux/capability.h>
#ifndef PI_SPEC_AUTHORITY_ONLY
#define PI_SPEC_AUTHORITY_ONLY 0
#endif
#include <sys/uio.h>
#include <sys/auxv.h>
#include <ctype.h>
#include <errno.h>
#include <endian.h>
#include <fcntl.h>
#include <sys/eventfd.h>
#include <limits.h>
#include <inttypes.h>
#include <linux/audit.h>
#include <linux/kcmp.h>
#include <linux/magic.h>
#include <linux/mount.h>
#include <linux/openat2.h>
#include <sys/mount.h>
#include <linux/netlink.h>
#include <linux/rtnetlink.h>
#include <linux/sock_diag.h>
#include <linux/unix_diag.h>
#include <sched.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/file.h>
#include <sys/vfs.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/signalfd.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/sysmacros.h>
#include <sys/ptrace.h>
#include <sys/prctl.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/user.h>
#include <sys/wait.h>
#include <unistd.h>

static const long options = PTRACE_O_TRACEFORK | PTRACE_O_TRACEVFORK |
	PTRACE_O_TRACEVFORKDONE | PTRACE_O_TRACECLONE | PTRACE_O_TRACEEXEC | PTRACE_O_EXITKILL;

/* Stable Node-API types. Weak imports keep the same source usable as a static
 * tracer and as the image library loaded by programs without a Node runtime. */
typedef void *napi_env, *napi_value, *napi_callback_info;
typedef napi_value (*napi_callback)(napi_env, napi_callback_info);
extern int napi_get_cb_info(napi_env, napi_callback_info, size_t *, napi_value *, napi_value *, void **) __attribute__((weak));
extern int napi_get_value_int32(napi_env, napi_value, int32_t *) __attribute__((weak));
extern int napi_get_buffer_info(napi_env, napi_value, void **, size_t *) __attribute__((weak));
extern int napi_create_int32(napi_env, int32_t, napi_value *) __attribute__((weak));
extern int napi_create_function(napi_env, const char *, size_t, napi_callback, void *, napi_value *) __attribute__((weak));
extern int napi_get_undefined(napi_env, napi_value *) __attribute__((weak));
extern int napi_call_function(napi_env, napi_value, napi_value, size_t, const napi_value *, napi_value *) __attribute__((weak));
static __thread int startup_gate[2] = {-1, -1};
static __thread pid_t startup_observer;
static pthread_once_t startup_registration = PTHREAD_ONCE_INIT;
static int startup_registered;

static void startup_child(void) {
	if (startup_gate[0] < 0 || startup_observer <= 0) return;
	pid_t pid = getpid(); char ready;
	struct pollfd channel = {.fd = startup_gate[0], .events = POLLIN};
	/* Only async-signal-safe operations here, before libuv's original child setup.
	 * A missing observer leaves the original exec and its error channel intact. */
	if (!prctl(PR_SET_PTRACER, (unsigned long)startup_observer, 0, 0, 0)) {
		if (send(channel.fd, &pid, sizeof(pid), MSG_NOSIGNAL) == sizeof(pid)) {
			int status;
			do { status = poll(&channel, 1, 5000); } while (status < 0 && errno == EINTR);
			if (status > 0 && read(channel.fd, &ready, 1) != 1) ready = 0;
		}
		(void)prctl(PR_SET_PTRACER, 0, 0, 0, 0);
	}
	close(startup_gate[0]); startup_gate[0] = -1;
}

static void register_startup(void) { startup_registered = pthread_atfork(NULL, NULL, startup_child) == 0; }

/* One detached read-only mount tree per capture. The raw child never enters
 * libc's atfork launch gate and uses no allocator, stdio, or inherited locks. */
static int readonly_root(void) {
	int channel[2], descriptor = -1, error = 0, status = 0;
	char uid[48], gid[48];
	int uid_length = snprintf(uid, sizeof(uid), "0 %u 1\n", getuid()), gid_length = snprintf(gid, sizeof(gid), "0 %u 1\n", getgid());
	if (socketpair(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0, channel)) return -1;
	pid_t parent = getpid(), child = syscall(SYS_clone, SIGCHLD, 0, NULL, NULL, 0);
	if (child == 0) {
		close(channel[0]);
		if (syscall(SYS_prctl, PR_SET_PDEATHSIG, SIGKILL, 0, 0, 0) || getppid() != parent ||
			syscall(SYS_unshare, CLONE_NEWUSER | CLONE_NEWNS)) { error = errno ? errno : ECHILD; goto publish; }
		const char *names[] = {"/proc/self/uid_map", "/proc/self/setgroups", "/proc/self/gid_map"};
		const char *values[] = {uid, "deny", gid};
		int lengths[] = {uid_length, 4, gid_length};
		for (unsigned i = 0; i < 3; i++) {
			int fd = syscall(SYS_openat, AT_FDCWD, names[i], O_WRONLY | O_CLOEXEC, 0);
			if (fd < 0) { if (i == 1 && errno == ENOENT) continue; error = errno; goto publish; }
			ssize_t count = syscall(SYS_write, fd, values[i], lengths[i]); error = count == lengths[i] ? 0 : errno ? errno : EIO;
			close(fd); if (error) goto publish;
		}
		if (syscall(SYS_mount, NULL, "/", NULL, MS_REC | MS_PRIVATE, NULL)) { error = errno; goto publish; }
		descriptor = syscall(SYS_open_tree, AT_FDCWD, "/", OPEN_TREE_CLONE | OPEN_TREE_CLOEXEC | AT_RECURSIVE);
		if (descriptor < 0) { error = errno; goto publish; }
		struct mount_attr attributes = {.attr_set = MOUNT_ATTR_RDONLY};
		if (syscall(SYS_mount_setattr, descriptor, "", AT_EMPTY_PATH | AT_RECURSIVE, &attributes, sizeof(attributes))) error = errno;
	publish:;
		char control[CMSG_SPACE(sizeof(descriptor))] = {0};
		struct iovec data = {.iov_base = &error, .iov_len = sizeof(error)};
		struct msghdr message = {.msg_iov = &data, .msg_iovlen = 1};
		if (!error) {
			message.msg_control = control; message.msg_controllen = sizeof(control);
			struct cmsghdr *header = CMSG_FIRSTHDR(&message);
			header->cmsg_level = SOL_SOCKET; header->cmsg_type = SCM_RIGHTS; header->cmsg_len = CMSG_LEN(sizeof(descriptor));
			*(int *)CMSG_DATA(header) = descriptor;
		}
		(void)syscall(SYS_sendmsg, channel[1], &message, MSG_NOSIGNAL);
		syscall(SYS_exit_group, 0); __builtin_unreachable();
	}
	if (child < 0) error = errno;
	close(channel[1]);
	if (child < 0) { close(channel[0]); errno = error; return -1; }
	char control[CMSG_SPACE(sizeof(descriptor))] = {0};
	struct iovec data = {.iov_base = &error, .iov_len = sizeof(error)};
	struct msghdr message = {.msg_iov = &data, .msg_iovlen = 1, .msg_control = control, .msg_controllen = sizeof(control)};
	ssize_t count; do { count = recvmsg(channel[0], &message, MSG_CMSG_CLOEXEC); } while (count < 0 && errno == EINTR);
	if (count != sizeof(error) || message.msg_flags & (MSG_TRUNC | MSG_CTRUNC)) error = EIO;
	struct cmsghdr *header = CMSG_FIRSTHDR(&message);
	if (header && header->cmsg_level == SOL_SOCKET && header->cmsg_type == SCM_RIGHTS && header->cmsg_len == CMSG_LEN(sizeof(descriptor)))
		descriptor = *(int *)CMSG_DATA(header);
	close(channel[0]);
	pid_t reaped; do { reaped = waitpid(child, &status, 0); } while (reaped < 0 && errno == EINTR);
	if (reaped != child || !WIFEXITED(status) || WEXITSTATUS(status) || descriptor < 0) error = error ? error : EIO;
	if (error) { if (descriptor >= 0) close(descriptor); errno = error; return -1; }
	struct statfs filesystem;
	if (fstatfs(descriptor, &filesystem)) error = errno;
	else if (filesystem.f_type != EXT4_SUPER_MAGIC) error = EXDEV;
	if (error) { close(descriptor); errno = error; return -1; }
	return descriptor;
}

/* Open a gate (0), bind its observer PID (>0), or release the parent ends (-1).
 * Thread-local state cannot lend one worker's launch to another callback. */
static napi_value startup_bridge(napi_env env, napi_callback_info info) {
	size_t count = 3; napi_value arguments[3], result; int32_t operation, value = -1;
	if (napi_get_cb_info(env, info, &count, arguments, NULL, NULL) || !count ||
		napi_get_value_int32(env, arguments[0], &operation)) return NULL;
	if (count >= 2) {
		void *bytes; size_t length;
		if (napi_get_buffer_info(env, arguments[1], &bytes, &length) || length > 1024 * 1024) return NULL;
		if (operation == -3) {
			/* Two little-endian int64 values followed by one terminated path; keep its access time. */
			uint64_t fields[2];
			if (length < sizeof(fields) + 2 || strnlen((char *)bytes + sizeof(fields), length - sizeof(fields)) != length - sizeof(fields) - 1) return NULL;
			memcpy(fields, bytes, sizeof(fields));
			struct timespec times[2] = {{ .tv_nsec = UTIME_OMIT }, { .tv_sec = (int64_t)le64toh(fields[0]), .tv_nsec = (int64_t)le64toh(fields[1]) }};
			if (times[1].tv_nsec < 0 || times[1].tv_nsec > 999999999) return NULL;
			value = utimensat(AT_FDCWD, (char *)bytes + sizeof(fields), times, AT_SYMLINK_NOFOLLOW);
			if (value < 0 && errno == ENOENT) value = 0;
		} else if (operation == -5) {
			if (length) return NULL;
			value = readonly_root();
		} else if (operation == -6) {
			/* Root fd and open flags, followed by one terminated absolute path. */
			uint32_t fields[2];
			if (length < 10 || strnlen((char *)bytes + 8, length - 8) != length - 9) return NULL;
			memcpy(fields, bytes, sizeof(fields));
			if (le32toh(fields[1]) & ~(O_PATH | O_NOFOLLOW | O_DIRECTORY | O_NONBLOCK | O_NOATIME | O_CLOEXEC)) { errno = EINVAL; value = -1; }
			else {
				struct open_how how = {.flags = le32toh(fields[1]) | O_CLOEXEC,
					.resolve = RESOLVE_IN_ROOT | RESOLVE_NO_XDEV};
				value = syscall(SYS_openat2, (int32_t)le32toh(fields[0]), (char *)bytes + 8, &how, sizeof(how));
			}
		} else if (operation == -7) {
			uint32_t fd; if (length < 5) return NULL; memcpy(&fd, bytes, sizeof(fd));
			struct stat entry;
			if (fstat((int32_t)le32toh(fd), &entry)) value = -1;
			else if (!S_ISLNK(entry.st_mode)) { errno = EINVAL; value = -1; }
			else value = syscall(SYS_readlinkat, (int32_t)le32toh(fd), "", (char *)bytes + 4, length - 4);

		} else {
			if (length < 24) return NULL;
			value = syscall(SYS_getdents64, operation, bytes, length);
		}
		if (value >= 0 && (operation == -5 || operation == -6)) {
			/* Native ownership never crosses this synchronous call. Node tracks the adopted
			 * duplicate even if its worker terminates while the JavaScript callback runs. */
			napi_value fd, receiver;
			int failed = count != 3 || napi_create_int32(env, value, &fd) || napi_get_undefined(env, &receiver) ||
				napi_call_function(env, receiver, arguments[2], 1, &fd, &result);
			close(value); return failed ? NULL : result;
		}
		if (value < 0) value = -errno;
	} else if (operation == -11) {
		value = 2; /* Filesystem ABI: synchronous adoption, ext4 read-only root, no mount crossings. */
	} else if (!operation && startup_gate[0] < 0) {
		if (!socketpair(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0, startup_gate)) value = startup_gate[1];
	} else if (operation > 0 && startup_gate[0] >= 0 && !startup_observer) {
		startup_observer = operation; close(startup_gate[1]); startup_gate[1] = -1; value = 0;
	} else if (operation == -2 && startup_gate[0] < 0) {
		value = prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0);
	} else if (operation == -1) {
		close(startup_gate[0]); close(startup_gate[1]); startup_gate[0] = startup_gate[1] = -1; startup_observer = 0; value = 0;
	}
	return napi_create_int32(env, value, &result) ? NULL : result;
}

napi_value napi_register_module_v1(napi_env env, napi_value exports) {
	(void)exports; napi_value result;
	if (!napi_create_function || !napi_get_cb_info || !napi_get_value_int32 || !napi_create_int32 || !napi_get_buffer_info || !napi_get_undefined || !napi_call_function ||
		pthread_once(&startup_registration, register_startup) || !startup_registered) return NULL;
	return napi_create_function(env, "startup", (size_t)-1, startup_bridge, NULL, &result) ? NULL : result;
}

#define MAX_LINE 32768
#define MAX_OUTPUT_EVENTS 65536
#define MAX_OUTPUT_BYTES (512UL * 1024 * 1024)
#define MAX_POSITIONS 64
#define MAX_HANDLES 256
#define MAX_OBJECTS 1024
#define MAX_INPUT_BYTES (2UL * 1024 * 1024)
#define MAX_REQUEST_BYTES (MAX_INPUT_BYTES * 2 + MAX_LINE)

struct queue_rights { unsigned count; int fds[MAX_POSITIONS]; };
struct queue_message { size_t start, end; struct queue_rights rights; };
struct queue_messages { unsigned count; struct queue_message *entries; };
struct ofd_lock { int kind, type; int64_t start, length; };
struct file_locks { unsigned count; struct ofd_lock *entries; };
struct file_position {
	/* stream: 0=file, 1/2=pipe input (closed/live), 3=pipe output, 4/5=Unix endpoint, 6=event counter. */
	int descriptor, duplicate, writer, flags, after_flags, alias, stream, queue_alias, eof, producer, object;
	int capacity, shutdown, peer_shutdown, peer_queued, allocated, peer_descriptor, outside, directory, installed;
	unsigned peer_inode, event, socket_type;
	uintmax_t device, inode;
	int64_t before, after;
	int64_t content_length;
	unsigned char *content;
	char *path;
	struct queue_messages messages;
	struct file_locks locks;
};

struct descriptor_origin { int fd, cloexec, access; unsigned long id; unsigned object; };
struct resource_object {
	dev_t device; ino_t inode; mode_t type;
	unsigned internal, foreign, pipe, event, messages, socket_type, anonymous;
	unsigned long channel;
};
struct descriptor_table {
	unsigned references, count, active, complete;
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
	size_t length, requested;
	unsigned char *data;
};

struct process_image;
struct file_reference { dev_t device; ino_t inode; int access, pin, queue; };
struct decision_job {
	struct process_image *image;
	pthread_t thread;
	pid_t pid;
	const char *socket_path, *token, *execution_id;
	int channel[2], connection, outputs[3], result, pidfd, holder_control; pid_t holder;
	struct output_event *events;
	unsigned count, resource_count;
	struct file_position *positions;
	unsigned position_count;
	struct descriptor_domain *domain;
	struct traced_process *process;
	char *context;
	struct file_reference *references;
	unsigned reference_count;
	int reference_state, needs_tracking;
	struct file_position *captures;
	unsigned capture_count;
};

struct traced_process {
	pid_t pid;
	int fd, armed, stopped, pending, delivered, listening, historical, awaiting_parent, vforking;
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

static int process_open(pid_t pid, const char *name, int flags) {
	char path[64]; snprintf(path, sizeof(path), "/proc/%d/%s", pid, name);
	return open(path, flags);
}
static FILE *process_file(pid_t pid, const char *name) {
	char path[64]; snprintf(path, sizeof(path), "/proc/%d/%s", pid, name);
	return fopen(path, "re");
}
static FILE *descriptor_file(pid_t pid, int fd) {
	char path[64]; snprintf(path, sizeof(path), "/proc/%ld/fdinfo/%d", (long)pid, fd);
	return fopen(path, "re");
}
static int descriptor_stat(pid_t pid, int fd, struct stat *state) {
	char path[64]; snprintf(path, sizeof(path), "/proc/%ld/fd/%d", (long)pid, fd);
	return stat(path, state);
}
/* Reopening procfs creates a new description; it never substitutes for an OFD duplicate. */
static int reopen_descriptor(int fd, int flags) {
	char path[64]; snprintf(path, sizeof(path), "/proc/self/fd/%d", fd);
	return open(path, flags);
}

/* A continuation carries private userspace state, never another process's PID or
 * kernel handles. The existing resource transaction supplies the authoritative
 * OFDs before this image resumes at its blocked read. */
#define IMAGE_MAPS 128
#define IMAGE_BYTES (64UL * 1024 * 1024)
#define IMAGE_SCRATCH 0x100000000UL
struct image_map {
	uint64_t start, end, offset, size, device, inode;
	unsigned prot, file, no_huge_pages;
	char path[PATH_MAX];
};
struct image_header {
	uint64_t magic, length;
	unsigned count, reserved;
	struct image_map maps[IMAGE_MAPS];
	struct user_regs_struct registers;
	unsigned char xstate[65536]; size_t xstate_size;
	struct prctl_mm_map mm; unsigned char auxv[4096]; size_t auxv_size;
	unsigned long sigmask, clear_tid, robust, robust_length;
	struct { uint64_t pointer; uint32_t size, signature, flags, pad; } rseq;
	struct { unsigned long handler, flags, restorer, mask; } actions[65];
	stack_t alternate;
	struct rlimit limits[RLIM_NLIMITS];
};
struct process_image {
	struct image_header header;
	unsigned char *data;
	void *allocation;
	int files[IMAGE_MAPS];
	unsigned fd_count;
	struct { int fd, flags, source; int64_t offset; uintmax_t device, inode; } fds[MAX_HANDLES];
	pid_t producer;
};

static void image_free(struct process_image *image) {
	if (!image) return;
	for (unsigned i = 0; i < IMAGE_MAPS; i++) if (image->files[i] >= 0) close(image->files[i]);
	free(image->allocation); free(image);
}
static struct process_image *image_new(void) {
	struct process_image *image = calloc(1, sizeof(*image));
	if (image) for (unsigned i = 0; i < IMAGE_MAPS; i++) image->files[i] = -1;
	return image;
}
static int image_special(const struct image_map *map) {
	return !strcmp(map->path, "[vdso]") || !strcmp(map->path, "[vvar]") ||
		!strcmp(map->path, "[vvar_vclock]") || !strcmp(map->path, "[vsyscall]");
}
static int image_wait(pid_t pid, int *status) {
	pid_t result;
	do { result = waitpid(pid, status, __WALL | __WNOTHREAD); } while (result < 0 && errno == EINTR);
	return result == pid && WIFSTOPPED(*status) ? 0 : -1;
}
static int image_memory(pid_t pid, uint64_t address, void *data, size_t bytes, int writing) {
	int fd = process_open(pid, "mem", (writing ? O_RDWR : O_RDONLY) | O_CLOEXEC);
	if (fd < 0) return -1;
	int result = lseek(fd, (off_t)address, SEEK_SET) == (off_t)address ? transfer(fd, data, bytes, writing) : -1;
	close(fd); return result;
}
/* Confinement owns CORE/NPROC; elapsed CPU and realtime budgets are not in the image. */
static int image_limits(pid_t pid, struct image_header *header, int writing) {
	for (unsigned resource = 0; resource < RLIM_NLIMITS; resource++) if (resource != RLIMIT_CORE && resource != RLIMIT_NPROC) {
		struct rlimit *limit = &header->limits[resource];
		if ((!writing && prlimit(pid, resource, NULL, limit)) || limit->rlim_cur > limit->rlim_max ||
			((resource == RLIMIT_CPU || resource == RLIMIT_RTTIME) && limit->rlim_cur != RLIM_INFINITY) ||
			(writing && prlimit(pid, resource, limit, NULL))) return -1;
	}
	return 0;
}
static long image_invoke(pid_t pid, unsigned long at, long number, const unsigned long args[6]) {
	struct user_regs_struct saved, call;
	if (ptrace(PTRACE_GETREGS, pid, 0, &saved) < 0) return -1;
	call = saved;
	unsigned long original = 0; int bootstrap = !at, status;
	if (bootstrap) {
		at = saved.rip; errno = 0; original = (unsigned long)ptrace(PTRACE_PEEKTEXT, pid, at, 0);
		if (errno || ptrace(PTRACE_POKETEXT, pid, at, (original & ~0xffffffUL) | 0xcc050fUL) < 0) return -1;
	}
	call.rip = at; call.rax = number; call.orig_rax = (unsigned long)-1;
	call.rdi = args[0]; call.rsi = args[1]; call.rdx = args[2]; call.r10 = args[3]; call.r8 = args[4]; call.r9 = args[5];
	int unmapping = at == IMAGE_SCRATCH && number == SYS_munmap && args[0] == IMAGE_SCRATCH;
	if (ptrace(PTRACE_SETREGS, pid, 0, &call) < 0) return -1;
	for (unsigned step = 0; step < (unmapping ? 2U : 1U); step++)
		if (ptrace(unmapping ? PTRACE_SYSCALL : PTRACE_CONT, pid, 0, 0) < 0 || image_wait(pid, &status) < 0 ||
			WSTOPSIG(status) != (unmapping ? SIGTRAP | 0x80 : SIGTRAP) || (unsigned)status >> 16) return -1;
	if (ptrace(PTRACE_GETREGS, pid, 0, &call) < 0 || call.rip != at + (unmapping ? 2 : 3) ||
		(bootstrap && ptrace(PTRACE_POKETEXT, pid, at, original) < 0) || ptrace(PTRACE_SETREGS, pid, 0, &saved) < 0) return -1;
	return (long)call.rax;
}
#define image_call(pid, at, number, ...) image_invoke(pid, at, number, (unsigned long[6]){__VA_ARGS__})
#define image_syscall(pid, number, ...) image_call(pid, IMAGE_SCRATCH, number, __VA_ARGS__)
static int image_scratch(pid_t pid) {
	unsigned char code[] = {0x0f, 0x05, 0xcc};
	return image_call(pid, 0, SYS_mmap, IMAGE_SCRATCH, 8192, PROT_READ | PROT_WRITE | PROT_EXEC,
		MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED_NOREPLACE, -1) == IMAGE_SCRATCH &&
		image_memory(pid, IMAGE_SCRATCH, code, sizeof(code), 1) == 0 ? 0 : -1;
}
static int image_maps(pid_t pid, struct image_header *header, int capturing) {
	char line[8192];
	FILE *input = process_file(pid, "smaps"); if (!input) return -1;
	int valid = 0, checked = 1;
	struct image_map *map = NULL;
	while (fgets(line, sizeof(line), input)) {
		if (!strncmp(line, "VmFlags:", 8)) {
			checked = 1;
			if (!capturing || !map || image_special(map)) continue;
			char *saved;
			for (char *flag = strtok_r(line + 8, " \n", &saved); flag; flag = strtok_r(NULL, " \n", &saved)) {
				if (!strcmp(flag, "gd")) { if (strcmp(map->path, "[stack]")) goto done; }
				else if (!strcmp(flag, "nh")) map->no_huge_pages = 1;
				else if (strcmp(flag, "rd") && strcmp(flag, "wr") && strcmp(flag, "ex") && strcmp(flag, "mr") &&
					strcmp(flag, "mw") && strcmp(flag, "me") && strcmp(flag, "ac") && strcmp(flag, "sd")) goto done;
			}
			continue;
		}
		uint64_t start, end, offset; char permissions[5]; unsigned major, minor; unsigned long inode; int used;
		if (sscanf(line, "%" SCNx64 "-%" SCNx64 " %4s %" SCNx64 " %x:%x %lu %n", &start, &end,
			permissions, &offset, &major, &minor, &inode, &used) != 7) continue;
		if (!checked) goto done;
		checked = 0; map = NULL;
		if (start == IMAGE_SCRATCH && end == IMAGE_SCRATCH + 8192) continue;
		if (header->count == IMAGE_MAPS) goto done;
		map = &header->maps[header->count]; map->start = start; map->end = end; map->offset = offset;
		if (end <= start || strlen(line + used) >= sizeof(map->path)) goto done;
		map->device = makedev(major, minor); map->inode = inode;
		strcpy(map->path, line + used); map->path[strcspn(map->path, "\n")] = 0;
		if (capturing && !image_special(map) && (permissions[3] != 'p' || strstr(map->path, " (deleted)") ||
			(*map->path && *map->path != '/' && strcmp(map->path, "[heap]") && strcmp(map->path, "[stack]")))) goto done;
		map->prot = (permissions[0] == 'r' ? PROT_READ : 0) | (permissions[1] == 'w' ? PROT_WRITE : 0) | (permissions[2] == 'x' ? PROT_EXEC : 0);
		map->file = *map->path == '/'; header->count++;
	}
	valid = checked && !ferror(input);
done:
	fclose(input); return valid ? 0 : -1;
}
static int image_mm(pid_t pid, struct image_header *header, unsigned long code) {
	char data[8192]; unsigned long fields[53] = {0};
	FILE *file = process_file(pid, "stat"); if (!file) return -1;
	int valid = fgets(data, sizeof(data), file) != NULL; fclose(file); if (!valid) return -1;
	char *position = strrchr(data, ')'), *saved; if (!position) return -1;
	unsigned index = 3;
	for (char *part = strtok_r(position + 2, " ", &saved); part && index < 53; part = strtok_r(NULL, " ", &saved), index++)
		if (index != 3) fields[index] = strtoull(part, NULL, 10);
	long brk = (long)fields[47];
	/* A fresh ELF image has brk == start_brk until its first instruction. */
	if (index < 52 || fields[20] != 1 || (code && (brk = image_call(pid, code, SYS_brk, 0)) < 0)) return -1;
	header->mm = (struct prctl_mm_map){.start_code = fields[26], .end_code = fields[27], .start_stack = fields[28],
		.start_data = fields[45], .end_data = fields[46], .start_brk = fields[47], .brk = (unsigned long)brk,
		.arg_start = fields[48], .arg_end = fields[49], .env_start = fields[50], .env_end = fields[51], .exe_fd = (unsigned)-1};
	int fd = process_open(pid, "auxv", O_RDONLY | O_CLOEXEC); if (fd < 0) return -1;
	ssize_t size = read(fd, header->auxv, sizeof(header->auxv)); close(fd);
	if (size <= 0 || size >= (ssize_t)sizeof(header->auxv)) return -1;
	header->auxv_size = (size_t)size; return 0;
}
/* Advance owned descriptor execution in the tracer's normal signal/event loop. */
static int duplicate_tracee_fd(struct decision_job *job, unsigned fd);
struct exec_files { unsigned refs; int fd, pin, pending; };
struct exec_call {
	struct user_regs_struct original;
	unsigned long empty, mask, code, command;
	struct exec_files *files;
	int phase, eligible, lease; pid_t tid;
};
static struct exec_files *exec_files(void **context) {
	if (!*context) { struct exec_files *files = calloc(1, sizeof(*files)); if (!files) return NULL; files->refs = 1; files->fd = files->pin = -1; *context = files; }
	return *context;
}
int pi_clone_exec_files(void **parent, void **child, int shared) {
	struct exec_files *files = exec_files(parent); if (!files || *child) return -1;
	if (shared) { files->refs++; *child = files; }
	else if (files->fd >= 0) return -1; /* Birth waits until the transient descriptor is gone. */
	return 0;
}
static int exec_pin(pid_t pid, int fd) {
	struct decision_job source = {.pid = pid, .pidfd = -1};
	int pin = duplicate_tracee_fd(&source, (unsigned)fd), error = errno; close(source.pidfd); errno = error; return pin;
}
static void exec_closed(struct exec_files *files) {
	close(files->pin); files->fd = files->pin = -1; files->pending = 0;
}
static int exec_restarting(long value) { return value == -512 || value == -513 || value == -514 || value == -516; }
static int exec_record_path(char *path, size_t size, const char *source, pid_t tid) {
	return source && snprintf(path, size, "%s.route.%d", source, tid) < (int)size ? 0 : -1;
}
static int exec_injected_fd(pid_t tid, const char *source) {
	char path[PATH_MAX + 64]; FILE *file;
	if (exec_record_path(path, sizeof(path), source, tid) < 0 || !(file = fopen(path, "re"))) return -1;
	char *field = NULL; size_t capacity = 0; int result = -1;
	for (unsigned index = 0; index < 5; index++) {
		ssize_t size = getdelim(&field, &capacity, 0, file); if (size < 0 || size > 8192 || field[size - 1]) break;
		if (index == 4) { char *end; long value = strtol(field, &end, 10); if (*field && !*end && value >= 0 && value <= INT_MAX) result = value; }
	}
	free(field); fclose(file); return result;
}
int pi_exec_call_ready(void *context) {
	struct exec_call *call = context;
	return !call || !call->lease || !flock(call->lease - 1, LOCK_EX | LOCK_NB);
}
void pi_drop_exec_files(pid_t pid, void **context, void **pending, const char *source, int executed) {
	struct exec_files *files = *context; struct exec_call *call = *pending;
	if (call) {
		if (call->files && call->files->fd < 0 && call->phase == 0) call->files->fd = exec_injected_fd(call->tid, source);
		if (call->files && call->files->fd >= 0) call->files->pending = 1;
		if (executed && call->tid && call->tid != pid) {
			char from[PATH_MAX + 64], to[PATH_MAX + 64];
			if (!exec_record_path(from, sizeof(from), source, call->tid) && !exec_record_path(to, sizeof(to), source, pid)) rename(from, to);
		}
		if (call->lease) close(call->lease - 1);
		free(call); *pending = NULL;
	}
	*context = NULL;
	if (files && !--files->refs) { exec_closed(files); free(files); }
}
static int exec_mask(pid_t pid, struct exec_call *call, int blocked) {
	unsigned long mask = ~0UL;
	if (blocked && ptrace(PTRACE_GETSIGMASK, pid, 8, &call->mask) < 0) return -1;
	return ptrace(PTRACE_SETSIGMASK, pid, 8, blocked ? &mask : &call->mask) == 0 ? 0 : -1;
}
static int exec_rewind(pid_t pid, const struct user_regs_struct *original, long number, unsigned long a, unsigned long b) {
	struct user_regs_struct registers = *original;
	registers.rip -= 2; registers.orig_rax = (unsigned long)-1; registers.rax = number;
	registers.rdi = a; registers.rsi = b;
	return ptrace(PTRACE_SETREGS, pid, 0, &registers) == 0 ? 0 : -1;
}
int pi_prepare_exec_call(pid_t pid, void **context, void **file_context, const char *source) {
	struct exec_call *call = calloc(1, sizeof(*call));
	struct user_regs_struct registers; char path[PATH_MAX + 64], name[4096]; int memory = -1, result = -1;
	if (!call || !(call->files = exec_files(file_context)) || ptrace(PTRACE_GETREGS, pid, 0, &call->original) < 0) goto done;
	const struct user_regs_struct *original = &call->original;
	int at = original->orig_rax == SYS_execveat;
	if ((!at && original->orig_rax != SYS_execve) || original->cs != 0x33 || original->rip < 2) goto done;
	memory = process_open(pid, "mem", O_RDONLY | O_CLOEXEC);
	unsigned long address = at ? original->rsi : original->rdi;
	ssize_t length = memory >= 0 ? pread(memory, name, sizeof(name), address) : -1;
	if (length <= 0 || !memchr(name, 0, (size_t)length)) { result = 0; goto done; }
	call->empty = address + strlen(name); call->tid = pid;
	if (exec_record_path(path, sizeof(path), source, pid) < 0) goto done;
	call->lease = open(path, O_RDWR | O_CREAT | O_TRUNC | O_CLOEXEC, 0600) + 1;
	if (!call->lease || exec_mask(pid, call, 1) < 0) goto done;
	registers = *original; registers.r9 = 0x5049455800000000UL | (at ? (unsigned int)original->r8 : 0);
	if (ptrace(PTRACE_SETREGS, pid, 0, &registers) < 0) goto done;
	*context = call; call = NULL; result = 0;
done:
	close(memory); if (call && call->lease) close(call->lease - 1); free(call); return result;
}
/* 1 consumes an internal stop, 0 exposes the completed original syscall. */
int pi_step_exec_call(pid_t pid, void **context, void **file_context, int exiting) {
	struct exec_call *call = *context;
	if (!call) {
		struct exec_files *files = *file_context;
		if (!files || !files->pending) return 0;
		if (files->pin < 0) files->pin = exec_pin(pid, files->fd);
		long same = files->pin < 0 ? -1 : syscall(SYS_kcmp, pid, getpid(), KCMP_FILE, files->fd, files->pin);
		if (same) {
			if (same < 0 && errno != EBADF) return -1;
			exec_closed(files); return 0;
		}
		call = calloc(1, sizeof(*call)); if (!call) return -1;
		call->files = files; call->phase = exiting ? 3 : 12; files->pending = 0; *context = call;
		if (ptrace(PTRACE_GETREGS, pid, 0, &call->original) < 0 || exec_mask(pid, call, 1) < 0) return -1;
		if (exiting) return exec_rewind(pid, &call->original, SYS_close, files->fd, 0) == 0 ? 1 : -1;
		struct user_regs_struct cleanup = call->original; cleanup.rax = cleanup.orig_rax = SYS_close; cleanup.rdi = files->fd;
		return ptrace(PTRACE_SETREGS, pid, 0, &cleanup) == 0 ? 1 : -1;
	}
	struct user_regs_struct registers;
	if (ptrace(PTRACE_GETREGS, pid, 0, &registers) < 0) return -1;
	long value = (long)registers.rax;
	if (!(call->phase & 1) && exec_restarting(value)) { call->phase--; return 1; }
	switch (call->phase) {
	case -1: call->phase = 0; return 1;
	case 0:
		if (value > 0 && value <= INT_MAX) {
			call->files->fd = (int)value - 1;
			if (call->files->refs > 1 && (call->files->pin = exec_pin(pid, call->files->fd)) < 0) return -1;
			registers = call->original; registers.rip -= 2; registers.rax = SYS_execveat; registers.orig_rax = (unsigned long)-1;
			registers.rdi = call->files->fd; registers.rsi = call->empty;
			registers.rdx = call->original.orig_rax == SYS_execveat ? call->original.rdx : call->original.rsi;
			registers.r10 = call->original.orig_rax == SYS_execveat ? call->original.r10 : call->original.rdx;
			registers.r8 = AT_EMPTY_PATH; registers.r9 = 0x5049455900000000UL;
			call->phase = 1; return ptrace(PTRACE_SETREGS, pid, 0, &registers) == 0 ? 1 : -1;
		}
		/* A native-path continuation already executed; errors retain the caller registers. */
		__attribute__((fallthrough));
	case 2:
		if (!value) return exec_mask(pid, call, 0);
		call->original.rax = registers.rax;
		if (call->files->fd >= 0) { call->phase = 3; return exec_rewind(pid, &call->original, SYS_close, call->files->fd, 0) == 0 ? 1 : -1; }
		break;
	case 1: call->phase = 2; return 1;
	case 3: call->phase = 4; return 1;
	case 4: case 12: {
		if (value) return -1;
		exec_closed(call->files);
		if (call->phase == 12) {
			registers = call->original; registers.rip -= 2; registers.rax = registers.orig_rax; registers.orig_rax = (unsigned long)-1;
			call->phase = 13; return ptrace(PTRACE_SETREGS, pid, 0, &registers) == 0 ? 1 : -1;
		}
		break;
	}
	case 7: call->phase = 8; return 1;
	case 8:
		if (value) return -1;
		registers = call->original; registers.rax = SYS_prctl; registers.orig_rax = (unsigned long)-1;
		registers.rdi = PR_SET_NAME; registers.rsi = call->command; registers.rdx = registers.r10 = registers.r8 = registers.r9 = 0;
		call->phase = 9; return ptrace(PTRACE_SETREGS, pid, 0, &registers) == 0 ? 1 : -1;
	case 9: call->phase = 10; return 1;
	case 10:
		if (value || ptrace(PTRACE_POKETEXT, pid, call->original.rip, call->code) < 0) return -1;
		break;
	case 11: call->phase = 12; return 1;
	case 13: break;
	}
	if (exec_mask(pid, call, 0) < 0 || ptrace(PTRACE_SETREGS, pid, 0, &call->original) < 0) return -1;
	int result = call->phase == 10 ? 2 | (call->eligible ? 4 : 0) : 0;
	if (call->lease) close(call->lease - 1);
	free(call); *context = NULL;
	return result;
}

/* Restore the requested exec pathname after a confined fd exec, before the image runs. */
int pi_restore_exec_context(pid_t pid, const char *source, const char *invoked, const char *command, void **context) {
	struct user_regs_struct registers;
	struct image_header *header = calloc(1, sizeof(*header));
	unsigned long *vector = NULL, argc, word, aux, slot = 0; int result = -1, consumed = 0, eligible = 0;
	char *arguments = NULL, *fields[32] = {0}; unsigned count = 0, prefix = 0; const char *name = NULL, *comm = NULL; FILE *record = NULL;
	char path[PATH_MAX + 64], original[8192]; size_t argument_bytes = 0;
	if (snprintf(path, sizeof(path), "%s.route.%d", source, pid) >= (int)sizeof(path) || !(record = fopen(path, "re"))) goto done;
	for (; count < 32; count++) { size_t capacity = 0; ssize_t bytes = getdelim(&fields[count], &capacity, 0, record);
		if (bytes < 0) break;
		if (bytes > 8192 || fields[count][bytes - 1]) goto done;
	}
	if (ferror(record) || count < 5 || count == 32 || (strcmp(fields[0], "B") && strcmp(fields[0], "N")) || *fields[1] != '/') goto done;
	name = fields[2]; comm = fields[3];
	prefix = count - 5; eligible = !strcmp(fields[0], "B");
	if (!eligible) {
		if (invoked && strcmp(name, source)) { name = invoked; comm = command; consumed = 1; }
	}
	if (!comm) { comm = strrchr(name, '/'); comm = comm ? comm + 1 : name; }
	size_t length = strlen(name) + 1, comm_length = strlen(comm) + 1;
	if (!header || length > 8192 || ptrace(PTRACE_GETREGS, pid, 0, &registers) < 0 || registers.cs != 0x33 || image_mm(pid, header, 0) < 0) goto done;
	for (size_t at = 0; at + 16 <= header->auxv_size; at += 16) if (*(unsigned long *)(header->auxv + at) == AT_EXECFN) { slot = at + 8; break; }
	if (!slot) goto done;
	int memory = process_open(pid, "mem", O_RDONLY | O_CLOEXEC); if (memory < 0) goto done;
	ssize_t read = pread(memory, original, sizeof(original), *(unsigned long *)(header->auxv + slot)); close(memory);
	if (read <= 0 || !memchr(original, 0, (size_t)read)) goto done;
	if (!prefix && !strcmp(original, name)) { result = 0; goto done; }
	if (image_memory(pid, registers.rsp, &argc, 8, 0) < 0 || argc > 1048576) goto done;
	aux = argc + 2;
	do { if (aux > 1048576 || image_memory(pid, registers.rsp + 8 * aux++, &word, 8, 0) < 0) goto done; } while (word);
	size_t bytes = aux * 8 + header->auxv_size;
	vector = malloc(bytes + 8 * prefix); if (!vector || image_memory(pid, registers.rsp, vector, bytes, 0) < 0 || memcmp(vector + aux, header->auxv, header->auxv_size)) goto done;
	if (prefix) {
		size_t before = header->mm.arg_end - header->mm.arg_start, added = 0;
		if (!argc || before > 4 * 1024 * 1024) goto done;
		for (unsigned index = 5; index < count; index++) added += strlen(fields[index]) + 1;
		arguments = malloc(added + before); if (!arguments || image_memory(pid, header->mm.arg_start, arguments + added, before, 0) < 0) goto done;
		size_t skipped = strnlen(arguments + added, before) + 1; if (skipped > before) goto done;
		memmove(arguments + added, arguments + added + skipped, before - skipped); argument_bytes = added + before - skipped;
		for (unsigned index = 5, offset = 0; index < count; index++) { size_t size = strlen(fields[index]) + 1; memcpy(arguments + offset, fields[index], size); offset += size; }
		memmove(vector + argc + prefix, vector + argc + 1, bytes - (argc + 1) * 8);
		vector[0] = argc + prefix - 1; aux += prefix - 1; bytes += 8 * (prefix - 1);
	}
	unsigned long stack = (registers.rsp - bytes - argument_bytes - length - comm_length - sizeof(header->mm) - 31) & ~15UL;
	unsigned long args = stack + bytes, text = args + argument_bytes, mm = (text + length + comm_length + 7) & ~7UL;
	if (prefix) {
		size_t offset = 0;
		for (unsigned long index = 1; index <= vector[0]; index++) { if (offset >= argument_bytes) goto done; vector[index] = args + offset; offset += strlen(arguments + offset) + 1; }
		if (offset != argument_bytes || image_memory(pid, args, arguments, argument_bytes, 1) < 0) goto done;
		header->mm.arg_start = args; header->mm.arg_end = text;
	}
	vector[aux + slot / 8] = text;
	header->mm.auxv = (void *)(stack + aux * 8); header->mm.auxv_size = header->auxv_size; header->mm.start_stack = stack;
	if (image_memory(pid, stack, vector, bytes, 1) < 0 || image_memory(pid, text, (void *)name, length, 1) < 0 ||
		image_memory(pid, text + length, (void *)comm, comm_length, 1) < 0 || image_memory(pid, mm, &header->mm, sizeof(header->mm), 1) < 0) goto done;
	struct exec_call *call = calloc(1, sizeof(*call));
	if (!call) goto done;
	*context = call; call->original = registers; call->original.rsp = stack; call->command = text + length;
	call->phase = 7; call->eligible = eligible; errno = 0;
	call->code = (unsigned long)ptrace(PTRACE_PEEKTEXT, pid, registers.rip, 0);
	if (errno || exec_mask(pid, call, 1) < 0 || ptrace(PTRACE_POKETEXT, pid, registers.rip, (call->code & ~0xffffUL) | 0x050fUL) < 0) goto done;
	registers.rax = SYS_prctl; registers.orig_rax = (unsigned long)-1;
	registers.rdi = PR_SET_MM; registers.rsi = PR_SET_MM_MAP; registers.rdx = mm; registers.r10 = sizeof(header->mm); registers.r8 = registers.r9 = 0;
	result = ptrace(PTRACE_SETREGS, pid, 0, &registers) == 0 ? 8 : -1;

done:
	if (record) fclose(record);
	for (unsigned index = 0; index < 32; index++) free(fields[index]);
	free(arguments); free(vector); free(header); return result < 0 ? result : result | consumed | (eligible ? 2 : 0);
}

static int image_read_backing(int fd, const struct image_map *map, void *buffer) {
	size_t length = (size_t)(map->end - map->start), bytes = map->offset >= map->size ? 0 :
		(size_t)(map->size - map->offset < length ? map->size - map->offset : length);
	memset(buffer, 0, length);
	return !bytes || (lseek(fd, (off_t)map->offset, SEEK_SET) == (off_t)map->offset && transfer(fd, buffer, bytes, 0) == 0) ? 0 : -1;
}
/* pagemap's file-page bit is available without PFNs. Equal bytes alone cannot
 * distinguish an untouched file page from a private page written back to its old value. */
static int image_cow(pid_t pid, uint64_t start, size_t length, unsigned char *cow) {
	int fd = process_open(pid, "pagemap", O_RDONLY | O_CLOEXEC); if (fd < 0) return -1;
	uint64_t pages[256]; size_t count = length / 4096, moved = 0;
	while (moved < count) {
		size_t chunk = count - moved < 256 ? count - moved : 256;
		if (lseek(fd, (off_t)((start / 4096 + moved) * sizeof(*pages)), SEEK_SET) < 0 ||
			transfer(fd, pages, chunk * sizeof(*pages), 0) < 0) break;
		for (size_t i = 0; i < chunk; i++) {
			if (!(pages[i] & (UINT64_C(1) << 63))) { close(fd); return -1; }
			cow[moved + i] = !(pages[i] & (UINT64_C(1) << 61));
		}
		moved += chunk;
	}
	close(fd); return moved == count ? 0 : -1;
}
static int descriptor_numbers(pid_t pid, int minimum, int *fds, unsigned capacity) {
	char path[64]; snprintf(path, sizeof(path), "/proc/%ld/fd", (long)pid);
	DIR *directory = opendir(path);
	if (!directory) return -1;
	unsigned count = 0; int error = 0, scanner = pid == getpid() ? dirfd(directory) : -1;
	for (;;) {
		errno = 0; struct dirent *entry = readdir(directory);
		if (!entry) { error = errno; break; }
		char *end; long fd = strtol(entry->d_name, &end, 10);
		if (!*entry->d_name || *end || fd < minimum || fd > INT_MAX || fd == scanner) continue;
		if (count == capacity) { error = E2BIG; break; }
		fds[count++] = (int)fd;
	}
	closedir(directory);
	if (error) errno = error;
	return error ? -1 : (int)count;
}

/* Read installed state, never a tracee's mutable result/control buffer. */
static int descriptor_info(pid_t pid, int fd, int64_t *position) {
	char line[256]; unsigned flags; int result = -1; int64_t offset = -1;
	FILE *file = descriptor_file(pid, fd); if (!file) return -1;
	while (fgets(line, sizeof(line), file)) {
		if (sscanf(line, "flags: %o", &flags) == 1 && flags <= INT_MAX) result = (int)flags;
		if (position) sscanf(line, "pos: %" SCNd64, &offset);
	}
	int failed = ferror(file); fclose(file);
	if (position) *position = offset;
	return failed || (position && offset < 0) ? -1 : result;
}

static int image_descriptors(pid_t pid, struct process_image *image) {
	int fds[MAX_HANDLES], count = descriptor_numbers(pid, 0, fds, MAX_HANDLES - image->fd_count);
	if (count < 0) return -1;
	for (int i = 0; i < count; i++) {
		struct stat state;
		if (descriptor_stat(pid, fds[i], &state) < 0) return -1;
		unsigned index = image->fd_count++;
		image->fds[index].fd = fds[i]; image->fds[index].flags = descriptor_info(pid, fds[i], &image->fds[index].offset);
		image->fds[index].device = state.st_dev; image->fds[index].inode = state.st_ino;
		if (image->fds[index].flags < 0) return -1;
	}
	return 0;
}

/* Called by the already authorized strace owner; no new ptracer exception or
 * sandbox permission is needed. A positive result leaves the process unchanged. */
int pi_process_image_frontier(long number) {
	if (number == SYS_read || number == SYS_readv || number == SYS_recvfrom || number == SYS_recvmsg ||
		number == SYS_write || number == SYS_writev || number == SYS_sendto || number == SYS_sendmsg) return 1;
	/* Any native x86-64 syscall entry precedes its effects. Only restartable I/O also permits an exit stop. */
	return number >= 0 && number < 0x40000000 ? 2 : 0;
}
struct process_credentials {
	unsigned parent, uid[4], gid[4], no_new_privileges, threads, seccomp;
	unsigned long long permitted, effective, inheritable, ambient, pending, shared_pending;
};

static int process_credentials(pid_t pid, struct process_credentials *value) {
	char line[256];
	FILE *input = process_file(pid, "status"); if (!input) return -1;
	unsigned fields = 0;
	while (fgets(line, sizeof(line), input)) {
		if (sscanf(line, "PPid: %u", &value->parent) == 1) fields |= 1;
		if (sscanf(line, "Uid: %u %u %u %u", value->uid, value->uid + 1, value->uid + 2, value->uid + 3) == 4) fields |= 2;
		if (sscanf(line, "Gid: %u %u %u %u", value->gid, value->gid + 1, value->gid + 2, value->gid + 3) == 4) fields |= 4;
		if (sscanf(line, "CapPrm: %llx", &value->permitted) == 1) fields |= 8;
		if (sscanf(line, "CapEff: %llx", &value->effective) == 1) fields |= 16;
		if (sscanf(line, "CapInh: %llx", &value->inheritable) == 1) fields |= 32;
		if (sscanf(line, "CapAmb: %llx", &value->ambient) == 1) fields |= 64;
		if (sscanf(line, "NoNewPrivs: %u", &value->no_new_privileges) == 1) fields |= 128;
		if (sscanf(line, "Threads: %u", &value->threads) == 1) fields |= 256;
		if (sscanf(line, "Seccomp: %u", &value->seccomp) == 1) fields |= 512;
		if (sscanf(line, "SigPnd: %llx", &value->pending) == 1) fields |= 1024;
		if (sscanf(line, "ShdPnd: %llx", &value->shared_pending) == 1) fields |= 2048;
	}
	int valid = fields == 4095 && !ferror(input); fclose(input); return valid ? 0 : -1;
}

static int image_status(pid_t pid, int consumer) {
	struct process_credentials value;
	return process_credentials(pid, &value) == 0 && value.threads == 1 && (!consumer || !value.seccomp) && !value.pending && !value.shared_pending ? 0 : -1;
}
static int image_write(int output, pid_t pid, struct process_image *image, int complete) {
	struct image_header *header = &image->header;
	if (dprintf(output, "PIIMAGE4 %d %u %zu %" PRIu64 "\n", pid, image->fd_count, complete ? sizeof(*header) : 0, complete ? header->length : 0) < 0) return -1;
	for (unsigned i = 0; i < image->fd_count; i++) if (dprintf(output, "%d %d %" PRId64 " %ju %ju %d\n", image->fds[i].fd,
		image->fds[i].flags, image->fds[i].offset, image->fds[i].device, image->fds[i].inode, image->fds[i].fd) < 0) return -1;
	return !complete || (transfer(output, header, sizeof(*header), 1) == 0 && transfer(output, image->data, (size_t)header->length, 1) == 0) ? 0 : -1;
}
int pi_prepare_process_image(pid_t pid, const char *path, unsigned long watched_tid, int (*admit)(void *), void *context) {
	struct process_image *image = image_new(); int result = 1, output = -1;
	if (!image) return 1;
	struct image_header *header = &image->header;
	if (ptrace(PTRACE_GETREGS, pid, 0, &header->registers) < 0 || header->registers.cs != 0x33 ||
		!pi_process_image_frontier(header->registers.orig_rax) || ((long)header->registers.rax != -ENOSYS &&
			(pi_process_image_frontier(header->registers.orig_rax) != 1 || ((long)header->registers.rax != -512 && (long)header->registers.rax != -514))) ||
		image_status(pid, 0) < 0 || image_limits(pid, header, 0) < 0) goto done;
	unsigned long original; unsigned char bootstrap_cow[2]; unsigned short instruction;
	uint64_t bootstrap = header->registers.rip & ~UINT64_C(4095);
	size_t bootstrap_length = ((header->registers.rip + sizeof(original) - 1) & ~UINT64_C(4095)) - bootstrap + 4096;
	if (image_memory(pid, header->registers.rip - 2, &instruction, sizeof(instruction), 0) < 0 || instruction != 0x050f ||
		image_memory(pid, header->registers.rip, &original, sizeof(original), 0) < 0 ||
		image_cow(pid, bootstrap, bootstrap_length, bootstrap_cow) < 0) goto done;
	header->registers.rax = header->registers.orig_rax; header->registers.orig_rax = (unsigned long)-1; header->registers.rip -= 2;
	if (image_descriptors(pid, image) < 0) goto done;
	output = open(path, O_WRONLY | O_CLOEXEC | O_CREAT | O_EXCL | O_NOFOLLOW, 0600);
	if (output < 0 || image_write(output, pid, image, 0) < 0) goto done;
	result = admit(context); if (result) goto done;
	/* Admission only reads the stopped process. From this point failure retires it. */
	result = -1;
	if (image_scratch(pid) < 0 || image_mm(pid, header, IMAGE_SCRATCH) < 0) goto done;
	struct iovec xstate = {.iov_base = header->xstate, .iov_len = sizeof(header->xstate)};
	if (ptrace(PTRACE_GETREGSET, pid, NT_X86_XSTATE, &xstate) < 0 ||
		ptrace(PTRACE_GETSIGMASK, pid, sizeof(header->sigmask), &header->sigmask) < 0) goto done;
	header->xstate_size = xstate.iov_len;
	for (unsigned signal = 1; signal <= 64; signal++) if (signal != SIGKILL && signal != SIGSTOP &&
		image_syscall(pid, SYS_rt_sigaction, signal, 0, IMAGE_SCRATCH + 4096 + signal * sizeof(*header->actions), 8) != 0) goto done;
	if (image_memory(pid, IMAGE_SCRATCH + 4096, header->actions, sizeof(header->actions), 0) < 0 ||
		image_syscall(pid, SYS_sigaltstack, 0, IMAGE_SCRATCH + 4096) != 0 ||
		image_memory(pid, IMAGE_SCRATCH + 4096, &header->alternate, sizeof(header->alternate), 0) < 0 || (header->alternate.ss_flags & SS_ONSTACK) ||
		image_syscall(pid, SYS_prctl, PR_GET_TID_ADDRESS, IMAGE_SCRATCH + 4096) != 0 ||
		image_memory(pid, IMAGE_SCRATCH + 4096, &header->clear_tid, sizeof(header->clear_tid), 0) < 0 ||
		syscall(SYS_get_robust_list, pid, &header->robust, &header->robust_length) < 0 ||
		ptrace((enum __ptrace_request)0x420f, pid, sizeof(header->rseq), &header->rseq) != sizeof(header->rseq)) goto done;
	if (header->clear_tid != watched_tid) goto done;
	if (header->rseq.pointer) {
		uint64_t active;
		if (image_memory(pid, header->rseq.pointer + 8, &active, sizeof(active), 0) < 0 || active || header->rseq.flags || header->rseq.size < 20) goto done;
	}
	if (image_maps(pid, header, 1) < 0) goto done;
	for (unsigned i = 0; i < header->count; i++) if (!image_special(&header->maps[i])) {
		struct image_map *map = &header->maps[i]; size_t length = (size_t)(map->end - map->start);
		size_t bytes = length * (1 + map->file) + map->file * (length / 4096);
		if (length > IMAGE_BYTES || bytes > IMAGE_BYTES || header->length > IMAGE_BYTES - bytes) goto done;
		header->length += bytes;
		if (map->file) {
			struct stat state; image->files[i] = open(map->path, O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
			if (image->files[i] < 0 || fstat(image->files[i], &state) < 0 || !S_ISREG(state.st_mode) || state.st_size < 0 ||
				(uint64_t)state.st_dev != map->device || (uint64_t)state.st_ino != map->inode) goto done;
			map->size = (uint64_t)state.st_size;
		}
	}
	image->data = image->allocation = malloc((size_t)header->length); if (!image->data) goto done;
	size_t offset = 0;
	for (unsigned i = 0; i < header->count; i++) if (!image_special(&header->maps[i])) {
		struct image_map *map = &header->maps[i]; size_t length = (size_t)(map->end - map->start);
		if (image_memory(pid, map->start, image->data + offset, length, 0) < 0) goto done;
		offset += length;
		if (map->file) {
			if (image_read_backing(image->files[i], map, image->data + offset) < 0) goto done;
			offset += length;
			if (image_cow(pid, map->start, length, image->data + offset) < 0) goto done;
			for (size_t page = 0; page < length / 4096; page++) {
				uint64_t address = map->start + page * 4096;
				if (address >= bootstrap && address < bootstrap + bootstrap_length) image->data[offset + page] = bootstrap_cow[(address - bootstrap) / 4096];
				if (!image->data[offset + page] && memcmp(image->data + offset - 2 * length + page * 4096,
					image->data + offset - length + page * 4096, 4096)) goto done;
			}
			offset += length / 4096;
		}
	}
	header->magic = UINT64_C(0x50494d4147453034);
	if (ftruncate(output, 0) < 0 || lseek(output, 0, SEEK_SET) != 0) goto done;
	result = image_write(output, pid, image, 1);
done:
	if (output >= 0 && close(output) < 0 && !result) result = -1;
	if (result > 0 && output >= 0) unlink(path);
	image_free(image); return result;
}

struct image_reader { const unsigned char *cursor; size_t remaining; };
static int image_line(struct image_reader *input, char *line, size_t capacity) {
	const unsigned char *end = memchr(input->cursor, '\n', input->remaining < capacity ? input->remaining : capacity);
	if (!end) return -1;
	size_t bytes = (size_t)(end - input->cursor);
	memcpy(line, input->cursor, bytes); line[bytes] = 0;
	input->cursor += bytes + 1; input->remaining -= bytes + 1; return 0;
}
static int image_load(struct decision_job *job, size_t length, const char *physical_root, const char *source_root) {
	struct process_image *image = job->image;
	struct image_reader input = {.cursor = image->allocation, .remaining = length};
	struct image_header *header = &image->header; char line[MAX_LINE]; size_t header_size; uint64_t bytes;
	if (image_line(&input, line, sizeof(line)) < 0 || sscanf(line, "PIIMAGE4 %d %u %zu %" SCNu64, &image->producer,
		&image->fd_count, &header_size, &bytes) != 4 || image->producer <= 0 || image->fd_count > MAX_HANDLES ||
		header_size != sizeof(*header) || bytes > IMAGE_BYTES) goto fail;
	for (unsigned i = 0; i < image->fd_count; i++) if (image_line(&input, line, sizeof(line)) < 0 ||
		sscanf(line, "%d %d %" SCNd64 " %ju %ju %d", &image->fds[i].fd, &image->fds[i].flags,
			&image->fds[i].offset, &image->fds[i].device, &image->fds[i].inode, &image->fds[i].source) != 6 ||
		image->fds[i].fd < 0 || image->fds[i].fd == INT_MAX || image->fds[i].source < 0 || image->fds[i].flags < 0 || image->fds[i].offset < 0) goto fail;
	if (input.remaining != sizeof(*header) + bytes) goto fail;
	memcpy(header, input.cursor, sizeof(*header));
	if (header->magic != UINT64_C(0x50494d4147453034) || header->length != bytes ||
		!header->count || header->count > IMAGE_MAPS || !header->xstate_size || header->xstate_size > sizeof(header->xstate) ||
		!header->auxv_size || header->auxv_size > sizeof(header->auxv) || header->mm.auxv || header->mm.exe_fd != (unsigned)-1 ||
		header->registers.cs != 0x33 || header->registers.orig_rax != (unsigned long)-1 || !pi_process_image_frontier(header->registers.rax) ||
		(header->alternate.ss_flags & ~(unsigned)SS_DISABLE) || header->rseq.flags || header->rseq.size > 4096) goto fail;
	image->data = (unsigned char *)(input.cursor + sizeof(*header));
	size_t offset = 0, root_length = strlen(physical_root);
	for (unsigned i = 0; i < header->count; i++) {
		struct image_map *map = &header->maps[i];
		if (!memchr(map->path, 0, sizeof(map->path)) || map->start >= map->end || ((map->start | map->end | map->offset) & 4095) ||
			(map->start < IMAGE_SCRATCH + 8192 && map->end > IMAGE_SCRATCH) ||
			(i && header->maps[i - 1].end > map->start) || map->prot > 7 || map->no_huge_pages > 1 || map->file != (unsigned)(*map->path == '/')) goto fail;
		if (image_special(map)) continue;
		size_t length = (size_t)(map->end - map->start);
		if (length > IMAGE_BYTES || length * (1 + map->file) + map->file * (length / 4096) > bytes - offset) goto fail;
		offset += length;
		if (!map->file) { if (*map->path && strcmp(map->path, "[heap]") && strcmp(map->path, "[stack]")) goto fail; continue; }
		if (strstr(map->path, " (deleted)")) goto fail;
		if (root_length && !strncmp(map->path, physical_root, root_length) && (!map->path[root_length] || map->path[root_length] == '/')) {
			char mapped[PATH_MAX]; int size = snprintf(mapped, sizeof(mapped), "%s%s", source_root, map->path + root_length);
			if (size < 0 || size >= (int)sizeof(mapped)) goto fail;
			strcpy(map->path, mapped);
		}
		/* An inherited nameless backing belongs to the captured kernel object, not
		 * the temporary pathname used to reconstruct its private producer. */
		for (unsigned fd = 0; fd < image->fd_count && image->files[i] < 0; fd++) if (image->fds[fd].device == map->device && image->fds[fd].inode == map->inode)
			for (unsigned pin = 0; pin < job->capture_count; pin++) if (job->captures[pin].descriptor == image->fds[fd].source) {
				image->files[i] = reopen_descriptor(job->captures[pin].duplicate, O_RDONLY | O_CLOEXEC); break;
			}
		struct stat state;
		if (image->files[i] < 0) image->files[i] = open(map->path, O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
		if (image->files[i] < 0 || fstat(image->files[i], &state) < 0 || !S_ISREG(state.st_mode) || state.st_size < 0 || (uint64_t)state.st_size != map->size) goto fail;
		void *backing = malloc(length); if (!backing) goto fail;
		int valid = image_read_backing(image->files[i], map, backing) == 0 && !memcmp(backing, image->data + offset, length);
		free(backing); if (!valid) goto fail; offset += length;
		for (size_t page = 0; page < length / 4096; page++) if (image->data[offset + page] > 1 ||
			(!image->data[offset + page] && memcmp(image->data + offset - 2 * length + page * 4096,
				image->data + offset - length + page * 4096, 4096))) goto fail;
		offset += length / 4096;
	}
	if (offset != bytes) goto fail;
	return 0;
fail:
	return -1;
}

static int duplicate_tracee_fd(struct decision_job *job, unsigned fd) {
	#if defined(SYS_pidfd_open) && defined(SYS_pidfd_getfd)
	if (job->pidfd < 0) job->pidfd = (int)syscall(SYS_pidfd_open, job->pid, 0);
    if (job->pidfd < 0) {
        char line[256]; int group = job->pid; FILE *status = process_file(job->pid, "status");
        if (status) { while (fgets(line, sizeof(line), status)) if (sscanf(line, "Tgid: %d", &group) == 1) break; fclose(status); }
        if (group != job->pid && syscall(SYS_kcmp, job->pid, group, KCMP_FILES, 0, 0) == 0) job->pidfd = (int)syscall(SYS_pidfd_open, group, 0);
    }

	/* Both syscalls return CLOEXEC handles; one process pin serves the entire held decision. */
	if (job->pidfd >= 0) return (int)syscall(SYS_pidfd_getfd, job->pidfd, fd, 0);
	#endif
	return -1;
}

static const struct file_position *resource_position(const struct decision_job *job, int source) {
	for (unsigned p = 0; p < job->position_count; p++) if (job->positions[p].descriptor == source) return &job->positions[p];
	return NULL;
}

/* Transfer the verified OFDs themselves, including queued-only sources. Both
 * qualification and restoration use this channel; it never reopens a pathname. */
static int image_sources(pid_t pid, const int *pins, unsigned count, int *sources) {
	if (!count) return 0;
	if (count > MAX_POSITIONS) return -1;
	struct image_message {
		struct msghdr message; struct iovec vector;
		union { struct cmsghdr alignment; unsigned char bytes[CMSG_SPACE(MAX_POSITIONS * sizeof(int))]; } control;
		char byte;
	} packet = {0};
	_Static_assert(sizeof(packet) <= 4096, "image transfer fits in scratch page");
	const unsigned long address = IMAGE_SCRATCH + 4096;
	struct decision_job target = {.pid = pid, .pidfd = -1};
	int channel[2] = {-1, -1}, sender = -1, result = -1;
	if (image_syscall(pid, SYS_socketpair, AF_UNIX, SOCK_DGRAM | SOCK_CLOEXEC, 0, address) != 0 ||
		image_memory(pid, address, channel, sizeof(channel), 0) < 0 || (sender = duplicate_tracee_fd(&target, (unsigned)channel[1])) < 0) goto done;
	packet.vector = (struct iovec){.iov_base = &packet.byte, .iov_len = 1};
	packet.message = (struct msghdr){.msg_iov = &packet.vector, .msg_iovlen = 1, .msg_control = packet.control.bytes,
		.msg_controllen = CMSG_SPACE(count * sizeof(int))};
	struct cmsghdr *header = CMSG_FIRSTHDR(&packet.message);
	header->cmsg_level = SOL_SOCKET; header->cmsg_type = SCM_RIGHTS; header->cmsg_len = CMSG_LEN(count * sizeof(int));
	for (unsigned p = 0; p < count; p++) ((int *)CMSG_DATA(header))[p] = pins[p];
	if (sendmsg(sender, &packet.message, MSG_DONTWAIT | MSG_NOSIGNAL) != 1) goto done;
	packet.message.msg_iov = (void *)(address + offsetof(struct image_message, vector));
	packet.vector.iov_base = (void *)(address + offsetof(struct image_message, byte));
	packet.message.msg_control = (void *)(address + offsetof(struct image_message, control));
	memset(&packet.control, 0, sizeof(packet.control));
	if (image_memory(pid, address, &packet, sizeof(packet), 1) < 0 ||
		image_syscall(pid, SYS_recvmsg, channel[0], address, MSG_DONTWAIT | MSG_CMSG_CLOEXEC) != 1 ||
		image_memory(pid, address, &packet, sizeof(packet), 0) < 0) goto done;
	header = (struct cmsghdr *)packet.control.bytes;
	if (packet.byte || (packet.message.msg_flags & (MSG_CTRUNC | MSG_TRUNC | MSG_OOB)) ||
		packet.message.msg_controllen != CMSG_SPACE(count * sizeof(int)) || header->cmsg_level != SOL_SOCKET ||
		header->cmsg_type != SCM_RIGHTS || header->cmsg_len != CMSG_LEN(count * sizeof(int))) goto done;
	for (unsigned p = 0; p < count; p++) {
		sources[p] = ((int *)CMSG_DATA(header))[p];
		if (sources[p] < 0 || syscall(SYS_kcmp, pid, getpid(), KCMP_FILE, sources[p], pins[p]) != 0) goto done;
	}
	result = 0;
done:
	close(sender); close(target.pidfd);
	for (unsigned end = 0; end < 2; end++) if (channel[end] >= 0 && image_syscall(pid, SYS_close, channel[end]) != 0) result = -1;
	return result;
}

/* Pin every source before replacing any slot, so aliases and cycles retain identity. */
static int image_handles(pid_t pid, struct decision_job *job) {
	struct process_image *image = job->image;
	int current[MAX_HANDLES], count = descriptor_numbers(pid, 0, current, MAX_HANDLES), minimum = 3, sources[MAX_POSITIONS], borrowed[MAX_POSITIONS];
	long pins[MAX_HANDLES];
	for (unsigned p = 0; p < job->position_count; p++) borrowed[p] = job->positions[p].duplicate;
	if (count < 0 || image_sources(pid, borrowed, job->position_count, sources) < 0) return -1;
	for (int i = 0; i < count; i++) if (current[i] >= minimum) minimum = current[i] + 1;
	for (unsigned i = 0; i < image->fd_count; i++) if (image->fds[i].fd >= minimum) minimum = image->fds[i].fd + 1;
	for (unsigned i = 0; i < image->fd_count; i++) {
		int source = image->fds[i].source;
		const struct file_position *position = resource_position(job, source);
		if (position) source = sources[position - job->positions];
		else if (source >= 3) return -1;
		pins[i] = image_syscall(pid, SYS_fcntl, source, F_DUPFD_CLOEXEC, minimum);
		if (pins[i] < minimum || pins[i] >= INT_MAX) return -1;
		minimum = (int)pins[i] + 1;
	}
	for (unsigned p = 0; p < job->position_count; p++) if (image_syscall(pid, SYS_close, sources[p]) != 0) return -1;
	for (unsigned i = 0; i < image->fd_count; i++) if (image_syscall(pid, SYS_dup3, pins[i], image->fds[i].fd,
		image->fds[i].flags & O_CLOEXEC) != image->fds[i].fd) return -1;
	for (int i = 0; i < count; i++) {
		unsigned next = 0;
		while (next < image->fd_count && image->fds[next].fd != current[i]) next++;
		if (next == image->fd_count && image_syscall(pid, SYS_close, current[i]) != 0) return -1;
	}
	for (unsigned i = 0; i < image->fd_count; i++) if (image_syscall(pid, SYS_close, pins[i]) != 0) return -1;
	return 0;
}
static int image_restore(pid_t pid, struct decision_job *job) {
	struct process_image *image = job->image;
	struct image_header *header = &image->header, *initial = calloc(1, sizeof(*initial));
	int result = -1; size_t offset = 0;
	if (!initial || image_maps(pid, initial, 0) < 0 || image_scratch(pid) < 0) goto done;
	/* A forked qualification child may inherit an rseq registration. Exec clears it
	 * in the actual consumer; unregister before removing either old image. */
	if (ptrace((enum __ptrace_request)0x420f, pid, sizeof(initial->rseq), &initial->rseq) != sizeof(initial->rseq)) goto done;
	if (initial->rseq.pointer && image_syscall(pid, SYS_rseq, initial->rseq.pointer, initial->rseq.size, 1, initial->rseq.signature) != 0) goto done;
	for (unsigned i = 0; i < initial->count; i++) if (!image_special(&initial->maps[i])) {
		const struct image_map *map = &initial->maps[i];
		if (image_syscall(pid, SYS_munmap, map->start, map->end - map->start) != 0) goto done;
	}
	for (unsigned i = 0; i < initial->count; i++) if (image_special(&initial->maps[i]) && strcmp(initial->maps[i].path, "[vsyscall]")) {
		const struct image_map *map = &initial->maps[i]; unsigned target = 0;
		while (target < header->count && strcmp(header->maps[target].path, map->path)) target++;
		if (target == header->count) goto done;
		const struct image_map *next = &header->maps[target]; if (map->end - map->start != next->end - next->start) goto done;
		if (map->start != next->start && image_syscall(pid, SYS_mremap, map->start, map->end - map->start,
			next->end - next->start, MREMAP_MAYMOVE | MREMAP_FIXED, next->start) != (long)next->start) goto done;
	}
	for (unsigned i = 0; i < header->count; i++) {
		struct image_map *map = &header->maps[i]; if (image_special(map)) continue;
		int fd = -1, flags = MAP_PRIVATE | MAP_FIXED;
		if (map->file) {
			if (image_sources(pid, &image->files[i], 1, &fd) < 0) goto done;
		} else { flags |= MAP_ANONYMOUS; if (!strcmp(map->path, "[stack]")) flags |= MAP_GROWSDOWN; }
		size_t length = (size_t)(map->end - map->start);
		if (image_syscall(pid, SYS_mmap, map->start, length, PROT_READ | PROT_WRITE, flags, fd, map->offset) != (long)map->start ||
			(fd >= 0 && image_syscall(pid, SYS_close, fd) != 0)) goto done;
		if (!map->file) { if (image_memory(pid, map->start, image->data + offset, length, 1) < 0) goto done; }
		else for (size_t page = 0; page < length / 4096; page++) if (image->data[offset + 2 * length + page] &&
			image_memory(pid, map->start + page * 4096, image->data + offset + page * 4096, 4096, 1) < 0) goto done;
		if (image_syscall(pid, SYS_mprotect, map->start, length, map->prot) != 0 ||
			(map->no_huge_pages && image_syscall(pid, SYS_madvise, map->start, length, MADV_NOHUGEPAGE) != 0)) goto done;
		offset += length * (1 + map->file) + map->file * (length / 4096);
	}
	if (header->clear_tid) {
		pid_t original;
		if (image_memory(pid, header->clear_tid, &original, sizeof(original), 0) < 0 || original != image->producer ||
			image_memory(pid, header->clear_tid, &pid, sizeof(pid), 1) < 0) goto done;
	}
	if (image_syscall(pid, SYS_set_tid_address, header->clear_tid) != pid ||
		(header->robust && image_syscall(pid, SYS_set_robust_list, header->robust, header->robust_length) != 0) ||
		(header->rseq.pointer && image_syscall(pid, SYS_rseq, header->rseq.pointer, header->rseq.size, 0, header->rseq.signature) != 0)) goto done;
	if (image_handles(pid, job) < 0 || image_memory(pid, IMAGE_SCRATCH + 4096, header->actions, sizeof(header->actions), 1) < 0) goto done;
	for (unsigned signal = 1; signal <= 64; signal++) if (signal != SIGKILL && signal != SIGSTOP &&
		image_syscall(pid, SYS_rt_sigaction, signal, IMAGE_SCRATCH + 4096 + signal * sizeof(*header->actions), 0, 8) != 0) goto done;
	struct prctl_mm_map mm = header->mm; mm.auxv = (void *)(IMAGE_SCRATCH + 4096); mm.auxv_size = header->auxv_size;
	if (image_memory(pid, IMAGE_SCRATCH + 4096, &header->alternate, sizeof(header->alternate), 1) < 0 ||
		image_syscall(pid, SYS_sigaltstack, IMAGE_SCRATCH + 4096) != 0 ||
		image_memory(pid, IMAGE_SCRATCH + 4096, header->auxv, header->auxv_size, 1) < 0 ||
		image_memory(pid, IMAGE_SCRATCH + 256, &mm, sizeof(mm), 1) < 0 ||
		image_syscall(pid, SYS_prctl, PR_SET_MM, PR_SET_MM_MAP, IMAGE_SCRATCH + 256, sizeof(mm)) != 0 ||
		ptrace(PTRACE_SETREGS, pid, 0, &header->registers) < 0 ||
		image_syscall(pid, SYS_munmap, IMAGE_SCRATCH, 8192) != 0) goto done;
	struct iovec xstate = {.iov_base = header->xstate, .iov_len = header->xstate_size};
	if (image_limits(pid, header, 1) < 0 || ptrace(PTRACE_SETREGSET, pid, NT_X86_XSTATE, &xstate) < 0 ||
		ptrace(PTRACE_SETSIGMASK, pid, sizeof(header->sigmask), &header->sigmask) < 0 ||
		ptrace(PTRACE_SETREGS, pid, 0, &header->registers) < 0) goto done;
	result = 0;
done:
	free(initial); return result;
}

static int image_qualify(struct decision_job *job) {
	pid_t child = fork(); if (child < 0) return -1;
	if (!child) {
		if (prctl(PR_SET_DUMPABLE, 1, 0, 0, 0) < 0 || prctl(PR_SET_PDEATHSIG, SIGKILL) < 0 || ptrace(PTRACE_TRACEME, 0, 0, 0) < 0 || raise(SIGSTOP)) _exit(70);
		_exit(70);
	}
	int status, result = -1;
	if (image_wait(child, &status) == 0 && WSTOPSIG(status) == SIGSTOP &&
		ptrace(PTRACE_SETOPTIONS, child, 0, PTRACE_O_EXITKILL | PTRACE_O_TRACESYSGOOD) == 0) result = image_restore(child, job);
	kill(child, SIGKILL);
	while (waitpid(child, &status, __WALL | __WNOTHREAD) < 0 && errno == EINTR) {}
	return result;
}

static void free_events(struct output_event *events, unsigned count) {
	if (!events) return;
	for (unsigned index = 0; index < count; index++) free(events[index].data);
	free(events);
}

static int has_unmodeled_descriptors(int minimum) {
	/* A closed standard stream would be filled by the first file opened; bypass before any is. */
	for (int fd = 0; fd < 3; fd++) {
		if (fcntl(fd, F_GETFD) < 0) return errno == EBADF ? 1 : -1;
	}
	return descriptor_numbers(getpid(), minimum, NULL, 0) < 0 ? (errno == E2BIG ? 1 : -1) : 0;
}

/* The broker decodes a request as UTF-8, as it decoded Node's strings. */
static void json_bytes(FILE *out, const char *value, size_t length) {
	fputc('"', out);
	for (size_t index = 0; index < length; index++) {
		unsigned char byte = (unsigned char)value[index];
		if (byte == '"' || byte == '\\') fprintf(out, "\\%c", byte);
		else if (byte < 0x20) fprintf(out, "\\u%04x", byte);
		else fputc(byte, out);
	}
	fputc('"', out);
}

static void json_hex(FILE *out, const unsigned char *bytes, size_t length) {
	fputc('"', out);
	for (size_t index = 0; index < length; index++) {
		putc_unlocked("0123456789abcdef"[bytes[index] >> 4], out);
		putc_unlocked("0123456789abcdef"[bytes[index] & 15], out);
	}
	fputc('"', out);
}

/* The image is already installed, and stays stopped while its owner decides.
 * Pins outlive the exchange but never the target's resumed execution. */
struct broker_job {
	pid_t pid; pthread_t thread;
	int channel[2], connection, result, count, numbers[MAX_HANDLES], files[MAX_HANDLES];
	char *request, *response; size_t request_size, response_size;
	struct sockaddr_un address;
};
static int pipe_bytes(int fd, unsigned char **bytes, int *eof);
static int broker_pipes(FILE *out, struct broker_job *job) {
	int described = 0;
	for (int index = 0; index < job->count; index++) {
		if (job->numbers[index] < 3) continue;
		int fd = job->files[index], flags = fcntl(fd, F_GETFL), capacity = fcntl(fd, F_GETPIPE_SZ), eof = 0, alias = job->numbers[index], reader = -1;
		struct stat state, other_state; unsigned char *bytes = NULL;
		if (fstat(fd, &state) < 0 || !S_ISFIFO(state.st_mode) || flags < 0 || capacity <= 0) return -1;
		for (int other = 0; other < index; other++) if (syscall(SYS_kcmp, getpid(), getpid(), KCMP_FILE, fd, job->files[other]) == 0) { alias = job->numbers[other]; break; }
		for (int other = 0; other < job->count && reader < 0; other++)
			if ((fcntl(job->files[other], F_GETFL) & O_ACCMODE) != O_WRONLY && fstat(job->files[other], &other_state) == 0 && other_state.st_dev == state.st_dev && other_state.st_ino == state.st_ino) reader = job->files[other];
		int length = reader < 0 ? -1 : pipe_bytes(reader, &bytes, &eof);
		if (length < 0) return -1;
		fprintf(out, "%s{\"fd\":%d,\"alias\":%d,\"device\":\"%ju\",\"inode\":\"%ju\",\"flags\":%d,\"capacity\":%d,\"eof\":%s,\"queueHex\":",
			described++ ? "," : "", job->numbers[index], alias, (uintmax_t)state.st_dev, (uintmax_t)state.st_ino, flags, capacity, eof ? "true" : "false");
		json_hex(out, bytes, (size_t)length);
		fputc('}', out); free(bytes);
	}
	return described;
}
static void broker_free(struct broker_job *job) {
	for (int index = 0; index < job->count; index++) close(job->files[index]);
	close(job->channel[0]); close(job->channel[1]); close(job->connection);
	free(job->request); free(job->response); free(job);
}
static int broker_reply(struct broker_job *job) {
	const char *at = job->response, *end = at + job->response_size;
	while (at < end) {
		const char *newline = memchr(at, '\n', (size_t)(end - at));
		int fd, value, consumed = -1; unsigned long long length;
		if (!newline) return -1;
		char kind = *at;
		if ((kind == 'o' || kind == 'i') && sscanf(at + 1, "%d %llu%n", &fd, &length, &consumed) == 2 && at + 1 + consumed == newline && fd >= 1 &&
			length <= (unsigned long long)(end - newline - 1)) {
			int pin = -1; for (int index = 0; index < job->count; index++) if (job->numbers[index] == fd) pin = job->files[index];
			if (pin < 0) return -1;
			for (size_t offset = 0; kind == 'i' && offset < length; offset++) {
				unsigned char seen;
				if (transfer(pin, &seen, 1, 0) < 0 || seen != (unsigned char)newline[1 + offset]) return -1;
			}
			if (kind == 'o' && transfer(pin, (void *)(newline + 1), (size_t)length, 1) < 0) return -1;
			at = newline + 1 + length; continue;
		}
		if (newline + 1 != end) return -1;
		if (newline == at + 1 && *at == 'b') return -2;
		if (sscanf(at, "x %d%n", &value, &consumed) == 1 && at + consumed == newline && value >= 0 && value <= 255) return value;
		return sscanf(at, "s %d%n", &value, &consumed) == 1 && at + consumed == newline && value > 0 && value < NSIG ? 256 + value : -1;
	}
	return -1;
}
static void *broker_decide(void *context) {
	struct broker_job *job = context;
	sigset_t blocked; sigfillset(&blocked); pthread_sigmask(SIG_SETMASK, &blocked, NULL);
	job->result = -1;
	if (connect(job->connection, (struct sockaddr *)&job->address, sizeof(job->address)) < 0 ||
		transfer(job->connection, job->request, job->request_size, 1) < 0 || shutdown(job->connection, SHUT_WR) < 0) goto done;
	char bytes[65536]; ssize_t size;
	while ((size = read(job->connection, bytes, sizeof(bytes))) > 0 || (size < 0 && errno == EINTR)) if (size > 0) {
		if ((size_t)size > MAX_OUTPUT_BYTES - job->response_size) goto done;
		char *grown = realloc(job->response, job->response_size + (size_t)size);
		if (!grown) goto done;
		job->response = grown; memcpy(grown + job->response_size, bytes, (size_t)size); job->response_size += (size_t)size;
	}
	if (size == 0) job->result = broker_reply(job);
done:
	shutdown(job->channel[1], SHUT_WR); return NULL;
}
void *pi_broker_start(pid_t pid, const char *source, int *notification) {
	*notification = -1;
	struct broker_job *job = calloc(1, sizeof(*job));
	if (!job) return NULL;
	job->pid = pid; job->channel[0] = job->channel[1] = job->connection = -1;
	for (int index = 0; index < MAX_HANDLES; index++) job->files[index] = -1;
	job->count = descriptor_numbers(pid, 0, job->numbers, MAX_HANDLES);
	struct decision_job target = {.pid = pid, .pidfd = -1};
	int captured = job->count >= 0, outputs = 0;
	for (int index = 0; captured && index < job->count; index++) {
		captured = (job->files[index] = duplicate_tracee_fd(&target, (unsigned)job->numbers[index])) >= 0;
		if (job->numbers[index] == 1 || job->numbers[index] == 2) outputs |= 1 << job->numbers[index];
	}
	close(target.pidfd);
	if (!captured || outputs != 6) { broker_free(job); return NULL; }
	char configuration[PATH_MAX], token[256]; FILE *file = NULL, *out = NULL;
	if (!source || snprintf(configuration, sizeof(configuration), "%s.configuration", source) >= (int)sizeof(configuration) ||
		!(file = fopen(configuration, "re")) || !fgets(job->address.sun_path, sizeof(job->address.sun_path), file) || !fgets(token, sizeof(token), file)) goto fail;
	job->address.sun_family = AF_UNIX;
	job->address.sun_path[strcspn(job->address.sun_path, "\n")] = token[strcspn(token, "\n")] = 0;
	fclose(file); file = NULL;
	out = open_memstream(&job->request, &job->request_size); if (!out) goto fail;
	fputs("{\"token\":", out); json_bytes(out, token, strlen(token)); fprintf(out, ",\"pid\":%d,\"descriptors\":[", pid);
	int pipes = broker_pipes(out, job); fputs("]}", out); int written = ferror(out); if (fclose(out)) written = 1; out = NULL;
	if (pipes < 0) { broker_free(job); return NULL; }
	if (written || (job->connection = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0)) < 0 || socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, job->channel) < 0 ||
		pthread_create(&job->thread, NULL, broker_decide, job)) goto fail;
	*notification = job->channel[0]; return job;
fail:
	if (file) fclose(file);
	if (out) fclose(out);
	broker_free(job); *notification = -2; return NULL;
}
/* 1 resumes native code, 2 exits with the brokered result, 2+signal delivers that signal. */
int pi_broker_finish(void *context, int cancel) {
	struct broker_job *job = context; int result = -1;
	if (cancel) pthread_cancel(job->thread);
	pthread_join(job->thread, NULL);
	if (cancel || job->result == -2) result = 1;
	else if (job->result >= 0) {
		unsigned signal = job->result > 256 ? (unsigned)job->result - 256 : 0;
		if (replace_with_exit(job->pid, signal ? 128 + signal : (unsigned)job->result) < 0) goto done;
		if (signal) {
			struct user_regs_struct regs; unsigned long data[5] = {0, 0, 0, 0, 1UL << (signal - 1)};
			if (ptrace(PTRACE_GETREGS, job->pid, 0, &regs) < 0) goto done;
			unsigned long at = (regs.rsp - sizeof(data) - 256) & ~7UL;
			if (image_memory(job->pid, at, data, sizeof(data), 1) < 0 ||
				(signal != SIGKILL && signal != SIGSTOP && image_call(job->pid, 0, SYS_rt_sigaction, signal, at, 0, 8) != 0) ||
				image_call(job->pid, 0, SYS_rt_sigprocmask, SIG_UNBLOCK, at + 32, 0, 8) != 0) goto done;
		}
		result = 2 + (int)signal;
	}
done:
	broker_free(job); return result;
}

/* Preserve the libuv launcher's empty signal mask and default dispositions. */
static int reset_signals(void) {
	sigset_t empty; sigemptyset(&empty);
	if (sigprocmask(SIG_SETMASK, &empty, NULL) < 0) return -1;
	for (int number = 1; number < NSIG; number++) signal(number, SIG_DFL);
	return 0;
}

/* Hold the sandbox's launch context for its owner after proving a script's read position. */
static int probe_context(const char *root, const char *script) {
	char cwd[PATH_MAX], ready;
	int status;
	if (reset_signals() < 0 || !getcwd(cwd, sizeof(cwd)) || strcmp(cwd, root)) return 70;
	pid_t child = fork();
	if (child == 0) { execl(script, script, (char *)NULL); _exit(127); }
	if (child < 0 || waitpid(child, &status, 0) < 0 || !WIFEXITED(status) || WEXITSTATUS(status) != 42) return 71;
	pid_t pid = getpid();
	return transfer(3, &pid, sizeof(pid), 1) == 0 && transfer(3, &ready, 1, 0) == 0 && ready == 'R' ? 0 : 70;
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

/* fdinfo identifies locks owned by this OFD. Process-owned locks remain a separate
 * ownership class; the global check below refuses them when a lock is reused. */
static int parse_file_lock(const char *line, struct ofd_lock *lock, dev_t *device, ino_t *inode) {
	char kind[16], advisory[16], type[16], end[32]; unsigned major_id, minor_id; intmax_t pid; uintmax_t object;
	const char *fields = strchr(line, ':'); if (!fields) return -1;
	fields++; while (isspace((unsigned char)*fields)) fields++;
	int waiting = !strncmp(fields, "->", 2); if (waiting) fields += 2;
	if (sscanf(fields, "%15s %15s %15s %jd %x:%x:%ju %" SCNd64 " %31s", kind, advisory, type, &pid, &major_id, &minor_id, &object, &lock->start, end) != 9) return -1;
	*device = makedev(major_id, minor_id); *inode = (ino_t)object;
	lock->kind = !strcmp(advisory, "ADVISORY") ? !strcmp(kind, "FLOCK") ? 0 : !strcmp(kind, "OFDLCK") ? 1 : -1 : -1;
	if (waiting) lock->kind = -1;
	lock->type = !strcmp(type, "READ") ? F_RDLCK : !strcmp(type, "WRITE") ? F_WRLCK : -1;
	lock->length = 0;
	if (strcmp(end, "EOF")) {
		char *next; errno = 0; int64_t last = strtoll(end, &next, 10);
		if (errno || *next || last < lock->start || last == INT64_MAX) return -1;
		lock->length = last - lock->start + 1;
	}
	return lock->start < 0 || lock->type < 0 ? -1 : 0;
}

static int append_lock(struct file_locks *locks, struct ofd_lock lock) {
	if (locks->count == MAX_POSITIONS || lock.kind < 0 || lock.kind > 1 || lock.type < 0 || lock.type > 1 || lock.start < 0 || lock.length < 0 ||
		(lock.length && lock.start > INT64_MAX - lock.length) || (!lock.kind && (lock.start || lock.length))) return -1;
	struct ofd_lock *entries = realloc(locks->entries, (locks->count + 1) * sizeof(*entries));
	if (!entries) return -1;
	locks->entries = entries; entries[locks->count++] = lock; return 0;
}

static int compare_locks(const void *a, const void *b) {
	const struct ofd_lock *left = a, *right = b;
	if (left->kind != right->kind) return left->kind - right->kind;
	if (left->start != right->start) return left->start < right->start ? -1 : 1;
	if (left->length != right->length) return left->length < right->length ? -1 : 1;
	return left->type - right->type;
}

static int capture_file_locks(pid_t pid, int fd, struct file_locks *locks) {
	char line[512];
	FILE *file = descriptor_file(pid, fd); if (!file) return -1;
	int result = 0;
	while (fgets(line, sizeof(line), file)) if (!strncmp(line, "lock:", 5)) {
		struct ofd_lock lock; dev_t device; ino_t inode;
		if (parse_file_lock(line + 5, &lock, &device, &inode) < 0 || append_lock(locks, lock) < 0) { result = -1; break; }
	}
	if (ferror(file)) result = -1;
	fclose(file);
	if (locks->count) qsort(locks->entries, locks->count, sizeof(*locks->entries), compare_locks);
	return result;
}

static int set_file_lock(int fd, struct ofd_lock lock) {
	if (!lock.kind) return flock(fd, (lock.type == F_UNLCK ? LOCK_UN : lock.type == F_WRLCK ? LOCK_EX : LOCK_SH) | LOCK_NB);
	struct flock range = {.l_type = lock.type, .l_whence = SEEK_SET, .l_start = lock.start, .l_len = lock.length};
	return fcntl(fd, F_OFD_SETLK, &range);
}

/* Anonymous inode numbers alone cannot distinguish event counters. fdinfo supplies
 * a kernel-assigned object ID and a non-consuming state observation. */
static unsigned event_state(pid_t pid, int fd, uint64_t *value, unsigned *semaphore) {
	char line[256]; unsigned id = 0, found = 0; uint64_t count = 0; unsigned mode = 0;
	FILE *file = descriptor_file(pid, fd);
	while (file && fgets(line, sizeof(line), file)) {
		if (sscanf(line, "eventfd-count: %" SCNx64, &count) == 1) found |= 1;
		if (sscanf(line, "eventfd-id: %u", &id) == 1) found |= 2;
		if (sscanf(line, "eventfd-semaphore: %u", &mode) == 1) found |= 4;
	}
	if (file) fclose(file);
	if (found != 7 || id == UINT_MAX || mode > 1 || count == UINT64_MAX) return 0;
	if (value) *value = count;
	if (semaphore) *semaphore = mode;
	return id + 1;
}

static int same_object(const struct file_position *left, const struct file_position *right) {
	return left->device == right->device && left->inode == right->inode && left->event == right->event;
}

/* Object history grants no OFD identity. Foreign ancestry is sticky, including
 * inode recycling: it can only refuse a later proof, never manufacture one. */
static unsigned remember_object(struct descriptor_domain *domain, pid_t pid, int fd, int foreign) {
	struct stat state;
	if (descriptor_stat(pid, fd, &state) < 0) { domain->incomplete = 1; return 0; }
	unsigned event = !(state.st_mode & S_IFMT) ? event_state(pid, fd, NULL, NULL) : 0;
	unsigned index = 0;
	while (index < domain->object_count && !(domain->objects[index].device == state.st_dev &&
		domain->objects[index].inode == state.st_ino && domain->objects[index].type == (state.st_mode & S_IFMT) && domain->objects[index].event == event)) index++;
	if (index == MAX_OBJECTS) { domain->incomplete = 1; return 0; }
	if (index == domain->object_count) domain->objects[domain->object_count++] =
		(struct resource_object){.device = state.st_dev, .inode = state.st_ino, .type = state.st_mode & S_IFMT, .event = event};
	if (foreign) domain->objects[index].foreign = 1;
	return index + 1;
}

static struct resource_object *origin_object(struct descriptor_domain *domain, struct descriptor_origin origin) {
	return origin.object ? &domain->objects[origin.object - 1] : NULL;
}

static struct resource_object *pin_object(struct descriptor_domain *domain, int pin) {
	struct stat state;
	if (!domain || fstat(pin, &state) < 0) return NULL;
	unsigned event = !(state.st_mode & S_IFMT) ? event_state(getpid(), pin, NULL, NULL) : 0;
	for (unsigned index = 0; index < domain->object_count; index++) {
		struct resource_object *object = &domain->objects[index];
		if (object->device == state.st_dev && object->inode == state.st_ino && object->type == (state.st_mode & S_IFMT) && object->event == event) return object;
	}
	return NULL;
}

static struct descriptor_origin created_origin(struct traced_process *process, struct descriptor_domain *domain,
	int fd, int cloexec, int pipe, unsigned long channel) {
	int flags = descriptor_info(process->pid, fd, NULL);
	if (flags < 0) domain->incomplete = 1;
	struct descriptor_origin origin = {.fd = fd, .cloexec = cloexec, .id = ++domain->next,
		.object = remember_object(domain, process->pid, fd, 0), .access = flags < 0 ? 3 : (flags & O_ACCMODE) + 1};
	struct resource_object *object = origin_object(domain, origin);
	if (object) { object->internal = 1; object->pipe |= pipe; if (channel) object->channel = channel; }
	return origin;
}

/* Recover aliases with KCMP_FILE first. An orphaned internal transfer can create
 * a fresh OFD node only when every external import is accounted for and the
 * object has exclusively internal ancestry. No kernel reference is retained. */
static struct descriptor_origin received_origin(struct traced_process *process, struct descriptor_domain *domain, int fd) {
	int flags = descriptor_info(process->pid, fd, NULL);
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
		.id = owned ? ++domain->next : 0, .object = id, .access = (flags & O_ACCMODE) + 1};
}

static struct descriptor_table *copy_descriptor_table(struct traced_process *source) {
	struct descriptor_table *table = calloc(1, sizeof(*table));
	if (!table) return NULL;
	if (source && source->table && !source->table->active && source->call_epoch == source->table->epoch) {
		table->count = source->table->count;
		memcpy(table->entries, source->table->entries, table->count * sizeof(*table->entries));
		table->complete = source->table->complete;
	}
	if (!source) table->complete = 1;
	table->references = 1; table->generation = 1;
	return table;
}

static void drop_descriptor_table(struct traced_process *process, struct descriptor_domain *domain) {
	if (process->uncertain) { domain->uncertain--; process->uncertain = 0; domain->escaped = 1; }
	struct descriptor_table *table = process->table;
	if (!table) return;
	if (process->mutation) { table->active--; table->count = 0; table->generation++; table->complete = 0; process->mutation = 0; }
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
		int fds[MAX_HANDLES], count = descriptor_numbers(pid, 0, fds, MAX_HANDLES);
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
		int escapes = number == SYS_io_setup || number == SYS_io_submit || number == SYS_io_uring_setup ||
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
		process->internal_message = message && !table->active && channel && channel->channel != 0 && channel->socket_type != SOCK_DGRAM;
		/* A message only affects its immutable channel, not unrelated objects. Queue
		 * references are inspected from the kernel under the next tree lease. */
		if (process->internal_message) for (unsigned index = 0; index < domain->object_count; index++)
			if (domain->objects[index].channel == channel->channel) domain->objects[index].messages = 1;
		if (number == SYS_clone3 || number == SYS_ioctl || message || number == SYS_connect ||
			(number == SYS_sendto && process->arguments[4])) { process->uncertain = 1; domain->uncertain++; }
		if (escapes) domain->escaped = 1;
		process->call_epoch = table->epoch;
		process->mutation = number == SYS_close || (number == SYS_close_range && !detached) || number == SYS_dup ||
			number == SYS_dup2 || number == SYS_dup3 || number == SYS_open || number == SYS_openat ||
			number == SYS_openat2 || number == SYS_creat || number == SYS_memfd_create || number == SYS_eventfd || number == SYS_eventfd2 || pair || receiving || number == SYS_pidfd_getfd ||
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
		if (pair || receiving) process->descriptor_count = descriptor_numbers(pid, 0, process->descriptors, MAX_HANDLES);
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
		int installed[MAX_HANDLES], count = descriptor_numbers(pid, 0, installed, MAX_HANDLES), added = 0;
		if (count < 0 || process->descriptor_count < 0) return 0;
		for (int index = 0; index < count; index++) {
			int previous = 0;
			while (previous < process->descriptor_count && installed[index] != process->descriptors[previous]) previous++;
			if (previous == process->descriptor_count) installed[added++] = installed[index];
		}
		if (pair && added != 2) return 0;
		/* Datagram peer changes and addressed sends revoke this closed-pair proof. */
		unsigned long type = second & ~(unsigned long)(SOCK_CLOEXEC | SOCK_NONBLOCK);
		unsigned long channel = number == SYS_socketpair && first == AF_UNIX && !third &&
			(type == SOCK_STREAM || type == SOCK_SEQPACKET || type == SOCK_DGRAM) ? ++domain->next : 0;
		/* recvmsg may install rights before failing to write its userspace result. */
		if (receiving && !process->internal_message && added) { domain->escaped = 1; return 0; }
		for (int index = 0; index < added; index++) {
			struct descriptor_origin origin = receiving ? received_origin(process, domain, installed[index])
				: created_origin(process, domain, installed[index], number != SYS_pipe && (second & O_CLOEXEC), number != SYS_socketpair, channel);
			struct resource_object *object = origin_object(domain, origin);
			if (object && channel) object->socket_type = type;
			set_descriptor_origin(process, domain, origin);
		}
	} else if (number == SYS_pidfd_getfd) {
		set_descriptor_origin(process, domain, received_origin(process, domain, fd));
	} else if (number == SYS_fcntl && second == F_SETFD) {
		source.cloexec = (third & FD_CLOEXEC) != 0; set_descriptor_origin(process, domain, source);
	} else if (number == SYS_dup || number == SYS_dup2 || number == SYS_dup3 ||
		(number == SYS_fcntl && (second == F_DUPFD || second == F_DUPFD_CLOEXEC))) {
		source.fd = fd; source.cloexec = (number == SYS_dup2 && first == second && source.cloexec) ||
			(number == SYS_dup3 && (third & O_CLOEXEC)) || (number == SYS_fcntl && second == F_DUPFD_CLOEXEC);
		set_descriptor_origin(process, domain, source);
	} else if (number == SYS_open || number == SYS_openat || number == SYS_creat || number == SYS_memfd_create || number == SYS_eventfd || number == SYS_eventfd2) {
		unsigned long flags = number == SYS_open || number == SYS_eventfd2 ? second : number == SYS_openat ? third : 0;
		struct descriptor_origin origin = created_origin(process, domain, fd, (flags & O_CLOEXEC) || (number == SYS_memfd_create && (second & 1)), 0, 0);
		struct resource_object *object = origin_object(domain, origin);
		if (object && number == SYS_memfd_create) object->anonymous = 1;
		set_descriptor_origin(process, domain, origin);
	} else if (number == SYS_openat2) {
		int flags = descriptor_info(pid, fd, NULL);
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

/* Read a separate directory OFD so snapshots never consume the shared cursor.
 * Kernel cookies and inode numbers remain raw 64-bit values. Padding is not data. */
static int directory_bytes(int fd, unsigned char **bytes) {
	struct stat before, after; int result = -1;
	*bytes = NULL;
	int reader = reopen_descriptor(fd, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
	if (reader < 0) return -1;
	if (fstat(reader, &before) < 0 || !(*bytes = calloc(MAX_INPUT_BYTES + 1, 1))) goto done;
	size_t used = 0; int64_t previous_cookie = 0;
	for (;;) {
		ssize_t length = syscall(SYS_getdents64, reader, *bytes + used, MAX_INPUT_BYTES - used);
		if (length < 0) goto done;
		if (!length) break;
		for (size_t offset = used; offset < used + (size_t)length;) {
			unsigned short size; int64_t cookie;
			if (used + (size_t)length - offset < 20) goto done;
			memcpy(&size, *bytes + offset + 16, 2); memcpy(&cookie, *bytes + offset + 8, 8);
			if (size < 24 || size % 8 || size > used + (size_t)length - offset || cookie <= previous_cookie) goto done;
			previous_cookie = cookie;
			size_t name = strnlen((char *)*bytes + offset + 19, size - 19);
			if (!name || name == (size_t)size - 19) goto done;
			memset(*bytes + offset + 20 + name, 0, size - 20 - name); offset += size;
		}
		used += (size_t)length;
		if (used == MAX_INPUT_BYTES) goto done;
	}
	if (fstat(reader, &after) == 0 && before.st_dev == after.st_dev && before.st_ino == after.st_ino &&
		before.st_mtim.tv_sec == after.st_mtim.tv_sec && before.st_mtim.tv_nsec == after.st_mtim.tv_nsec &&
		before.st_ctim.tv_sec == after.st_ctim.tv_sec && before.st_ctim.tv_nsec == after.st_ctim.tv_nsec) result = (int)used;
done:
	close(reader);
	if (result < 0) { free(*bytes); *bytes = NULL; }
	return result;
}

/* A queue snapshot keeps bytes and producer EOF separate. Repeating tee would copy
 * the same prefix again, so require one complete non-consuming transfer. */
static int pipe_bytes(int fd, unsigned char **bytes, int *eof) {
	struct pollfd ready = {.fd = fd, .events = POLLIN};
	int length, fds[2], result = -1, reader = -1;
	*bytes = NULL;
	if (poll(&ready, 1, 0) < 0 || ioctl(fd, FIONREAD, &length) < 0 ||
		length < 0 || (unsigned)length > MAX_INPUT_BYTES) return -1;
	*eof = !!(ready.revents & POLLHUP);
	if (!length) return 0;
	if (pipe2(fds, O_CLOEXEC | O_NONBLOCK) < 0) return -1;
	if ((fcntl(fd, F_GETFL) & O_ACCMODE) == O_WRONLY) {
		reader = reopen_descriptor(fd, O_RDONLY | O_CLOEXEC | O_NONBLOCK);
	}
	int capacity = fcntl(fd, F_GETPIPE_SZ);
	if (capacity > 0 && (fcntl(fds[1], F_GETPIPE_SZ) >= capacity || fcntl(fds[1], F_SETPIPE_SZ, capacity) >= capacity) &&
		(*bytes = malloc((size_t)length)) && tee(reader >= 0 ? reader : fd, fds[1], (size_t)length, SPLICE_F_NONBLOCK) == length &&
		transfer(fds[0], *bytes, (size_t)length, 0) == 0) result = length;
	close(reader); close(fds[0]); close(fds[1]);
	if (result < 0) { free(*bytes); *bytes = NULL; }
	return result;
}

struct socket_state { unsigned peer, type; int shutdown, queued, allocated; };

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
	if (size >= (ssize_t)NLMSG_LENGTH(sizeof(struct nlmsgerr)) && NLMSG_OK(header, size) &&
		header->nlmsg_type == NLMSG_ERROR && ((struct nlmsgerr *)NLMSG_DATA(header))->error == -ENOENT) { result = 1; goto done; }
	if (size < 0 || !NLMSG_OK(header, size) || header->nlmsg_type != SOCK_DIAG_BY_FAMILY || header->nlmsg_seq != 1 ||
		header->nlmsg_len < NLMSG_LENGTH(sizeof(struct unix_diag_msg))) goto done;
	struct unix_diag_msg *message = NLMSG_DATA(header);
	if (message->udiag_ino != inode || message->udiag_family != AF_UNIX ||
		(message->udiag_type != SOCK_STREAM && message->udiag_type != SOCK_DGRAM && message->udiag_type != SOCK_SEQPACKET)) goto done;
	state->type = message->udiag_type;
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

/* Temporary MSG_PEEK handles never become owners. Unrecognized control data refuses the image. */
static int receive_rights(struct msghdr *message, struct queue_rights *rights) {
	int valid = !(message->msg_flags & (MSG_CTRUNC | MSG_OOB));
	for (struct cmsghdr *header = CMSG_FIRSTHDR(message); header; header = CMSG_NXTHDR(message, header)) {
		if (header->cmsg_level != SOL_SOCKET || header->cmsg_type != SCM_RIGHTS || header->cmsg_len < CMSG_LEN(0)) { valid = 0; continue; }
		for (size_t offset = 0; offset + sizeof(int) <= header->cmsg_len - CMSG_LEN(0); offset += sizeof(int)) {
			int fd; memcpy(&fd, CMSG_DATA(header) + offset, sizeof(fd));
			if (rights->count == MAX_POSITIONS) { close(fd); valid = 0; }
			else rights->fds[rights->count++] = fd;
		}
	}
	return valid;
}
static void close_rights(struct queue_rights *rights) {
	for (unsigned index = 0; index < rights->count; index++) close(rights->fds[index]);
	rights->count = 0;
}

static void close_messages(struct queue_messages *messages) {
	for (unsigned index = 0; index < messages->count; index++) close_rights(&messages->entries[index].rights);
	free(messages->entries); *messages = (struct queue_messages){0};
}

static int append_message(struct queue_messages *messages, const struct queue_message *message) {
	unsigned rights = message->rights.count;
	for (unsigned index = 0; index < messages->count; index++) rights += messages->entries[index].rights.count;
	if (!message->rights.count || rights > MAX_POSITIONS || messages->count == MAX_POSITIONS ||
		message->start >= message->end || message->end > MAX_INPUT_BYTES ||
		(messages->count && messages->entries[messages->count - 1].end > message->start)) return -1;
	struct queue_message *grown = realloc(messages->entries, (messages->count + 1) * sizeof(*grown));
	if (!grown) return -1;
	messages->entries = grown; grown[messages->count++] = *message; return 0;
}

static int peek_cursor(int fd, int offset) {
	int result;
	do { result = setsockopt(fd, SOL_SOCKET, SO_PEEK_OFF, &offset, sizeof(offset)); } while (result < 0 && errno == EINTR);
	return result;
}

static ssize_t peek_message(int fd, void *bytes, size_t length, struct queue_rights *rights, void *fault_tail) {
	struct iovec vector[2] = {{bytes, length - !!fault_tail}, {fault_tail, 1}};
	union { struct cmsghdr alignment; unsigned char bytes[CMSG_SPACE(MAX_POSITIONS * sizeof(int))]; } control = {0};
	struct msghdr message = {.msg_iov = vector, .msg_iovlen = fault_tail ? 2 : 1, .msg_control = control.bytes, .msg_controllen = sizeof(control.bytes)};
	ssize_t copied = recvmsg(fd, &message, MSG_PEEK | MSG_DONTWAIT | MSG_CMSG_CLOEXEC);
	int valid = receive_rights(&message, rights);
	return valid ? copied : -1;
}

/* PEEK can attach rights from a following skb even after filling its data buffer.
 * A fault at the control skb's last byte returns only completed preceding skbs:
 * unix_stream_read_generic does not count or attach an skb whose copy failed. */
static ssize_t control_prefix(int fd, void *bytes, size_t length, void *fault) {
	struct queue_rights probe = {0};
	ssize_t prefix = peek_message(fd, bytes, length, &probe, fault);
	int valid = !probe.count && prefix < (ssize_t)length && (prefix >= 0 || errno == EFAULT);
	close_rights(&probe);
	return valid ? prefix < 0 ? 0 : prefix : -1;
}

/* This cursor is shared by every socket handle. Only the stopped owned tree may
 * lend it; simple head snapshots continue to need no mutation or tracking. */
static int owned_pin(struct descriptor_domain *domain, int pin) {
	if (!domain || !domain->enabled || domain->escaped || domain->uncertain || domain->incomplete) return 0;
	struct resource_object *object = pin_object(domain, pin);
	return object && object->internal && !object->foreign;
}

/* Capture bytes and ancillary barriers together. The snapshot owns installed peek
 * handles until they are mapped to the existing OFD graph, never to synthetic FDs. */
static int socket_bytes(int fd, unsigned char **bytes, int *eof, struct queue_messages *messages, int exclusive, int *needs_tracking) {
	int length, result = -1, moved = 0; *bytes = NULL;
	void *fault = MAP_FAILED;
	struct pollfd ready = {.fd = fd, .events = POLLIN | POLLRDHUP};
	struct queue_rights rights = {0};
	int type = socket_option(fd, SO_TYPE);
	if ((type != SOCK_STREAM && type != SOCK_DGRAM && type != SOCK_SEQPACKET) || socket_option(fd, SO_DOMAIN) != AF_UNIX ||
		socket_option(fd, SO_PEEK_OFF) != -1 || socket_option(fd, SO_PASSCRED) != 0 || socket_option(fd, SO_PASSSEC) != 0 ||
		socket_option(fd, SO_RCVLOWAT) != 1 || socket_option(fd, SO_OOBINLINE) != 0 ||
		poll(&ready, 1, 0) < 0 || ioctl(fd, FIONREAD, &length) < 0 || length < 0 || (unsigned)length > MAX_INPUT_BYTES) return -1;
	*eof = type != SOCK_DGRAM && !!(ready.revents & (POLLRDHUP | POLLHUP));
	/* Packet peeks can irreversibly mark zero-length skbs as peeked. An empty
	 * captured queue is proven without peeking; produced packets use the journal. */
	if (type != SOCK_STREAM) return length || (ready.revents & POLLIN) ? -1 : 0;
	if (!length) return 0;
	if (!(*bytes = malloc((size_t)length))) return -1;
	ssize_t copied = peek_message(fd, *bytes, (size_t)length, &rights, 0);
	if (copied < 0 || copied > length) goto done;
	if (!rights.count) { result = (int)copied; goto done; }
	fault = mmap(NULL, 1, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	if (fault == MAP_FAILED) goto done;
	if (copied == length) {
		ssize_t prefix = control_prefix(fd, *bytes, (size_t)copied, fault);
		if (prefix < 0) goto done;
		struct queue_message message = {.start = (size_t)prefix, .end = (size_t)copied, .rights = rights};
		if (append_message(messages, &message) < 0) goto done;
		rights.count = 0; result = (int)copied; goto done;
	}
	if (!exclusive) { if (needs_tracking) *needs_tracking = 1; goto done; }
	close_rights(&rights);
	if (peek_cursor(fd, 0) < 0) goto done;
	moved = 1;
	size_t used = 0;
	while (used < (size_t)length) {
		if (peek_cursor(fd, (int)used) < 0) goto done;
		copied = peek_message(fd, *bytes + used, (size_t)length - used, &rights, 0);
		if (!copied || (copied < 0 && errno == EAGAIN && !rights.count)) break;
		if (copied < 0 || (size_t)copied > (size_t)length - used) goto done;
		if (rights.count) {
			if (peek_cursor(fd, (int)used) < 0) goto done;
			ssize_t prefix = control_prefix(fd, *bytes + used, (size_t)copied, fault);
			if (prefix < 0) goto done;
			struct queue_message message = {.start = used + (size_t)prefix, .end = used + (size_t)copied, .rights = rights};
			if (append_message(messages, &message) < 0) goto done;
			rights.count = 0;
		}
		used += (size_t)copied;
	}
	result = (int)used;
done:
	/* A failed reset must never resume the authoritative tree with altered state. */
	if (moved && peek_cursor(fd, -1) < 0) _exit(125);
	if (fault != MAP_FAILED) munmap(fault, 1);
	close_rights(&rights);
	if (result < 0) { close_messages(messages); free(*bytes); *bytes = NULL; }
	return result;
}

/* One wire representation is shared by preparation and final graph transfer. */
static int parse_queue_message(const char *line, int *fd, struct queue_message *message) {
	int cursor;
	if (sscanf(line, "M %d %zu %zu %u %n", fd, &message->start, &message->end, &message->rights.count, &cursor) != 4 ||
		*fd < 0 || !message->rights.count || message->rights.count > MAX_POSITIONS) return -1;
	const char *next = line + cursor;
	for (unsigned index = 0; index < message->rights.count; index++) {
		char *end; long value = strtol(next, &end, 10);
		if (end == next || value < 0 || value > INT_MAX) return -1;
		message->rights.fds[index] = (int)value; next = end;
	}
	return *next && strcmp(next, "\n") ? -1 : 0;
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
			struct pollfd ready = {.fd = fd, .events = POLLOUT};
			if (poll(&ready, 1, 0) < 0 || (ready.revents & POLLERR)) return -1;
		}
		return state->capacity >= 4096 ? 0 : -1;
	}
	struct socket_state endpoint, peer = {.shutdown = 3};
	if (!S_ISSOCK(metadata.st_mode) || socket_state(metadata.st_ino, &endpoint) != 0 ||
		(endpoint.peer && (socket_state(endpoint.peer, &peer) != 0 || peer.peer != metadata.st_ino))) return -1;
	state->stream = endpoint.peer ? 4 : 5; state->shutdown = endpoint.shutdown; state->peer_shutdown = peer.shutdown;
	state->socket_type = endpoint.type;
	state->peer_queued = peer.queued; state->allocated = endpoint.allocated;
	state->peer_inode = endpoint.peer; state->capacity = socket_option(fd, SO_SNDBUF);
	return state->capacity >= 8192 ? 0 : -1;
}

static void clear_references(struct decision_job *job) {
	int cancellation; pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &cancellation);
	for (unsigned index = 0; index < job->reference_count; index++) close(job->references[index].pin);
	free(job->references); job->references = NULL; job->reference_count = 0; job->reference_state = 0;
	pthread_setcancelstate(cancellation, NULL);
}

static int add_reference(struct decision_job *job, const struct stat *state, int flags, int pin) {
	if (flags < 0) return -1;
	if (pin >= 0 && S_ISSOCK(state->st_mode)) for (unsigned index = 0; index < job->reference_count; index++) {
		const struct file_reference *entry = &job->references[index];
		if (entry->pin >= 0 && entry->device == state->st_dev && entry->inode == state->st_ino) { close(pin); return 0; }
	}
	if (job->reference_count == MAX_OBJECTS) return -1;
	job->references[job->reference_count++] = (struct file_reference){state->st_dev, state->st_ino, (flags & O_ACCMODE) + 1, pin, S_ISSOCK(state->st_mode)};
	return 0;
}

/* Only queues represented in the current graph own their references through its journal. */
static int represented_queue(struct decision_job *job, dev_t device, ino_t inode) {
	const struct file_position *positions = job->positions ? job->positions : job->captures;
	unsigned count = job->positions ? job->position_count : job->capture_count;
	for (unsigned index = 0; index < count; index++)
		if (positions[index].device == (uintmax_t)device && positions[index].inode == (uintmax_t)inode) return 1;
	return 0;
}

/* References outside the exec image survive its closes. No snapshot pin is an owner. */
/* Decision threads walk the tracer's process list; the tracer unlinks and releases nodes only under this lock. */
static pthread_mutex_t process_list_lock = PTHREAD_MUTEX_INITIALIZER;

static int walk_process_list(int (*walk)(struct decision_job *, int), struct decision_job *job, int pin) {
	int state, result;
	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &state); pthread_mutex_lock(&process_list_lock);
	result = walk(job, pin);
	pthread_mutex_unlock(&process_list_lock); pthread_setcancelstate(state, NULL);
	return result;
}

/* Discover every rooted queue before testing historical objects for lost ownership.
 * Observation pins preserve identity and reachability, never a new resource owner. */
static int queue_owners_locked(struct decision_job *job) {
	struct descriptor_domain *domain = job->domain;
	if (job->reference_state) return job->reference_state > 0 ? 0 : -1;
	job->reference_state = -1;
	job->references = calloc(MAX_OBJECTS, sizeof(*job->references));
	if (!job->references) return -1;
	for (struct traced_process *process = *domain->processes; process; process = process->next) {
		if (process->historical || (!domain->enabled && process == job->process)) continue;
		int fds[MAX_HANDLES], count = descriptor_numbers(process->pid, 0, fds, MAX_HANDLES);
		if (count < 0) return -1;
		for (int index = 0; index < count; index++) {
			struct stat state;
			if (domain->enabled) {
				struct resource_object *object = origin_object(domain, descriptor_origin(process, fds[index]));
				if (!object || !object->messages) continue;
				state = (struct stat){.st_dev = object->device, .st_ino = object->inode, .st_mode = S_IFSOCK};
			} else {
				if (descriptor_stat(process->pid, fds[index], &state) < 0) return -1;
				if (!S_ISFIFO(state.st_mode) && !S_ISSOCK(state.st_mode)) continue;
				if (add_reference(job, &state, descriptor_info(process->pid, fds[index], NULL), -1) < 0) return -1;
			}
			if (!S_ISSOCK(state.st_mode) || represented_queue(job, state.st_dev, state.st_ino)) continue;
			struct decision_job source = {.pid = process->pid, .pidfd = -1};
			int pin = duplicate_tracee_fd(&source, (unsigned)fds[index]); close(source.pidfd);
			if (pin < 0) return -1;
			if (add_reference(job, &state, fcntl(pin, F_GETFL), pin) < 0) { close(pin); return -1; }
		}
	}
	for (unsigned cursor = 0; cursor < job->reference_count; cursor++) {
		struct file_reference source = job->references[cursor];
		if (source.pin < 0 || !source.queue || represented_queue(job, source.device, source.inode)) continue;
		unsigned char *bytes; int eof; struct queue_messages messages = {0};
		int valid = socket_bytes(source.pin, &bytes, &eof, &messages, owned_pin(domain, source.pin), &job->needs_tracking) >= 0;
		free(bytes);
		for (unsigned message = 0; valid && message < messages.count; message++) for (unsigned index = 0; valid && index < messages.entries[message].rights.count; index++) {
			int reference = messages.entries[message].rights.fds[index]; struct stat state;
			if (fstat(reference, &state) < 0 || add_reference(job, &state, fcntl(reference, F_GETFL), reference) < 0) { valid = 0; break; }
			messages.entries[message].rights.fds[index] = -1;
		}
		close_messages(&messages); if (!valid) return -1;
	}
	for (unsigned index = 0; domain->enabled && index < domain->object_count; index++) {
		const struct resource_object *queue = &domain->objects[index];
		if (!queue->messages || represented_queue(job, queue->device, queue->inode)) continue;
		unsigned reference = 0;
		while (reference < job->reference_count && (job->references[reference].device != queue->device || job->references[reference].inode != queue->inode)) reference++;
		/* Unrooted cyclic queues still own their rights until the kernel destroys them. */
		struct socket_state state;
		if (reference == job->reference_count && socket_state(queue->inode, &state) != 1) return -1;
	}
	job->reference_state = 1;
	return 0;
}

/* The frozen kernel table proves current owners even during an unrelated close.
 * Streams count endpoint access; files and counters require exact OFD identity. */
static int outside_references_locked(struct decision_job *job, int pin) {
	struct descriptor_domain *domain = job->domain;
	struct stat state; if (fstat(pin, &state) < 0) return -1;
	int description = !S_ISFIFO(state.st_mode) && !S_ISSOCK(state.st_mode), unknown = description ? -1 : 3, access = 0;
	if (!domain || (domain->enabled && (!owned_pin(domain, pin) || !job->process->table || !job->process->table->complete))) return unknown;
	for (struct traced_process *process = *domain->processes; (description || domain->enabled) && process; process = process->next) {
		if (process == job->process || process->historical) continue;
		int fds[MAX_HANDLES], count = descriptor_numbers(process->pid, 0, fds, MAX_HANDLES);
		if (count < 0) return unknown;
		for (int index = 0; index < count; index++) {
			if (!description) {
				struct stat current;
				if (descriptor_stat(process->pid, fds[index], &current) < 0) return unknown;
				if (current.st_dev == state.st_dev && current.st_ino == state.st_ino) {
					int flags = descriptor_info(process->pid, fds[index], NULL); if (flags < 0) return unknown;
					access |= (flags & O_ACCMODE) + 1;
				}
				continue;
			}
			long same = syscall(SYS_kcmp, process->pid, getpid(), KCMP_FILE, fds[index], pin);
			if (same < 0) return -1;
			if (!same) return 1;
		}
	}
	if (queue_owners_locked(job) < 0) return unknown;
	for (unsigned index = 0; index < job->reference_count; index++) {
		if (job->references[index].device != state.st_dev || job->references[index].inode != state.st_ino) continue;
		if (!description) { access |= job->references[index].access; continue; }
		if (job->references[index].pin < 0) continue;
		long same = syscall(SYS_kcmp, getpid(), getpid(), KCMP_FILE, job->references[index].pin, pin);
		if (same < 0) return -1;
		if (!same) return 1;
	}
	return access & 3;
}

static void discard_captures(struct decision_job *job) {
	for (unsigned index = 0; index < job->capture_count; index++) {
		struct file_position *capture = &job->captures[index];
		close(capture->duplicate); free(capture->content); free(capture->messages.entries); free(capture->locks.entries);
	}
	free(job->captures); job->captures = NULL; job->capture_count = 0;
}

/* Queue references extend the same OFD graph. Peek handles become bounded job pins;
 * no synthetic slot is installed in either the authoritative or private process. */
static int capture_rights(struct decision_job *job, struct queue_messages *messages, int *next) {
	int ids[MAX_POSITIONS]; unsigned count = 0;
	for (unsigned message = 0; message < messages->count; message++) for (unsigned right = 0; right < messages->entries[message].rights.count; right++) {
		int pin = messages->entries[message].rights.fds[right];
		unsigned other = 0;
		for (; other < job->capture_count; other++) {
			long same = syscall(SYS_kcmp, getpid(), getpid(), KCMP_FILE, pin, job->captures[other].duplicate);
			if (same < 0) goto fail;
			if (!same) break;
		}
		if (count == MAX_POSITIONS) goto fail;
		if (other == job->capture_count) {
			if (other == MAX_POSITIONS || *next == INT_MAX) goto fail;
			job->captures[job->capture_count++] = (struct file_position){.descriptor = (*next)++, .duplicate = pin};
		} else close(pin);
		messages->entries[message].rights.fds[right] = -1;
		ids[count++] = job->captures[other].descriptor;
	}
	count = 0;
	for (unsigned message = 0; message < messages->count; message++) for (unsigned right = 0; right < messages->entries[message].rights.count; right++)
		messages->entries[message].rights.fds[right] = ids[count++];
	return 0;
fail:
	close_messages(messages);
	return -1;
}

/* The entire owned tree is stopped until this job retires. Capture the reachable
 * closure before calculating external owners, including rooted queue cycles. */
static int descriptor_context(struct decision_job *job, char *line, size_t capacity) {
	FILE *out = NULL;
	int installed[MAX_HANDLES], total = descriptor_numbers(job->pid, 0, installed, MAX_HANDLES), next = 3;
	if (total < 0 || !(job->captures = calloc(MAX_POSITIONS, sizeof(*job->captures)))) return -1;
	for (int index = 0; index < total; index++) {
		int descriptor = installed[index];
		if (descriptor == INT_MAX) goto fail;
		if (descriptor >= next) next = descriptor + 1;
		int pin = duplicate_tracee_fd(job, (unsigned)descriptor);
		struct stat state;
		if (pin < 0) goto fail;
		if (fstat(pin, &state) < 0) { close(pin); goto fail; }
		unsigned event = !(state.st_mode & S_IFMT) ? event_state(getpid(), pin, NULL, NULL) : 0;
		/* Unowned streams still describe the launch. Ownership remains a separate
		 * adoption proof; dropping their context prevents even safe discovery. */
		if (!S_ISREG(state.st_mode) && !S_ISDIR(state.st_mode) &&
			!((null_device(&state) || event || S_ISFIFO(state.st_mode) || S_ISSOCK(state.st_mode)) && descriptor != 1 && descriptor != 2)) { close(pin); continue; }
		if (job->capture_count == MAX_POSITIONS) { close(pin); goto fail; }
		job->captures[job->capture_count++] = (struct file_position){.descriptor = descriptor, .duplicate = pin, .installed = 1};
	}
	for (unsigned index = 0; index < job->capture_count; index++) for (unsigned previous = index; previous && job->captures[previous].descriptor < job->captures[previous - 1].descriptor; previous--) {
		struct file_position swap = job->captures[previous]; job->captures[previous] = job->captures[previous - 1]; job->captures[previous - 1] = swap;
	}
	for (unsigned index = 0; index < job->capture_count; index++) {
		struct file_position *capture = &job->captures[index];
		int pin = capture->duplicate, flags = capture->flags = fcntl(pin, F_GETFL);
		struct stat state;
		if (flags < 0 || fstat(pin, &state) < 0) goto fail;
		capture->device = state.st_dev; capture->inode = state.st_ino;
		capture->event = !(state.st_mode & S_IFMT) ? event_state(getpid(), pin, NULL, NULL) : 0;
		capture->directory = S_ISDIR(state.st_mode);
		if (capture->event) capture->stream = 6;
		else if (S_ISFIFO(state.st_mode) || S_ISSOCK(state.st_mode)) { if (stream_state(pin, capture) < 0) goto fail; }
		else if (!S_ISREG(state.st_mode) && !capture->directory && !null_device(&state)) goto fail;
		capture->before = capture->stream ? 0 : descriptor_seek(pin, flags, 0, SEEK_CUR);
		if (capture->before < 0 || (capture->stream && (flags & ~(O_ACCMODE | O_APPEND | O_NONBLOCK | 0x8000)))) goto fail;
		capture->alias = capture->descriptor;
		for (unsigned previous = 0; previous < index; previous++) {
			long same = syscall(SYS_kcmp, getpid(), getpid(), KCMP_FILE, pin, job->captures[previous].duplicate);
			if (same < 0) goto fail;
			unsigned long origin = capture->installed ? descriptor_origin(job->process, capture->descriptor).id : 0;
			unsigned long other = job->captures[previous].installed ? descriptor_origin(job->process, job->captures[previous].descriptor).id : 0;
			if (origin && other && ((same == 0) != (origin == other))) goto fail;
			if (!same) { capture->alias = job->captures[previous].descriptor; break; }
		}
		if (capture->directory && !(flags & O_PATH)) capture->content_length = directory_bytes(pin, &capture->content);
		else if (capture->stream && !capture->event) {
			capture->content_length = capture->stream >= 4 ? socket_bytes(pin, &capture->content, &capture->eof, &capture->messages,
				owned_pin(job->domain, pin), &job->needs_tracking) : pipe_bytes(pin, &capture->content, &capture->eof);
			if (capture_rights(job, &capture->messages, &next) < 0) goto fail;
		}
		if (capture->content_length < 0) goto fail;
		if (S_ISREG(state.st_mode) && capture->alias == capture->descriptor && capture_file_locks(getpid(), pin, &capture->locks) < 0) goto fail;
	}
	/* Ordinary null stdin keeps its profile unless a handle or queued right shares it. */
	if (job->capture_count && job->captures[0].descriptor == 0) {
		struct stat state; int shared = 0;
		if (fstat(job->captures[0].duplicate, &state) < 0) goto fail;
		for (unsigned index = 0; index < job->capture_count; index++) {
			const struct file_position *capture = &job->captures[index];
			if (index && capture->alias == 0) shared = 1;
			for (unsigned message = 0; message < capture->messages.count; message++) for (unsigned right = 0; right < capture->messages.entries[message].rights.count; right++)
				if (!capture->messages.entries[message].rights.fds[right]) shared = 1;
		}
		if (null_device(&state) && !shared) {
			close(job->captures[0].duplicate); job->capture_count--;
			memmove(job->captures, job->captures + 1, job->capture_count * sizeof(*job->captures));
		}
	}
	/* A captured queue can also belong to another process. Its transitive rights
	 * keep OFDs alive; endpoint closes still follow the ordered queue journal. */
	int endpoint_outside[MAX_POSITIONS]; unsigned external[MAX_POSITIONS], count = 0;
	for (unsigned index = 0; index < job->capture_count; index++) {
		endpoint_outside[index] = walk_process_list(outside_references_locked, job, job->captures[index].duplicate);
		if ((job->captures[index].outside = endpoint_outside[index] ? 3 : 0)) external[count++] = index;
	}
	for (unsigned cursor = 0; cursor < count; cursor++) {
		const struct file_position *queue = &job->captures[external[cursor]];
		for (unsigned message = 0; message < queue->messages.count; message++) {
			const struct queue_rights *rights = &queue->messages.entries[message].rights;
			for (unsigned right = 0; right < rights->count; right++) for (unsigned other = 0; other < job->capture_count; other++)
				if (!job->captures[other].outside && job->captures[other].alias == rights->fds[right]) { job->captures[other].outside = 3; external[count++] = other; }
		}
	}
	/* w+ also terminates an empty descriptor list. This thread exclusively owns the stream. */
	out = fmemopen(line, capacity, "w+"); if (!out) goto fail;
	for (unsigned index = 0; index < job->capture_count; index++) {
		struct file_position *capture = &job->captures[index];
		int pin = capture->duplicate, owned = capture->installed ? !job->domain->escaped && !job->domain->uncertain && job->process->table &&
			!job->process->table->active && descriptor_origin(job->process, capture->descriptor).id != 0 : owned_pin(job->domain, pin);
		struct stat state; if (fstat(pin, &state) < 0) goto fail;
		fprintf(out,
			"%s{\"fd\":%d,\"alias\":%d,\"device\":\"%ju\",\"inode\":\"%ju\",\"flags\":%d,\"offset\":%s%jd%s,\"owned\":%s%s",
			index ? "," : "", capture->descriptor, capture->alias, capture->device, capture->inode, capture->flags,
			capture->before > 9007199254740991LL ? "\"" : "", (intmax_t)capture->before, capture->before > 9007199254740991LL ? "\"" : "", owned ? "true" : "false",
			capture->event ? ",\"type\":\"eventfd\"" : capture->stream ? (S_ISSOCK(state.st_mode) ? ",\"type\":\"socket\"" : ",\"type\":\"pipe\"") : null_device(&state) ? ",\"type\":\"null\"" : capture->directory ? ",\"type\":\"directory\"" : "");
		if (!capture->installed) fprintf(out, ",\"pin\":%d", pin);
		if (S_ISREG(state.st_mode) || capture->event) {
			fprintf(out, ",\"outside\":%d", capture->outside);
		}
		if (capture->locks.count) {
			fputs(",\"locks\":[", out);
			for (unsigned index = 0; index < capture->locks.count; index++) {
				struct ofd_lock *lock = &capture->locks.entries[index];
				fprintf(out, "%s{\"kind\":%d,\"type\":%d,\"start\":\"%" PRId64 "\",\"length\":\"%" PRId64 "\"}", index ? "," : "", lock->kind, lock->type, lock->start, lock->length);
			}
			fputc(']', out);
		}
		if (capture->event) {
			uint64_t value; unsigned semaphore;
			if (event_state(getpid(), pin, &value, &semaphore) != capture->event) goto fail;
			fprintf(out, ",\"counter\":{\"id\":%u,\"value\":\"%" PRIu64 "\",\"semaphore\":%u}", capture->event - 1, value, semaphore);
		} else if ((capture->directory && !(capture->flags & O_PATH)) || capture->stream) {
			fprintf(out, ",\"%s\":", capture->directory ? "directoryHex" : "queueHex");
			json_hex(out, capture->content, (size_t)capture->content_length);
			if (capture->stream) fprintf(out, ",\"eof\":%s,\"capacity\":%d,\"outside\":%d", capture->eof ? "true" : "false", capture->capacity, endpoint_outside[index]);
			if (capture->messages.count) {
				fputs(",\"messages\":[", out);
				for (unsigned message = 0; message < capture->messages.count; message++) {
					const struct queue_message *entry = &capture->messages.entries[message];
					fprintf(out, "%s{\"start\":%zu,\"end\":%zu,\"rights\":[", message ? "," : "", entry->start, entry->end);
					for (unsigned right = 0; right < entry->rights.count; right++) fprintf(out, "%s%d", right ? "," : "", entry->rights.fds[right]);
					fputs("]}", out);
				}
				fputc(']', out);
			}
			if (capture->stream >= 4) fprintf(out, ",\"socket\":{\"shutdown\":%d,\"peerShutdown\":%d,\"peerInode\":%u,\"peerQueued\":%d,\"allocated\":%d,\"type\":%u}",
				capture->shutdown, capture->peer_shutdown, capture->peer_inode, capture->peer_queued, capture->allocated, capture->socket_type);
		}
		fputc('}', out);
		if (ferror(out)) goto fail;
	}
	long written = ftell(out);
	int failed = ferror(out) || written < 0 || (size_t)written >= capacity;
	if (fclose(out)) failed = 1;
	out = NULL;
	if (failed) goto fail;
	return 0;
fail:
	if (out) fclose(out);
	discard_captures(job);
	return -1;
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

static int position_state_matches(const struct file_position *position, const struct decision_job *job) {
	struct stat state, named;
	if (fstat(position->duplicate, &state) < 0) return 0;
	if (position->event) {
		uint64_t value; unsigned semaphore;
		return position->stream == 6 && position->content_length == 9 && !position->before && !position->after &&
			event_state(getpid(), position->duplicate, &value, &semaphore) == position->event &&
			!memcmp(&value, position->content, 8) && semaphore == position->content[8] &&
			(uintmax_t)state.st_dev == position->device && (uintmax_t)state.st_ino == position->inode &&
			fcntl(position->duplicate, F_GETFL) == position->flags && !(position->flags & ~(O_RDWR | O_NONBLOCK));
	}
	if (position->stream) {
		unsigned char *bytes;
		struct file_position current = {0};
		struct queue_messages messages = {0};
		int eof, size = position->stream >= 4 ? socket_bytes(position->duplicate, &bytes, &eof, &messages, owned_pin(job->domain, position->duplicate), NULL) : pipe_bytes(position->duplicate, &bytes, &eof);
		int matches = stream_state(position->duplicate, &current) == 0 && current.stream == position->stream && current.socket_type == position->socket_type &&
			current.capacity == position->capacity && current.shutdown == position->shutdown && current.peer_shutdown == position->peer_shutdown && current.peer_inode == position->peer_inode &&
			current.peer_queued == position->peer_queued && current.allocated == position->allocated &&
			!position->before && position->content_length >= 0 &&
			size == position->content_length && eof == position->eof && position->after <= size &&
			(!size || !memcmp(bytes, position->content, (size_t)size)) &&
			(uintmax_t)state.st_dev == position->device && (uintmax_t)state.st_ino == position->inode &&
			fcntl(position->duplicate, F_GETFL) == position->flags && !(position->flags & ~(O_ACCMODE | O_APPEND | O_NONBLOCK | 0x8000)) &&
			!(position->after_flags & ~(O_ACCMODE | O_APPEND | O_NONBLOCK | 0x8000)) && messages.count == position->messages.count;
		for (unsigned message = 0; matches && message < messages.count; message++) {
			const struct queue_message *actual = &messages.entries[message], *expected = &position->messages.entries[message];
			matches = !position->after && actual->start == expected->start && actual->end == expected->end && actual->rights.count == expected->rights.count;
			for (unsigned right = 0; matches && right < actual->rights.count; right++) {
				const struct file_position *other = resource_position(job, expected->rights.fds[right]);
				matches = other && syscall(SYS_kcmp, getpid(), getpid(), KCMP_FILE, actual->rights.fds[right], other->duplicate) == 0;
			}
		}
		close_messages(&messages);
		free(bytes); return matches;
	}
	if ((position->flags & O_PATH) && (position->before || position->after ||
		(!S_ISREG(state.st_mode) && position->content_length != -1) || position->after_flags != position->flags)) return 0;
	if (S_ISDIR(state.st_mode)) {
		char link[64], endpoint[PATH_MAX];
		snprintf(link, sizeof(link), "/proc/self/fd/%d", position->duplicate);
		ssize_t length = readlink(link, endpoint, sizeof(endpoint));
		if (!position->path || length < 0 || strlen(position->path) != (size_t)length || memcmp(endpoint, position->path, (size_t)length) ||
			lstat(position->path, &named) < 0 || named.st_dev != state.st_dev || named.st_ino != state.st_ino) return 0;
		if (!(position->flags & O_PATH)) {
			unsigned char *bytes; int size = directory_bytes(position->duplicate, &bytes);
			int valid = size >= 0 && size == position->content_length && (!size || !memcmp(bytes, position->content, (size_t)size));
			free(bytes); if (!valid) return 0;
		}
	}
	return (S_ISREG(state.st_mode) || (S_ISDIR(state.st_mode) && !(position->flags & O_PATH)) || ((null_device(&state) || S_ISDIR(state.st_mode)) &&
		!position->before && !position->after && position->content_length == -1)) &&
		(uintmax_t)state.st_dev == position->device && (uintmax_t)state.st_ino == position->inode &&
		fcntl(position->duplicate, F_GETFL) == position->flags &&
		descriptor_seek(position->duplicate, position->flags, 0, SEEK_CUR) == position->before;
}

/* A peek lends shared cursor state and installs temporary handles. Cancellation
 * resumes only after restoring that state and releasing every temporary owner. */
static int position_matches(const struct file_position *position, const struct decision_job *job) {
	int state;
	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &state);
	int valid = position_state_matches(position, job);
	pthread_setcancelstate(state, NULL);
	return valid;
}

/* Only the tracer thread may mutate a held image. Each job owns its reply channel. */
static int request_tracer(struct decision_job *job, unsigned code) {
	char reply;
	return transfer(job->channel[1], &code, sizeof(code), 1) < 0 ||
		transfer(job->channel[1], &reply, 1, 0) < 0 || reply != 'Y' ? -1 : 0;
}

/* Receive creates temporary helper handles; their identity is checked against the
 * captured OFDs and every installed handle is closed, including error paths. */
static int transfer_message(struct decision_job *job, int fd, const struct output_event *event, const unsigned *rights, unsigned count) {
	union { struct cmsghdr alignment; unsigned char bytes[CMSG_SPACE(MAX_POSITIONS * sizeof(int))]; } control = {0};
	unsigned char *bytes = event->kind == 2 ? event->data : malloc(event->length + 1);
	if (!bytes) return -1;
	struct iovec vector = {.iov_base = bytes, .iov_len = event->length};
	struct msghdr message = {.msg_iov = &vector, .msg_iovlen = 1};
	if (event->kind != 2 || count) { message.msg_control = control.bytes; message.msg_controllen = event->kind == 2 ? CMSG_SPACE(count * sizeof(int)) : sizeof(control.bytes); }
	int valid;
	if (event->kind == 2) {
		if (count) {
			struct cmsghdr *header = CMSG_FIRSTHDR(&message);
			header->cmsg_level = SOL_SOCKET; header->cmsg_type = SCM_RIGHTS; header->cmsg_len = CMSG_LEN(count * sizeof(int));
			for (unsigned index = 0; index < count; index++) ((int *)CMSG_DATA(header))[index] = job->positions[rights[index]].duplicate;
		}
		valid = sendmsg(fd, &message, MSG_DONTWAIT | MSG_NOSIGNAL) == (ssize_t)event->length;
	} else {
		ssize_t received = recvmsg(fd, &message, MSG_DONTWAIT | MSG_CMSG_CLOEXEC);
		valid = received == (ssize_t)event->length && !memcmp(bytes, event->data, event->length) && !(message.msg_flags & MSG_CTRUNC);
		struct queue_rights installed = {0};
		valid &= receive_rights(&message, &installed) && installed.count == count;
		for (unsigned index = 0; index < installed.count && index < count; index++)
			valid &= syscall(SYS_kcmp, getpid(), getpid(), KCMP_FILE, installed.fds[index], job->positions[rights[index]].duplicate) == 0;
		close_rights(&installed);
		free(bytes);
	}
	return valid ? 0 : -1;
}

/* One ordered transition interpreter validates an in-memory queue graph, then applies
 * that same sequence under the tree lease. Aliases never own another copy of a queue. */
/* The same graph is instantiated with kernel lock semantics during preflight.
 * No real lock is acquired until the common resource transaction is committed. */
static int closed_lock_object(struct decision_job *job, int pin) {
	struct descriptor_domain *domain = job->domain;
	struct resource_object *object = pin_object(domain, pin);
	if (!owned_pin(domain, pin) || !object->anonymous || queue_owners_locked(job) < 0) return 0;
	for (unsigned index = 0; index < job->reference_count; index++)
		if (job->references[index].device == object->device && job->references[index].inode == object->inode) return 0;
	for (struct traced_process *process = *domain->processes; process; process = process->next) {
		if (process->historical) continue;
		int fds[MAX_HANDLES], count = descriptor_numbers(process->pid, 0, fds, MAX_HANDLES);
		if (!process->table || !process->table->complete || count < 0) return 0;
		for (int index = 0; index < count; index++) {
			struct stat state;
			if (descriptor_stat(process->pid, fds[index], &state) < 0) return 0;
			if (state.st_dev != object->device || state.st_ino != object->inode) continue;
			unsigned represented = 0;
			while (represented < job->position_count && syscall(SYS_kcmp, process->pid, getpid(), KCMP_FILE, fds[index], job->positions[represented].duplicate) != 0) represented++;
			if (represented == job->position_count) return 0;
			struct file_locks locks = {0};
			int valid = capture_file_locks(process->pid, fds[index], &locks) == 0;
			free(locks.entries); if (!valid) return 0;
		}
	}
	return 1;
}

static int lock_positions(struct decision_job *job, int *selected) {
	int locks = 0;
	for (unsigned index = 0; index < job->position_count; index++) {
		int used = job->positions[index].locks.count != 0;
		for (unsigned event = 0; !used && event < job->resource_count; event++)
			used = job->events[event].kind == 13 && job->positions[index].descriptor == (int)job->events[event].fd;
		if (used) for (unsigned other = 0; other < job->position_count; other++)
			if (same_object(&job->positions[index], &job->positions[other])) selected[other] = locks = 1;
	}
	return locks;
}

static int prepare_lock_graph(struct decision_job *job, int *fds, const int *selected) {
	int matched[MAX_POSITIONS][MAX_POSITIONS] = {{0}};
	/* PID_NS_INIT_INO is reserved by Linux. Both ends also verify this proc mount's
	 * namespace: nested views omit flock owners whose creating process has exited. */
	struct stat self_namespace, root_namespace;
	int complete_view = stat("/proc/self/ns/pid", &self_namespace) == 0 && self_namespace.st_ino == 0xeffffffcUL &&
		stat("/proc/1/ns/pid", &root_namespace) == 0 && root_namespace.st_ino == self_namespace.st_ino && root_namespace.st_dev == self_namespace.st_dev;
	for (unsigned index = 0; index < job->position_count; index++) {
		struct file_position *position = &job->positions[index];
		if (!selected[index]) continue;
		if (position->alias) { fds[index] = fds[position->alias - 1]; continue; }
		if (!complete_view && !walk_process_list(closed_lock_object, job, position->duplicate)) return -1;
		struct stat state; struct statfs filesystem; struct file_locks current = {0};
		if (fstat(position->duplicate, &state) < 0 || !S_ISREG(state.st_mode) || fstatfs(position->duplicate, &filesystem) < 0 ||
			(filesystem.f_type != EXT4_SUPER_MAGIC && filesystem.f_type != TMPFS_MAGIC && filesystem.f_type != RAMFS_MAGIC)) return -1;
		int valid = capture_file_locks(getpid(), position->duplicate, &current) == 0 && current.count == position->locks.count;
		for (unsigned entry = 0; valid && entry < current.count; entry++) valid = !compare_locks(&current.entries[entry], &position->locks.entries[entry]);
		free(current.entries); if (!valid) return -1;
		unsigned previous = 0; while (previous < index && !same_object(position, &job->positions[previous])) previous++;
		int object = previous < index ? fds[previous] : memfd_create("pi-resource-locks", MFD_CLOEXEC);
		if (object < 0) return -1;
		fds[index] = reopen_descriptor(object, (position->flags & (O_ACCMODE | O_PATH)) | O_CLOEXEC);
		if (previous == index) close(object);
		if (fds[index] < 0) return -1;
	}
	FILE *file = complete_view ? fopen("/proc/locks", "re") : NULL; if (complete_view && !file) return -1;
	char line[512]; int valid = 1;
	while (file && valid && fgets(line, sizeof(line), file)) {
		struct ofd_lock lock; dev_t device; ino_t inode;
		if (parse_file_lock(line, &lock, &device, &inode) < 0) { valid = 0; break; }
		int relevant = 0, found = 0;
		for (unsigned index = 0; index < job->position_count; index++) {
			struct file_position *position = &job->positions[index];
			if (!selected[index] || position->alias || position->device != (uintmax_t)device || position->inode != (uintmax_t)inode) continue;
			relevant = 1;
			for (unsigned entry = 0; !found && entry < position->locks.count; entry++) if (!matched[index][entry] && !compare_locks(&lock, &position->locks.entries[entry]))
				matched[index][entry] = found = 1;
		}
		if (relevant && !found) valid = 0;
	}
	if (file) { if (ferror(file)) valid = 0; fclose(file); }
	for (unsigned index = 0; valid && index < job->position_count; index++) if (selected[index] && !job->positions[index].alias)
		for (unsigned entry = 0; valid && entry < job->positions[index].locks.count; entry++)
			valid = (!complete_view || matched[index][entry]) && set_file_lock(fds[index], job->positions[index].locks.entries[entry]) == 0;
	return valid ? 0 : -1;
}

static int lock_event(int fd, const struct output_event *event) {
	char line[256]; struct ofd_lock lock; int error, observed_type, cursor; int64_t observed_start, observed_length;
	if (fd < 0 || event->length >= sizeof(line)) return -1;
	memcpy(line, event->data, event->length); line[event->length] = 0;
	if (sscanf(line, "%d %d %" SCNd64 " %" SCNd64 " %d %d %" SCNd64 " %" SCNd64 " %n", &lock.kind, &lock.type, &lock.start, &lock.length,
		&error, &observed_type, &observed_start, &observed_length, &cursor) != 8 || line[cursor] || lock.kind < 0 || lock.kind > 2 ||
		line[0] != '0' + lock.kind || line[1] != ' ' ||
		lock.type < 0 || lock.type > F_UNLCK || lock.start < 0 || lock.length < 0 || lock.start > INT64_MAX - lock.length ||
		(!lock.kind && (lock.start || lock.length)) || (error && error != EAGAIN && error != EBADF && error != EINVAL)) return -1;
	struct flock range = {.l_type = lock.type, .l_whence = SEEK_SET, .l_start = lock.start, .l_len = lock.length};
	int result = lock.kind == 2 ? fcntl(fd, F_OFD_GETLK, &range) : set_file_lock(fd, lock);
	if ((result < 0 ? errno : 0) != error) return -1;
	return !error && lock.kind == 2 && (range.l_type != observed_type || range.l_start != observed_start || range.l_len != observed_length ||
		(range.l_type != F_UNLCK && range.l_pid != -1)) ? -1 : 0;
}

static int resource_events(struct decision_job *job, int apply) {
	if (!job->resource_count) return 0;
	int selected[MAX_POSITIONS] = {0}, lock_fds[MAX_POSITIONS], cancellation = PTHREAD_CANCEL_ENABLE;
	int locks = lock_positions(job, selected);
	for (unsigned index = 0; index < MAX_POSITIONS; index++) lock_fds[index] = -1;
	if (locks) pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &cancellation);
	struct { unsigned char *bytes; size_t length, produced, consumed; uint64_t counter; unsigned semaphore, writes; int eof, shutdown, owner, writable, flags, description, error; } queues[MAX_POSITIONS] = {0};
	size_t message_capacity = job->resource_count;
	for (unsigned index = 0; index < job->position_count; index++) message_capacity += job->positions[index].messages.count;
	struct control_message { int object; size_t start, end; unsigned count, rights[MAX_POSITIONS]; } *messages = calloc(message_capacity ? message_capacity : 1, sizeof(*messages));
	unsigned message_count = 0;
	int result = -1;
	if (!messages || (locks && !apply && prepare_lock_graph(job, lock_fds, selected) < 0)) goto done;
	for (unsigned index = 0; index < job->position_count; index++) {
		const struct file_position *position = &job->positions[index];
		queues[index].owner = (int)index;
		queues[index].description = position->alias ? queues[position->alias - 1].description : (int)index;
		queues[index].flags = position->flags;
		if (!position->stream) continue;
		for (unsigned previous = 0; previous < index; previous++) if (same_object(position, &job->positions[previous])) {
			queues[index].owner = queues[previous].owner; break;
		}
		if (queues[index].owner != (int)index) continue;
		for (unsigned message = 0; message < position->messages.count; message++) {
			const struct queue_message *captured = &position->messages.entries[message];
			if (position->stream < 4 || position->event || position->content_length <= 0 || position->after || captured->end > (size_t)position->content_length) goto done;
			struct control_message *control = &messages[message_count++];
			control->object = (int)index; control->start = captured->start; control->end = captured->end; control->count = captured->rights.count;
			for (unsigned right = 0; right < control->count; right++) {
				const struct file_position *other = resource_position(job, captured->rights.fds[right]);
				if (!other) goto done;
				control->rights[right] = (unsigned)(other - job->positions);
			}
		}
		if (position->event) {
			if (position->content_length != 9) goto done;
			memcpy(&queues[index].counter, position->content, 8); queues[index].semaphore = position->content[8]; continue;
		}
		queues[index].length = (size_t)(position->content_length - position->after);
		queues[index].eof = position->eof; queues[index].shutdown = position->shutdown;
		if (position->stream < 4) for (unsigned other = index; other < job->position_count; other++) {
			const struct file_position *writer = &job->positions[other];
			if (writer->device != position->device || writer->inode != position->inode || (writer->flags & O_ACCMODE) == O_RDONLY) continue;
			struct pollfd ready = {.fd = writer->duplicate, .events = POLLOUT};
			if (poll(&ready, 1, 0) < 0 || (ready.revents & POLLERR)) goto done;
			queues[index].writable = !!(ready.revents & POLLOUT); break;
		}
		queues[index].bytes = malloc(queues[index].length + 4096 + 1);
		if (!queues[index].bytes) goto done;
		if (queues[index].length) memcpy(queues[index].bytes, position->content + position->after, queues[index].length);
	}
	for (unsigned index = 0; index < job->resource_count; index++) {
		struct output_event decoded = job->events[index];
		const struct output_event *event = &decoded;
		unsigned rights[MAX_POSITIONS], right_count = 0;
		int message = event->kind >= 8 && event->kind <= 10, moving = event->kind == 11, copying = event->kind == 12, source = -1;
		if (message) {
			if (event->kind > 10 || event->length < 4) goto done;
			memcpy(&right_count, event->data, 4);
			if (right_count > MAX_POSITIONS || event->length < 4 + 4 * right_count) goto done;
			for (unsigned right = 0; right < right_count; right++) {
				unsigned fd; memcpy(&fd, event->data + 4 + 4 * right, 4);
				const struct file_position *other = resource_position(job, (int)fd); if (!other) goto done;
				rights[right] = (unsigned)(other - job->positions);
			}
			decoded.kind -= 8; decoded.data += 4 + 4 * right_count; decoded.length -= 4 + 4 * right_count;
		}
		if (moving || copying) {
			unsigned fd;
			if (event->length < 4) goto done;
			memcpy(&fd, event->data, 4);
			const struct file_position *other = resource_position(job, (int)fd); if (!other) goto done;
			source = (int)(other - job->positions);
			decoded.kind = 2; decoded.data += 4; decoded.length -= 4;
		}
		const struct file_position *position = resource_position(job, (int)event->fd); if (!position) goto done;
		unsigned handle = (unsigned)(position - job->positions);
		if (event->kind == 4) {
			if (event->length != 1 || !event->data[0] || event->data[0] > 3 || ((!position->stream || position->event) && event->data[0] != 3)) goto done;
			for (unsigned cursor = 0; cursor < message_count; cursor++) if (messages[cursor].object != -1)
				for (unsigned right = 0; right < messages[cursor].count; right++) {
					unsigned reference = messages[cursor].rights[right];
					if (!position->stream || position->event ? queues[reference].description == queues[handle].description :
						queues[reference].owner == queues[handle].owner && (((queues[reference].flags & O_ACCMODE) + 1) & event->data[0])) goto done;
				}
		}
		if (!position->stream) {
			if (event->kind == 13) {
				if (!apply && event->length && event->data[0] == '2') {
					unsigned owners = 0;
					for (unsigned other = 0; other < job->position_count; other++) if (!job->positions[other].alias && lock_fds[other] >= 0 &&
						lock_fds[other] != lock_fds[handle] && same_object(position, &job->positions[other])) {
						struct file_locks current = {0}; int valid = capture_file_locks(getpid(), lock_fds[other], &current) == 0, owned = 0;
						for (unsigned entry = 0; entry < current.count; entry++) owned |= current.entries[entry].kind == 1;
						free(current.entries); if (!valid || (owners += owned) > 1) goto done;
					}
				}
				if (lock_event(apply ? position->duplicate : lock_fds[handle], event) < 0) goto done;
			} else {
				if (event->kind != 4) goto done;
				if (selected[handle]) {
					if (!apply && walk_process_list(outside_references_locked, job, position->duplicate) != (position->outside ? 1 : 0)) goto done;
					if (!position->outside) {
						if (apply) {
							/* The retired exec image still contains its FD slots. Removing only
							 * their OFD locks has the same effect until the exit stub closes them. */
							if (!(position->flags & O_PATH) && (set_file_lock(position->duplicate, (struct ofd_lock){.type = F_UNLCK}) < 0 ||
								set_file_lock(position->duplicate, (struct ofd_lock){.kind = 1, .type = F_UNLCK}) < 0)) goto done;
						} else {
							int released = lock_fds[handle]; if (released < 0) goto done;
							close(released);
							for (unsigned other = 0; other < job->position_count; other++) if (lock_fds[other] == released) lock_fds[other] = -1;
						}
					}
				}
			}
			continue;
		}
		if (event->kind == 13) goto done;
		if (message && (position->stream < 4 || position->event)) goto done;
		int object = queues[handle].owner, peer = position->stream < 4 ? object : -1;
		int packet = position->socket_type && position->socket_type != SOCK_STREAM;
		struct control_message *packet_head = NULL;
		if (packet) for (unsigned cursor = 0; cursor < message_count; cursor++)
			if (messages[cursor].object == object) { packet_head = &messages[cursor]; break; }
		if (position->peer_inode) for (unsigned other = 0; other < job->position_count; other++)
			if (job->positions[other].stream >= 4 && job->positions[other].inode == position->peer_inode) { peer = queues[other].owner; break; }
		if (position->event && (event->kind < 3 || event->kind == 7)) {
			uint64_t value = 0, limit = UINT64_MAX - 1;
			if (event->kind == 7) {
				if (event->length != 4 && event->length != 12) goto done;
				int writing = event->data[0], error = event->data[2] | (int)event->data[3] << 8;
				if (writing > 1 || event->data[1] || (writing && event->requested >= 8 && event->length != 12)) goto done;
				if (event->length == 12) memcpy(&value, event->data + 4, 8);
				int invalid = event->requested < 8 || (writing && value == UINT64_MAX);
				int blocked = (queues[handle].flags & O_NONBLOCK) && (writing ? value > limit - queues[object].counter : !queues[object].counter);
				if (!(error == EINVAL ? invalid : error == EAGAIN && !invalid && blocked)) goto done;
			} else {
				if (event->kind == 1 || event->length != 8 || event->requested < 8) goto done;
				memcpy(&value, event->data, 8);
				if (event->kind == 0) {
					if (!queues[object].counter || value != (queues[object].semaphore ? 1 : queues[object].counter)) goto done;
					queues[object].counter -= value;
					if (apply) { uint64_t actual; if (read(position->duplicate, &actual, 8) != 8 || actual != value) goto done; }
				} else {
					if (value > limit - queues[object].counter) goto done;
					queues[object].counter += value;
					if (apply && write(position->duplicate, &value, 8) != 8) goto done;
				}
			}
			continue;
		}
		if (event->kind < 2) {
			if ((position->flags & O_ACCMODE) == O_WRONLY || event->length > queues[object].length ||
				(event->length ? memcmp(event->data, queues[object].bytes, event->length) : !packet_head && (!queues[object].eof || queues[object].error))) goto done;
			struct control_message *control = packet_head;
			for (unsigned cursor = 0; !packet && cursor < message_count; cursor++) if (messages[cursor].object == object &&
				messages[cursor].start < queues[object].consumed + event->length) { control = &messages[cursor]; break; }
			if (control && queues[object].consumed + event->length > control->end) goto done;
			size_t consumed = packet && control ? control->end - control->start : event->length;
			if (packet && control && (control->start != queues[object].consumed || event->length !=
				(event->requested < consumed ? event->requested : consumed))) goto done;
			if (message && (right_count != (control ? control->count : 0) ||
				(right_count && memcmp(rights, control->rights, right_count * sizeof(*rights))))) goto done;
			if (event->kind == 0) {
				if (apply && (event->length || packet_head)) {
					if (message) { if (transfer_message(job, position->duplicate, event, rights, right_count) < 0) goto done; }
					else if (packet) {
						unsigned char *bytes = malloc(event->length + 1); if (!bytes) goto done;
						int valid = recv(position->duplicate, bytes, event->length, MSG_DONTWAIT) == (ssize_t)event->length && !memcmp(bytes, event->data, event->length);
						free(bytes); if (!valid) goto done;
					}
					else {
					unsigned char bytes[65536]; size_t consumed = 0;
					while (consumed < event->length) {
						size_t length = event->length - consumed; if (length > sizeof(bytes)) length = sizeof(bytes);
						if (transfer(position->duplicate, bytes, length, 0) < 0 || memcmp(bytes, event->data + consumed, length)) goto done;
						consumed += length;
					}
					}
				}
				if (control) control->object = -1;
				queues[object].length -= consumed;
				queues[object].consumed += consumed;
				memmove(queues[object].bytes, queues[object].bytes + consumed, queues[object].length);
			}
		} else if (event->kind == 2) {
			if (source >= 0) {
				int input = queues[source].owner;
				if (position->stream >= 4 || job->positions[source].stream <= 0 || job->positions[source].stream >= 4 || input == object ||
					(job->positions[source].flags & O_ACCMODE) == O_WRONLY || event->length != event->requested || !event->length ||
					event->length > queues[input].length || memcmp(event->data, queues[input].bytes, event->length) ||
					/* A byte can occupy its own pipe slot. Bound slots before touching either endpoint. */
					(queues[object].length + event->length) * 4096UL > (unsigned)position->capacity) goto done;
				if (moving) {
					queues[input].length -= event->length; queues[input].consumed += event->length;
					memmove(queues[input].bytes, queues[input].bytes + event->length, queues[input].length);
				}
			}
			if (event->requested != event->length && !(position->stream < 4 && (queues[handle].flags & O_NONBLOCK) &&
				event->requested > 4096 && event->length == (unsigned)position->capacity && !queues[object].length)) goto done;
			if ((position->flags & O_ACCMODE) == O_RDONLY || (queues[object].shutdown & 2) ||
				(position->stream < 4 && !queues[object].writable && queues[object].consumed < 4096) ||
				(position->stream >= 4 && (peer < 0 ? position->peer_shutdown & 1 : queues[peer].shutdown & 1)) ||
				(queues[object].produced += event->length) > 4096 || ++queues[object].writes > 16 ||
				(position->stream >= 4 && (uint64_t)position->allocated + queues[object].writes * 8192UL > (unsigned)position->capacity)) goto done;
			if (peer >= 0) {
				if (queues[peer].length + event->length > (size_t)job->positions[peer].content_length + 4096 ||
					(position->stream < 4 && queues[peer].length + event->length > (unsigned)position->capacity)) goto done;
			}
			if (right_count || packet) {
				if (!event->length && !packet) goto done;
				struct control_message *control = &messages[message_count++];
				/* -2 is a queued reference held by a real peer outside the exec image. */
				control->object = peer >= 0 ? peer : -2; control->start = peer >= 0 ? queues[peer].consumed + queues[peer].length : 0;
				control->end = control->start + event->length; control->count = right_count; memcpy(control->rights, rights, right_count * sizeof(*rights));
			}
			if (peer >= 0) {
				memcpy(queues[peer].bytes + queues[peer].length, event->data, event->length); queues[peer].length += event->length;
			}
			if (apply && (source >= 0 ? (moving ? splice(job->positions[source].duplicate, NULL, position->duplicate, NULL, event->length, SPLICE_F_NONBLOCK) :
				tee(job->positions[source].duplicate, position->duplicate, event->length, SPLICE_F_NONBLOCK)) != (ssize_t)event->length :
				message ? transfer_message(job, position->duplicate, event, rights, right_count) < 0 :
				(position->stream >= 4 ? send(position->duplicate, event->data, event->length, MSG_DONTWAIT | MSG_NOSIGNAL)
				: write(position->duplicate, event->data, event->length)) != (ssize_t)event->length)) goto done;
		} else if (event->kind == 4) {
			if (position->event ? walk_process_list(outside_references_locked, job, position->duplicate) != 0 :
				(walk_process_list(outside_references_locked, job, position->duplicate) & event->data[0])) goto done;
			/* The held image exits after commit; replay closes are logical until then.
			 * Snapshot pins must not turn these last-reference observations into live peers. */
			if (position->event) continue;
			if (position->stream >= 4) {
				queues[object].shutdown = 3;
				if (peer >= 0 && position->socket_type != SOCK_DGRAM) {
					queues[peer].eof = 1; queues[peer].shutdown = 3;
					if (queues[object].length || packet_head) queues[peer].error = ECONNRESET;
				}
				for (unsigned cursor = 0; cursor < message_count; cursor++) if (messages[cursor].object == object) messages[cursor].object = -1;
				queues[object].length = 0;
			} else {
				if (event->data[0] & 2) queues[object].eof = 1;
				if (event->data[0] & 1) queues[object].shutdown |= 2;
			}
		} else if (event->kind == 5) {
			uint32_t request[3];
			if (event->length != sizeof(request)) goto done;
			memcpy(request, event->data, sizeof(request));
			unsigned ready = position->event ? (queues[object].counter ? POLLIN : 0) | (queues[object].counter < UINT64_MAX - 1 ? POLLOUT : 0)
				: queues[object].length || packet_head ? POLLIN | POLLRDNORM : 0;
			if (position->event) { /* Counter readiness is exact, independent of queue capacity. */ }
			else if (position->stream >= 4) {
				if (queues[object].shutdown & 1) ready |= POLLIN | POLLRDNORM | POLLRDHUP;
				if (queues[object].shutdown == 3 || (position->stream == 5 && position->socket_type != SOCK_DGRAM)) ready |= POLLHUP;
				if (queues[object].error) ready |= POLLERR;
			} else if ((position->flags & O_ACCMODE) == O_RDONLY && queues[object].eof) ready |= POLLHUP;
			else if ((position->flags & O_ACCMODE) == O_WRONLY && (queues[object].shutdown & 2)) ready |= POLLERR;
			if (!position->event && (position->flags & O_ACCMODE) != O_RDONLY && (request[0] & (POLLOUT | POLLWRNORM | POLLWRBAND))) {
				int writable;
				if (position->stream >= 4) {
					uint64_t bound = (uint64_t)position->allocated + queues[object].writes * 8192UL;
					if (bound * 4 <= (unsigned)position->capacity || (peer >= 0 && !queues[peer].length)) writable = 1;
					else if (!queues[object].writes && (peer < 0 || !queues[peer].consumed)) writable = 0;
					else goto done;
				} else if (!position->content_length) writable = (queues[object].length ? 4096 : 0) < (unsigned)position->capacity;
				else if (!queues[object].length) writable = 1;
				else if (!queues[object].produced && !queues[object].consumed) writable = queues[object].writable;
				else goto done;
				if (writable) ready |= POLLOUT | POLLWRNORM | (position->stream >= 4 ? POLLWRBAND : 0);
			}
			if (request[2] == 1) {
				unsigned selected = (ready & (POLLIN | POLLRDNORM | POLLHUP | POLLERR) ? POLLIN : 0) |
					(ready & (POLLOUT | POLLWRNORM | POLLERR) ? POLLOUT : 0) | (ready & POLLPRI);
				if ((request[0] & ~(POLLIN | POLLOUT | POLLPRI)) || (selected & request[0]) != request[1]) goto done;
			} else if (request[2] || (ready & (request[0] | POLLERR | POLLHUP | POLLNVAL)) != request[1]) goto done;
		} else if (event->kind == 6) {
			uint32_t flags;
			if (event->length != sizeof(flags)) goto done;
			memcpy(&flags, event->data, sizeof(flags));
			if (flags & ~(O_ACCMODE | O_APPEND | O_NONBLOCK | 0x8000)) goto done;
			for (unsigned other = 0; other < job->position_count; other++) if (queues[other].description == queues[handle].description)
				queues[other].flags = (queues[other].flags & ~(O_APPEND | O_NONBLOCK)) | (flags & (O_APPEND | O_NONBLOCK));
			/* Queued rights can outlive all original handles and their final report. */
			if (apply && fcntl(position->duplicate, F_SETFL, queues[handle].flags) < 0) goto done;
		} else if (event->kind == 7) {
			if (event->length != 4 || event->data[0] > 1 || event->data[1] > 1) goto done;
			int writing = event->data[0], error = event->data[2] | (int)event->data[3] << 8;
			int bad_access = (position->flags & O_ACCMODE) == (writing ? O_RDONLY : O_WRONLY);
			int broken = writing && ((queues[object].shutdown & 2) ||
				(position->stream >= 4 && (peer >= 0 ? queues[peer].shutdown & 1 : position->peer_shutdown & 1)));
			int blocked = (event->requested || packet) && (event->data[1] || (queues[handle].flags & O_NONBLOCK)) && (writing ?
				position->stream < 4 && !broken && (queues[object].length == (unsigned)position->capacity ||
					(event->requested <= 4096 && event->requested > (unsigned)position->capacity - queues[object].length)) :
				!queues[object].length && !packet_head && !queues[object].eof && !queues[object].error);
			if (!(error == EBADF ? bad_access : !bad_access && (error == EPIPE ? broken : error == EAGAIN && blocked))) goto done;
		} else {
			if (position->event || position->stream < 4 || event->length != 1 || !event->data[0] || event->data[0] > 3) goto done;
			queues[object].shutdown |= event->data[0];
			if ((event->data[0] & 1) && position->socket_type != SOCK_DGRAM) queues[object].eof = 1;
			if (peer >= 0 && position->socket_type != SOCK_DGRAM) {
				if (event->data[0] & 2) { queues[peer].eof = 1; queues[peer].shutdown |= 1; }
				if (event->data[0] & 1) queues[peer].shutdown |= 2;
			}
			if (apply && shutdown(position->duplicate, event->data[0] == 3 ? SHUT_RDWR : event->data[0] == 2 ? SHUT_WR : SHUT_RD) < 0) goto done;
		}
	}
	result = 0;
done:
	free(messages);
	for (unsigned index = 0; index < job->position_count; index++) free(queues[index].bytes);
	for (unsigned index = 0; index < job->position_count; index++) if (!job->positions[index].alias) close(lock_fds[index]);
	if (locks) pthread_setcancelstate(cancellation, NULL);
	return result;
}

/* A nonnegative return keeps the connection until the continued tracee exits. */
static int image_context(struct decision_job *job) {
	int current[MAX_HANDLES], count = descriptor_numbers(job->pid, 0, current, MAX_HANDLES);
	unsigned long call = job->image->header.registers.rax, descriptor = job->image->header.registers.rdi;
	int valid = count >= 0 && job->process->table &&
		job->process->table->references == 1 && !job->process->table->active;
	int input = pi_process_image_frontier(call) == 2 || ((descriptor == 1 || descriptor == 2) &&
		(call == SYS_write || call == SYS_writev || call == SYS_sendto || call == SYS_sendmsg));
	for (int i = 0; valid && i < count; i++) {
		if (current[i] < 3) continue;
		const struct file_position *position = resource_position(job, current[i]);
		valid = position && position->installed;
	}
	for (unsigned i = 0; valid && i < job->image->fd_count; i++) {
		int source = job->image->fds[i].source, fd = job->image->fds[i].fd;
		for (unsigned previous = 0; previous < i; previous++) if (job->image->fds[previous].fd == fd) valid = 0;
		const struct file_position *position = resource_position(job, source);
		if (position) {
			valid &= (job->image->fds[i].flags & ~O_CLOEXEC) == position->after_flags &&
				job->image->fds[i].offset == (position->stream ? 0 : position->after);
			if ((unsigned long)fd == descriptor) input = 1;
		} else valid &= fd < 3 && source == fd && job->image->fds[i].flags == descriptor_info(job->pid, fd, NULL);
	}
	return valid && input && image_status(job->pid, 1) == 0 && image_qualify(job) == 0 ? 0 : -1;
}

static int image_descriptor_table(struct decision_job *job) {
	struct descriptor_table *table = job->process->table;
	struct descriptor_origin final[MAX_HANDLES] = {0}; unsigned count = 0;
	for (unsigned i = 0; i < job->image->fd_count; i++) {
		int source = job->image->fds[i].source;
		final[i] = descriptor_origin(job->process, source);
		const struct file_position *position = resource_position(job, source);
		if (position && !position->installed) {
			struct resource_object *object = pin_object(job->domain, position->duplicate);
			if (!object || !object->internal || object->foreign) return -1;
			final[i] = (struct descriptor_origin){.id = ++job->domain->next,
				.object = (unsigned)(object - job->domain->objects) + 1, .access = (position->after_flags & O_ACCMODE) + 1};
			for (unsigned previous = 0; previous < i; previous++) if (job->image->fds[previous].source == source) final[i].id = final[previous].id;
		}
		final[i].fd = job->image->fds[i].fd; final[i].cloexec = !!(job->image->fds[i].flags & O_CLOEXEC);
	}
	for (unsigned i = 0; i < job->image->fd_count; i++) if (final[i].id) table->entries[count++] = final[i];
	table->count = count;
	job->process->call_epoch = ++table->epoch; table->generation++;
	return 0;
}

static void release_position_fds(struct decision_job *job) {
	int cancellation;
	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &cancellation);
	discard_captures(job);
	for (unsigned index = 0; job->positions && index < job->position_count; index++) {
		close(job->positions[index].duplicate); close(job->positions[index].writer);
		job->positions[index].duplicate = job->positions[index].writer = -1;
	}
	pthread_setcancelstate(cancellation, NULL);
}

/* The host may read queue-only pins without access to the protected observer.
 * These extra references end before any ownership or adoption validation. */
static void stop_pin_holder(struct decision_job *job) {
	int cancellation, status;
	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &cancellation);
	if (job->holder > 0) {
		kill(job->holder, SIGKILL);
		while (waitpid(job->holder, &status, __WALL) < 0 && errno == EINTR) {}
	}
	job->holder = 0;
	close(job->holder_control); job->holder_control = -1;
	pthread_setcancelstate(cancellation, NULL);
}

static int start_pin_holder(struct decision_job *job) {
	int keep[MAX_POSITIONS + 1], channel[2], count = 0;
	for (unsigned index = 0; index < job->capture_count; index++)
		if (!job->captures[index].installed) keep[count++] = job->captures[index].duplicate;
	if (!count) return 0;
	if (socketpair(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0, channel) < 0) return -1;
	keep[count++] = channel[1];
	for (int index = 1; index < count; index++) {
		int value = keep[index], previous = index;
		while (previous && keep[previous - 1] > value) { keep[previous] = keep[previous - 1]; previous--; }
		keep[previous] = value;
	}
	char control[24], parent[24];
	snprintf(control, sizeof(control), "%d", channel[1]); snprintf(parent, sizeof(parent), "%d", getpid());
	char *arguments[] = {"pi-held-pins", "--hold-pins", control, parent, NULL}, *environment[] = {NULL};
	job->holder_control = channel[0];
	job->holder = fork();
	if (!job->holder) {
		unsigned next = 0;
		for (int index = 0; index < count; index++) {
			if (next < (unsigned)keep[index] && syscall(SYS_close_range, next, (unsigned)keep[index] - 1, 0) < 0) _exit(70);
			if (fcntl(keep[index], F_SETFD, 0) < 0) _exit(70);
			next = (unsigned)keep[index] + 1;
		}
		if (syscall(SYS_close_range, next, UINT_MAX, 0) < 0 || prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) < 0) _exit(70);
		execve("/proc/self/exe", arguments, environment); _exit(70);
	}
	close(channel[1]);
	struct pollfd ready = {.fd = channel[0], .events = POLLIN}; char byte;
	if (job->holder <= 0 || poll(&ready, 1, 5000) <= 0 || read(channel[0], &byte, 1) != 1 || byte != 'R') {
		stop_pin_holder(job); return -1;
	}
	return 0;
}

static int actor_decision(struct decision_job *job) {
	unsigned code = 125;
	size_t total = 0, image_bytes = 0;
	unsigned physical_length = 0, source_length = 0;
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
		/* Publish every snapshot pin under job ownership before allowing cancellation. */
		pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
		captured = descriptor_context(job, descriptors, MAX_REQUEST_BYTES) == 0 && start_pin_holder(job) == 0;
		pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
	}
	int request_length = snprintf(line, sizeof(line),
		"{\"token\":\"%s\",\"execution\":\"%s\",\"pid\":%ld,\"tracer\":%ld,\"holder\":%ld%s",
		job->token, job->execution_id, (long)job->pid, (long)getpid(), (long)job->holder,
		captured ? ",\"descriptors\":[" : job->needs_tracking ? ",\"trackQueues\":true" : "");
	int sent = request_length > 0 && request_length < (int)sizeof(line) && transfer(connection, line, (size_t)request_length, 1) == 0 &&
		(!captured || transfer(connection, descriptors, strlen(descriptors), 1) == 0) && transfer(connection, captured ? "]}\n" : "}\n", captured ? 3 : 2, 1) == 0;
	free(descriptors); job->context = NULL;
	int received_line = sent ? read_line(connection, line, sizeof(line)) : -1;
	stop_pin_holder(job);
	if (received_line < 0) return -1;
	if (!strcmp(line, "C")) return -1;
	if (!strcmp(line, "F")) return -2;
	if (!strcmp(line, "O")) return connection;
	if (sscanf(line, "P %u %u %zu %u %u %zu %u %u", &code, &job->count, &total, &job->position_count, &job->resource_count,
		&image_bytes, &physical_length, &source_length) != 8 || code > 256 || (code == 256) != (image_bytes != 0) ||
		image_bytes > IMAGE_BYTES + sizeof(struct image_header) + MAX_LINE || physical_length >= PATH_MAX || source_length >= PATH_MAX ||
		(!image_bytes && (physical_length || source_length)) ||
		job->count > MAX_OUTPUT_EVENTS || total > MAX_OUTPUT_BYTES || job->position_count > MAX_POSITIONS || job->resource_count > 1024) goto decline;
	if (job->position_count) {
		job->positions = calloc(job->position_count, sizeof(*job->positions));
		if (!job->positions) goto decline;
		for (unsigned index = 0; index < job->position_count; index++) {
			job->positions[index].duplicate = -1; job->positions[index].writer = -1;
		}
	}
	size_t received = 0;
	for (unsigned index = 0; index < job->position_count; index++) {
		struct file_position *position = &job->positions[index];
		unsigned path_length;
		if (read_line(connection, line, sizeof(line)) < 0 ||
			sscanf(line, "S %d %ju %ju %d %" SCNd64 " %" SCNd64 " %" SCNd64 " %d %u %d %d %d %d %u %d %d %u %u %u", &position->descriptor,
				&position->device, &position->inode, &position->flags, &position->before, &position->after, &position->content_length, &position->after_flags, &path_length, &position->eof,
				&position->capacity, &position->shutdown, &position->peer_shutdown, &position->peer_inode, &position->peer_queued, &position->allocated, &position->event, &position->messages.count, &position->socket_type) != 19 || position->messages.count > MAX_POSITIONS || position->eof < -1 || position->eof > 1 ||
			(position->socket_type && position->socket_type != SOCK_STREAM && position->socket_type != SOCK_DGRAM && position->socket_type != SOCK_SEQPACKET) ||
			position->capacity < 0 || position->shutdown < 0 || position->shutdown > 3 || position->peer_shutdown < 0 || position->peer_shutdown > 3 ||
			position->peer_queued < 0 || position->allocated < 0 ||
			position->descriptor < 0 || position->before < 0 || position->after < 0 || position->content_length < -1 ||
			path_length >= PATH_MAX || path_length > total - received || position->after_flags < 0 || ((position->flags ^ position->after_flags) & ~(O_APPEND | O_NONBLOCK)) ||
			(position->content_length >= 0 && (uint64_t)position->content_length > total - received - path_length)) goto decline;
		unsigned messages = position->messages.count; position->messages.count = 0;
		for (unsigned index = 0; index < messages; index++) {
			struct queue_message message = {0}; int descriptor;
			if (read_line(connection, line, sizeof(line)) < 0 || parse_queue_message(line, &descriptor, &message) < 0 || descriptor != position->descriptor ||
				message.end > (uint64_t)position->content_length || append_message(&position->messages, &message) < 0) goto decline;
		}
		if (path_length) {
			position->path = calloc((size_t)path_length + 1, 1);
			if (!position->path || transfer(connection, position->path, path_length, 0) < 0) goto decline;
			if (*position->path != '/' || strlen(position->path) != path_length) goto decline;
			received += path_length;
		}
		if (position->content_length > 0) {
			position->content = malloc((size_t)position->content_length);
			if (!position->content || transfer(connection, position->content, (size_t)position->content_length, 0) < 0) goto decline;
		}
		if (position->content_length >= 0) received += (size_t)position->content_length;
	}
	job->events = calloc(job->count + job->resource_count + 1, sizeof(*job->events));
	if (!job->events) goto decline;
	for (unsigned index = 0; index < job->resource_count + job->count; index++) {
		size_t length, requested = 0;
		unsigned fd, kind = 0;
		if (read_line(connection, line, sizeof(line)) < 0 || (index < job->resource_count
			? sscanf(line, "Q %u %u %zu %zu", &kind, &fd, &length, &requested) != 4 || kind > 13 || length > MAX_INPUT_BYTES || requested > MAX_INPUT_BYTES || (kind == 2 && requested < length)
			: sscanf(line, "O %u %zu", &fd, &length) != 2 || (fd != 1 && fd != 2)) || length > total - received) goto decline;
		struct output_event *event = &job->events[index];
		event->fd = fd; event->kind = kind;
		event->length = length; event->requested = requested;
		if (index >= job->resource_count && job->outputs[fd] < 0) {
			/* Publish acquired descriptors before a cancellation point can retire the job. */
			pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
			job->outputs[fd] = open_tracee_output(job, fd);
			pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
			if (job->outputs[fd] < 0) goto decline;
		}
		if (length && (!(event->data = malloc(length)) || transfer(connection, event->data, length, 0) < 0)) goto decline;
		received += length;
	}
	if (image_bytes) {
		char physical_root[PATH_MAX] = {0}, source_root[PATH_MAX] = {0};
		job->image = image_new();
		if (!job->image || !(job->image->allocation = malloc(image_bytes)) ||
			transfer(connection, physical_root, physical_length, 0) < 0 || transfer(connection, source_root, source_length, 0) < 0 ||
			transfer(connection, job->image->allocation, image_bytes, 0) < 0) goto decline;
		if (*physical_root != '/' || *source_root != '/' || strlen(physical_root) != physical_length || strlen(source_root) != source_length ||
			image_load(job, image_bytes, physical_root, source_root) < 0) goto decline;
		received += image_bytes + physical_length + source_length;
	}
	if (received != total) goto decline;
	for (unsigned index = 0; index < job->position_count; index++) {
		struct file_position *position = &job->positions[index];
		pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
		position->installed = 1;
		for (unsigned capture = 0; capture < job->capture_count; capture++) if (job->captures[capture].descriptor == position->descriptor) {
			position->duplicate = job->captures[capture].duplicate; job->captures[capture].duplicate = -1;
			position->locks = job->captures[capture].locks; job->captures[capture].locks = (struct file_locks){0};
			position->outside = job->captures[capture].outside;
			position->installed = job->captures[capture].installed; break;
		}
		if (!job->capture_count) position->duplicate = duplicate_tracee_fd(job, (unsigned)position->descriptor);
		pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
	}
	/* Replay may represent fewer queues than capture; prove external owners again. */
	clear_references(job);
	/* Capturing is not ownership: unused pins cannot prolong last-close lifetime. */
	for (unsigned index = 0; index < job->capture_count; index++) { close(job->captures[index].duplicate); job->captures[index].duplicate = -1; }
	for (unsigned index = 0; index < job->position_count; index++) {
		struct file_position *position = &job->positions[index];
		struct stat state;
		if (position->duplicate >= 0 && fstat(position->duplicate, &state) == 0) position->directory = S_ISDIR(state.st_mode);
		if (position->event) position->stream = 6;
		if (position->duplicate >= 0 && fstat(position->duplicate, &state) == 0 && (S_ISFIFO(state.st_mode) || S_ISSOCK(state.st_mode))) {
			struct file_position current = {0};
			if (stream_state(position->duplicate, &current) < 0) goto decline;
			position->stream = current.stream;
		}
		if (position->stream && position->eof < 0) position->eof = 1;
		struct resource_object *object = pin_object(job->domain, position->duplicate);
		if (position->stream && (!object || (position->event ? object->event != position->event : position->stream >= 4 ? !object->channel : !object->pipe))) goto decline;
		if (position->duplicate < 0 || !position_matches(position, job)) goto decline;
		if (job->domain && (job->domain->escaped || job->domain->uncertain || !job->process->table ||
			job->process->table->active || (position->installed ? !descriptor_origin(job->process, position->descriptor).id : !owned_pin(job->domain, position->duplicate)))) goto decline;
		for (unsigned previous = 0; previous < index; previous++) {
			const struct file_position *other = &job->positions[previous];
			if (!position->stream && !position->directory && position->content_length >= 0 && other->content_length >= 0 &&
				position->device == other->device && position->inode == other->inode) goto decline;
			if (position->stream && same_object(position, other)) {
				if (!other->stream || position->after != other->after) goto decline;
				position->queue_alias = 1;
			}
			long same = syscall(SYS_kcmp, getpid(), getpid(), KCMP_FILE, position->duplicate, other->duplicate);
			if (same < 0) goto decline;
			if (same == 0) {
				if (position->before != other->before || position->after != other->after || position->after_flags != other->after_flags) goto decline;
				position->alias = (int)previous + 1;
			}
		}
		if (!position->stream && !position->directory && position->content_length >= 0) {
			struct stat state;
			pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
			position->writer = reopen_descriptor(position->duplicate, O_WRONLY | O_CLOEXEC);
			pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
			if (position->writer < 0 || fstat(position->writer, &state) < 0 ||
				(uintmax_t)state.st_dev != position->device || (uintmax_t)state.st_ino != position->inode) goto decline;
		}
	}
	if (resource_events(job, 0) < 0) goto decline;
	if (job->image && image_context(job) < 0) goto decline;
	/* From the first text mutation onward, failure terminates the entire trace tree. */
	if (request_tracer(job, 125) < 0) return -2;
	if (transfer(connection, "A\n", 2, 1) < 0 || read_line(connection, line, sizeof(line)) < 0 || strcmp(line, "R")) return -2;
	for (unsigned index = 0; index < job->position_count; index++)
		if (!position_matches(&job->positions[index], job)) return -2;
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
	if (resource_events(job, 1) < 0) return -2;
	clear_references(job);
	if (job->image) {
		if (request_tracer(job, 257) < 0) return -2;
		/* The restored FD table and queued messages now own all remaining references.
		 * Release temporary pins before output can block on a peer waiting for EOF. */
		release_position_fds(job);
	}
	/* Output may feed another tracee. Release the offset lease before a pipe write can block. */
	if (job->domain && job->domain->enabled && request_tracer(job, 256) < 0) return -2;
	for (unsigned index = job->resource_count; index < job->resource_count + job->count; index++) {
		struct output_event *event = &job->events[index];
		if (transfer(job->outputs[event->fd], event->data, event->length, 1) < 0) return -2;
	}
	if ((!job->image && request_tracer(job, code) < 0) || transfer(connection, "D\n", 2, 1) < 0) return -2;
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
	stop_pin_holder(job);
	image_free(job->image);
	release_position_fds(job);
	close(job->channel[0]); close(job->channel[1]); close(job->connection); close(job->pidfd);
	for (unsigned fd = 1; fd <= 2; fd++) close(job->outputs[fd]);
	if (job->positions) for (unsigned index = 0; index < job->position_count; index++) {
		free(job->positions[index].content); free(job->positions[index].path); free(job->positions[index].messages.entries); free(job->positions[index].locks.entries);
	}
	free(job->positions);
	clear_references(job);
	free(job->context);
	free_events(job->events, job->count + job->resource_count);
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
		.connection = -1, .pidfd = -1, .holder_control = -1, .outputs = {-1, -1, -1}, .result = -1, .domain = domain, .process = process };
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
	pthread_mutex_lock(&process_list_lock); *processes = observer; pthread_mutex_unlock(&process_list_lock);
	return 0;
}

static void finish_observation(struct traced_process *observer, int completed) {
	(void)transfer(observer->fd, completed ? "D\n" : "C\n", 2, 1);
	close(observer->fd); observer->fd = -1;
}

static void release_process(struct traced_process **processes, pid_t pid, struct descriptor_domain *domain, int completed) {
	struct traced_process **cursor = processes;
	while (*cursor) {
		struct traced_process *observer = *cursor;
		if (observer->pid != pid) { cursor = &observer->next; continue; }
		pthread_mutex_lock(&process_list_lock); *cursor = observer->next; pthread_mutex_unlock(&process_list_lock);
		if (observer->job) {
			pthread_cancel(observer->job->thread);
			pthread_join(observer->job->thread, NULL);
			free_job(observer->job);
		}
		finish_observation(observer, completed);
		pthread_mutex_lock(&process_list_lock); drop_descriptor_table(observer, domain); pthread_mutex_unlock(&process_list_lock);
		free(observer);
	}
}

/* No decision remains. Detach ready tracees; blocked vfork parents stay protected until VFORK_DONE. */
static int release_tracees(struct traced_process **processes, struct descriptor_domain *domain, int *acknowledged) {
	int live = 0, waiting = 0;
	for (struct traced_process *item = *processes; item;) {
		if (!item->historical) {
			if (item->stopped && !item->listening && !item->awaiting_parent) {
				if (ptrace(PTRACE_DETACH, item->pid, 0, item->delivered) < 0 && errno != ESRCH) return -1;
				release_process(processes, item->pid, domain, 0); item = *processes; live = waiting = 0; continue;
			}
			live = 1;
			if (!item->vforking) {
				waiting = 1;
				if (ptrace(PTRACE_INTERRUPT, item->pid, 0, 0) < 0 && errno != ESRCH && errno != EIO) return -1;
			}
		}
		item = item->next;
	}
	if (!waiting && !*acknowledged) {
		for (struct traced_process *item = *processes; item; item = item->next) finish_observation(item, 0);
		(void)transfer(STDOUT_FILENO, "R", 1, 1); close(STDOUT_FILENO); *acknowledged = 1;
	}
	return live;
}

/* An ordinary caller cannot lend either saved IDs or capabilities to a reusable image. */
static int process_owned(pid_t pid, struct process_credentials *value) {
	if (process_credentials(pid, value) < 0 || value->permitted || value->effective || value->inheritable || value->ambient) return 0;
	for (unsigned i = 0; i < 4; i++) if (value->uid[i] != getuid() || value->gid[i] != getgid()) return 0;
	return 1;
}

static int attach_owned(pid_t root, long flags) {
	struct ucred peer; socklen_t length = sizeof(peer);
	struct process_credentials caller, child, observer;
	if (getsockopt(3, SOL_SOCKET, SO_PEERCRED, &peer, &length) < 0 || length != sizeof(peer) ||
		peer.pid != getppid() || peer.uid != getuid() || peer.gid != getgid() ||
		!process_owned(peer.pid, &caller) || !process_owned(root, &child) || child.parent != (unsigned)peer.pid ||
		process_credentials(getpid(), &observer) < 0) return -1;
	unsigned long long extra = 1ULL << CAP_SYS_PTRACE;
	if (!child.no_new_privileges && !(observer.effective & extra)) return -1;
	if (observer.permitted & ~extra || observer.effective & ~extra || observer.inheritable || observer.ambient) return -1;
	int parent = syscall(SYS_pidfd_open, peer.pid, 0), target = syscall(SYS_pidfd_open, root, 0), result = -1;
	struct pollfd alive[2] = {{.fd = parent, .events = POLLIN}, {.fd = target, .events = POLLIN}};
	if (parent >= 0 && target >= 0 && poll(alive, 2, 0) == 0 &&
		ptrace(PTRACE_SEIZE, root, 0, flags & ~PTRACE_O_EXITKILL) == 0) {
		struct __user_cap_header_struct header = {_LINUX_CAPABILITY_VERSION_3, 0};
		struct __user_cap_data_struct empty[2] = {{0}, {0}};
		if (syscall(SYS_capset, &header, empty) == 0 && poll(alive, 2, 0) == 0 && ptrace(PTRACE_INTERRUPT, root, 0, 0) == 0) result = 0;
	}
	close(parent); close(target); return result;
}

static int hold_pins(int argc, char **argv) {
	struct process_credentials credentials;
	if (argc != 4 || !*argv[2] || !*argv[3] || strspn(argv[2], "0123456789") != strlen(argv[2]) ||
		strspn(argv[3], "0123456789") != strlen(argv[3]) || strtoul(argv[2], NULL, 10) < 3 || strtoul(argv[2], NULL, 10) >= INT_MAX ||
		strtoul(argv[3], NULL, 10) > INT_MAX || !process_owned(getpid(), &credentials) || !credentials.no_new_privileges ||
		prctl(PR_SET_PDEATHSIG, SIGKILL) < 0 || getppid() != (pid_t)strtol(argv[3], NULL, 10) ||
		prctl(PR_SET_DUMPABLE, 1, 0, 0, 0) < 0) return 64;
	int control = atoi(argv[2]); char byte;
	/* Secure libc startup may fill vacant stdio even though no_new_privs suppressed file capabilities. */
	close(0); close(1); close(2);
	if (write(control, "R", 1) != 1) return 70;
	while (read(control, &byte, 1) < 0 && errno == EINTR) {}
	return 0;
}

static int trace(const char *socket_path, const char *token, const char *execution_id,
	int skip, unsigned skip_code, int descriptors, int skip_root) {
	struct descriptor_domain domain = {.enabled = descriptors == 1};
	pid_t barrier = 0;
	sigset_t blocked, original;
	sigemptyset(&blocked); sigaddset(&blocked, SIGCHLD); sigaddset(&blocked, SIGPIPE);
	if (pthread_sigmask(SIG_BLOCK, &blocked, &original) != 0) return 70;
	int signals = signalfd(-1, &blocked, SFD_CLOEXEC | SFD_NONBLOCK);
	if (signals < 0) return 70;
	pid_t root;
	if (transfer(3, &root, sizeof(root), 0) < 0 || root <= 0) return 71;
	long flags = options | (descriptors ? PTRACE_O_TRACESYSGOOD : 0);
	if (attach_owned(root, flags) < 0) return 72;
	int status = 0, root_status = -1, retiring = 0, draining = 0, acknowledged = 0, admitted = 0;
	unsigned exec_events = !skip_root;
	struct traced_process *processes = NULL;
	domain.processes = &processes;
	struct pollfd *polling = NULL;
	size_t capacity = 0;
	if (track_process(&processes, root) < 0) { kill(root, SIGKILL); goto fatal; }
	for (;;) {
		struct pollfd control = {.fd = STDIN_FILENO, .events = POLLIN};
		if (!retiring && poll(&control, 1, 0) > 0) retiring = 1;
		struct signalfd_siginfo notification;
		while (read(signals, &notification, sizeof(notification)) > 0) {}
		for (struct traced_process *item = processes; item; item = item->next) {
			struct decision_job *job = item->job;
			if (!job) continue;
			unsigned code;
			ssize_t received = recv(job->channel[0], &code, sizeof(code), MSG_DONTWAIT);
			if (received == sizeof(code)) {
				char reply;
				if (code == 257 && job->image && item->armed) {
					int stopped;
					reply = ptrace(PTRACE_SYSCALL, item->pid, 0, 0) == 0 && image_wait(item->pid, &stopped) == 0 &&
						WSTOPSIG(stopped) == (SIGTRAP | 0x80) && image_restore(item->pid, job) == 0 ? 'Y' : 'N';
					if (reply == 'Y' && image_descriptor_table(job) < 0) reply = 'N';
				} else if (code == 256 && descriptors && item->armed) {
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
				else { if (ptrace(PTRACE_CONT, item->pid, 0, 0) < 0 && errno != ESRCH) goto fatal; item->stopped = 0; }
				continue;
			} else if (received > 0 || (errno != EAGAIN && errno != EINTR)) goto fatal;
		}
	manage_descriptors:
		if (descriptors && !draining) {
			if (!barrier) for (struct traced_process *item = processes; item; item = item->next)
				if (item->pending && !item->historical) { barrier = item->pid; break; }
			int ready = 1;
			for (struct traced_process *item = processes; item; item = item->next) {
				/* Between its vfork event and VFORK_DONE a parent runs nothing: it cannot stop, and needs not. */
				if (item->historical || item->stopped || item->vforking) continue;
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
					if (ptrace(item->listening ? PTRACE_LISTEN : !domain.enabled || domain.escaped ? PTRACE_CONT : PTRACE_SYSCALL, item->pid, 0, item->delivered) < 0 && errno != ESRCH) goto fatal;
					item->stopped = 0; item->delivered = 0; item->listening = 0;
				}
			}
		}
		if (retiring || (root_status >= 0 && !barrier)) {
			int busy = 0;
			for (struct traced_process *item = processes; item; item = item->next) busy |= item->job != NULL;
			if (!busy) {
				retiring = draining = 1;
				int live = release_tracees(&processes, &domain, &acknowledged);
				if (live < 0) goto fatal;
				if (!live) break;
			}
		}
		pid_t pid = waitpid(-1, &status, __WALL | WNOHANG | __WNOTHREAD);
		if (pid < 0) {
			if (errno == EINTR) continue;
			if (errno == ECHILD) break;
			goto fatal;
		}
		if (pid == 0) {
			size_t count = 2;
			for (struct traced_process *item = processes; item; item = item->next) if (item->job) count++;
			if (count > capacity) {
				struct pollfd *grown = realloc(polling, count * sizeof(*polling));
				if (!grown) goto fatal;
				polling = grown; capacity = count;
			}
			polling[0] = (struct pollfd){.fd = signals, .events = POLLIN};
			polling[1] = (struct pollfd){.fd = retiring ? -1 : STDIN_FILENO, .events = POLLIN};
			count = 2;
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
			release_process(&processes, pid, &domain, 1);
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
			current->awaiting_parent = event == PTRACE_EVENT_STOP;
		}
		current->stopped = 1; current->listening = 0;
		struct process_credentials credentials;
		if (!admitted) {
			int valid = !retiring && pid == root && event == PTRACE_EVENT_STOP && process_owned(pid, &credentials) && credentials.parent == (unsigned)getppid();
			if (valid && ptrace(PTRACE_SETOPTIONS, root, 0, flags) < 0) goto fatal;
			if (!valid) retiring = draining = 1;
			(void)transfer(3, valid ? "R" : "N", 1, 1); close(3); admitted = 1;
		}
		if (event == PTRACE_EVENT_EXEC && !process_owned(pid, &credentials)) retiring = draining = 1;
		if (event == PTRACE_EVENT_VFORK || event == PTRACE_EVENT_VFORK_DONE) current->vforking = event == PTRACE_EVENT_VFORK;
		if (descriptors && delivered == (SIGTRAP | 0x80)) {
			if (!draining && observe_descriptor_syscall(current, &domain) < 0) goto fatal;
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
			if (domain.enabled && !domain.escaped) {
				if (current->uncertain && current->syscall == SYS_clone3) { current->uncertain = 0; domain.uncertain--; }
				long shared = syscall(SYS_kcmp, pid, child, KCMP_FILES, 0, 0);
				if (shared < 0) domain.escaped = 1;
				else if (!shared && current->table) { known->table = current->table; known->table->references++; }
				else if (!(known->table = copy_descriptor_table(current))) goto fatal;
			}
		}
		if (!draining && event == PTRACE_EVENT_STOP && delivered != SIGTRAP) {
			if (descriptors) { current->listening = 1; continue; }
			if (ptrace(PTRACE_LISTEN, pid, 0, 0) < 0 && errno != ESRCH) goto fatal;
			current->stopped = 0;
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
				release_process(&processes, (pid_t)previous, &domain, 0);
			}
			/* exec unshares CLONE_FILES before closing only this image's CLOEXEC slots. */
			if (detach_descriptor_table(current, &domain) < 0) goto fatal;
			for (unsigned index = 0; current->table && index < current->table->count;)
				if (current->table->entries[index].cloexec) set_descriptor_origin(current, &domain,
					(struct descriptor_origin){.fd = current->table->entries[index].fd});
				else index++;
		}
		if (event == PTRACE_EVENT_EXEC && ++exec_events > 1) {
			if (!draining && skip && replace_with_exit(pid, skip_code) < 0) goto fatal;
			if (socket_path && !retiring) {
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
					if (start_decision(item, socket_path, token, execution_id, NULL) == 0) goto held;
					break;
				}
			}
		}
		if (event != 0) delivered = 0;
		if (descriptors || draining) { current->delivered = delivered; continue; }
		if (ptrace(PTRACE_CONT, pid, 0, delivered) < 0 && errno != ESRCH) goto fatal;
		current->stopped = 0;
	held:;
	}
	while (processes) release_process(&processes, processes->pid, &domain, 0);
	free(polling); close(signals);
	pthread_sigmask(SIG_SETMASK, &original, NULL);
	if (root_status < 0) return retiring ? 0 : 125;
	if (WIFEXITED(root_status)) return WEXITSTATUS(root_status);
	signal(WTERMSIG(root_status), SIG_DFL);
	raise(WTERMSIG(root_status));
	return 128 + WTERMSIG(root_status);
fatal:
	for (struct traced_process *item = processes; item; item = item->next) kill(item->pid, SIGKILL);
	while (waitpid(-1, &status, __WALL) > 0 || errno == EINTR) {}
	while (processes) release_process(&processes, processes->pid, &domain, 0);
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

struct output_relay { int source, writer, destination, started; pthread_t thread; };
static void *relay_output(void *argument) {
	struct output_relay *relay = argument; unsigned char bytes[16384];
	for (;;) {
		ssize_t count = read(relay->source, bytes, sizeof(bytes));
		if (count < 0 && errno == EINTR) continue;
		if (!count) return NULL;
		if (count < 0 || transfer(relay->destination, bytes, (size_t)count, 1) < 0) return (void *)1;
	}
}
static int descriptor_report(int output, struct file_position *positions, unsigned count, int journal, int initial) {
	if (dprintf(output, "OFD %u\n", count) < 0) return -1;
	for (unsigned index = 0; index < count; index++) {
		struct file_position *position = &positions[index];
		int flags = initial || position->duplicate < 0 ? position->flags : fcntl(position->duplicate, F_GETFL);
		off_t offset = position->before;
		if (!initial && position->stream && !journal) {
			unsigned char *bytes;
			int eof, remaining = pipe_bytes(position->duplicate, &bytes, &eof);
			const struct file_position *image = &positions[position->object];
			offset = image->content_length - remaining;
			int valid = remaining >= 0 && (position->stream >= 4 || eof == (position->stream == 1)) && offset >= 0 && (!remaining || !memcmp(bytes, image->content + offset, (size_t)remaining));
			free(bytes); if (!valid) return -1;
		} else if (!initial) offset = position->stream ? 0 : position->duplicate < 0 ? position->before : descriptor_seek(position->duplicate, flags, 0, SEEK_CUR);
		if (offset < 0 || flags < 0 || dprintf(output, "%d %d %jd %ju %ju\n",
			position->descriptor, flags, (intmax_t)offset, position->device, position->inode) < 0) return -1;
	}
	return 0;
}
static int execute_descriptors(const char *manifest, const char *report, char *executable, char **command, const char *route, unsigned closed_input, const int *output_flags, const rlim_t *stack_limit) {
	struct file_position positions[MAX_POSITIONS];
	struct output_relay relays[2] = {{.source = -1, .writer = -1, .destination = 1}, {.source = -1, .writer = -1, .destination = 2}};
	struct { int flags; uintmax_t inode; } output_state[2] = {{.flags = -1}, {.flags = -1}};
	char line[MAX_LINE];
	unsigned count = 0, close_input = closed_input, journal = 0, initialized = 0, inherited = 3;
	int result = 70, minimum = 3, output = -1, root_status = -1;
	/* Without a manifest the run inherits nothing beyond its standard streams. */
	FILE *input = manifest ? fopen(manifest, "re") : NULL;
	if (manifest && !input) return result;
	if (input && (!fgets(line, sizeof(line), input) || sscanf(line, "INPUTS %u %u %u", &count, &close_input, &journal) != 3 ||
		count > MAX_POSITIONS || close_input > 1 || journal > 1)) goto done;
	for (unsigned index = 0; index < count; index++) {
		struct file_position *position = &positions[index];
		*position = (struct file_position){.duplicate = -1, .writer = -1, .object = -1, .producer = -1}; initialized++;
		unsigned length;
		int alias;
		if (!fgets(line, sizeof(line), input) || sscanf(line, "%d %d %d %" SCNd64 " %u %d %d %d %d %d %d %d %u",
			&position->descriptor, &alias, &position->flags, &position->before, &length, &position->stream, &position->capacity, &position->shutdown, &position->peer_shutdown, &position->peer_descriptor, &position->outside, &position->installed, &position->socket_type) != 13 ||
			(position->stream == 4 || position->stream == 5 ? position->socket_type != SOCK_STREAM && position->socket_type != SOCK_DGRAM && position->socket_type != SOCK_SEQPACKET : position->socket_type != 0) ||
			(position->installed != 0 && position->installed != 1) || (!position->installed && (position->descriptor < 3 || alias != position->descriptor)) ||
			position->descriptor < 0 || position->descriptor == 1 || position->descriptor == 2 || position->descriptor == INT_MAX ||
			(close_input && position->descriptor == 0) || (index && position->descriptor <= positions[index - 1].descriptor) ||
			position->before < 0 || length >= PATH_MAX || alias > position->descriptor || alias < 0 || (position->stream < 0 || position->stream > 6) ||
			position->capacity < 0 || position->shutdown < 0 || position->shutdown > 3 || position->peer_shutdown < 0 || position->peer_shutdown > 3 || position->peer_descriptor < -1 || position->outside < 0 || position->outside > 3) goto done;
		position->alias = (int)index;
		if (alias != position->descriptor) {
			unsigned previous = 0;
			while (previous < index && positions[previous].descriptor != alias) previous++;
			if (previous == index || positions[previous].alias != (int)previous || length ||
				positions[previous].flags != position->flags || positions[previous].before != position->before || positions[previous].stream != position->stream || positions[previous].socket_type != position->socket_type) goto done;
			position->alias = (int)previous;
		} else if (!length) goto done;
		position->path = calloc((size_t)length + 1, 1);
		if (!position->path || fread(position->path, 1, length, input) != length || fgetc(input) != '\n' ||
			strlen(position->path) != length || (length && *position->path != '/')) goto done;
		if (position->descriptor >= minimum) minimum = position->descriptor + 1;
		if (alias == position->descriptor && (position->flags & O_PATH)) inherited++;
	}
	while (input && fgets(line, sizeof(line), input)) {
		int fd; struct queue_message message = {0};
		if (*line == 'L') {
			struct ofd_lock lock; int cursor;
			if (sscanf(line, "L %d %d %d %" SCNd64 " %" SCNd64 " %n", &fd, &lock.kind, &lock.type, &lock.start, &lock.length, &cursor) != 5 || line[cursor]) goto done;
			unsigned index = 0; while (index < count && positions[index].descriptor != fd) index++;
			if (index == count || positions[index].alias != (int)index || positions[index].stream || append_lock(&positions[index].locks, lock) < 0) goto done;
			continue;
		}
		if (parse_queue_message(line, &fd, &message) < 0) goto done;
		unsigned index = 0;
		while (index < count && positions[index].descriptor != fd) index++;
		if (index == count || positions[index].alias != (int)index || positions[index].stream < 4 || positions[index].stream > 5 ||
			append_message(&positions[index].messages, &message) < 0) goto done;
	}
	if (input && ferror(input)) goto done;
	if (input) fclose(input);
	input = NULL;
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
		} else if (position->stream == 6) {
			uint64_t value;
			if (!journal || position->before || load_queue_image(position) < 0 || position->content_length != 9 || position->content[8] > 1) goto done;
			memcpy(&value, position->content, 8);
			if (value == UINT64_MAX) goto done;
			int fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK | (position->content[8] ? EFD_SEMAPHORE : 0));
			if (fd < 0) goto done;
			int valid = (!value || write(fd, &value, 8) == 8) && fcntl(fd, F_SETFL, position->flags) == 0;
			if (valid) position->duplicate = fcntl(fd, F_DUPFD_CLOEXEC, minimum);
			close(fd); position->object = (int)index;
			if (!valid) goto done;
		} else if (position->stream) {
			if (position->before || (position->flags & ~(O_ACCMODE | O_APPEND | O_NONBLOCK | 0x8000))) goto done;
			for (unsigned previous = 0; previous < index; previous++) if (positions[previous].stream && !strcmp(position->path, positions[previous].path)) {
				position->object = positions[previous].object; break;
			}
			int fd;
			if (position->object >= 0) {
				if (position->stream >= 4) goto done;
				const struct file_position *image = &positions[position->object];
				/* The original opposite endpoint is an OFD too; reopening procfs would
				 * add O_LARGEFILE and manufacture another description. */
				if (!(position->flags & 0x8000) && (position->flags & O_ACCMODE) != (image->flags & O_ACCMODE)) {
					fd = fcntl(image->producer, F_DUPFD_CLOEXEC, minimum);
					if (fd >= 0 && fcntl(fd, F_SETFL, position->flags) < 0) { close(fd); goto done; }
				} else {
					fd = reopen_descriptor(image->duplicate, position->flags | O_CLOEXEC);
				}
			} else {
				int fds[2], valid = 1; struct file_position *peer = NULL;
				if (position->stream >= 4 && position->peer_descriptor >= 0) {
					for (unsigned other = index + 1; other < count; other++) if (positions[other].descriptor == position->peer_descriptor) { peer = &positions[other]; break; }
					if (!peer || peer->stream != 4 || peer->socket_type != position->socket_type || peer->peer_descriptor != position->descriptor || peer->alias != peer - positions || load_queue_image(peer) < 0) goto done;
				}
				if (load_queue_image(position) < 0 || (position->socket_type && position->socket_type != SOCK_STREAM &&
					(position->content_length || position->messages.count || (peer && (peer->content_length || peer->messages.count)))) ||
					(position->stream >= 4 ? socketpair(AF_UNIX, position->socket_type | SOCK_CLOEXEC | SOCK_NONBLOCK, 0, fds) : pipe2(fds, O_CLOEXEC | O_NONBLOCK)) < 0) goto done;
				if (position->stream >= 4) {
					int capacity = position->capacity / 2;
					valid = setsockopt(fds[0], SOL_SOCKET, SO_SNDBUF, &capacity, sizeof(capacity)) == 0 && socket_option(fds[0], SO_SNDBUF) == position->capacity;
					if (peer && valid) {
						capacity = peer->capacity / 2;
						valid = setsockopt(fds[1], SOL_SOCKET, SO_SNDBUF, &capacity, sizeof(capacity)) == 0 && socket_option(fds[1], SO_SNDBUF) == peer->capacity &&
							(peer->messages.count || transfer(fds[0], peer->content, (size_t)peer->content_length, 1) == 0) && fcntl(fds[1], F_SETFL, peer->flags) == 0;
					}
				} else if (fcntl(fds[1], F_GETPIPE_SZ) != position->capacity && fcntl(fds[1], F_SETPIPE_SZ, position->capacity) != position->capacity) valid = 0;
				if (valid && !position->messages.count) valid = transfer(fds[1], position->content, (size_t)position->content_length, 1) == 0;
				if (position->stream == 3) { int swap = fds[0]; fds[0] = fds[1]; fds[1] = swap; }
				if (valid) valid = fcntl(fds[0], F_SETFL, position->flags) == 0;
				if (position->stream != 1 && valid) { position->producer = fcntl(fds[1], F_DUPFD_CLOEXEC, minimum); valid = position->producer >= 0; }
				if (peer && valid) {
					peer->object = (int)(peer - positions); peer->duplicate = fcntl(fds[1], F_DUPFD_CLOEXEC, minimum);
					peer->producer = fcntl(fds[0], F_DUPFD_CLOEXEC, minimum); valid = peer->duplicate >= 0 && peer->producer >= 0;
				}
				close(fds[1]); fd = fds[0]; position->object = (int)index;
				if (!valid) { close(fd); goto done; }
				if (position->stream < 4 && (position->flags & 0x8000)) {
					int reopened = reopen_descriptor(fd, position->flags | O_CLOEXEC); close(fd); fd = reopened;
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
		if (position->stream == 6) position->event = event_state(getpid(), position->duplicate, NULL, NULL);
		if (position->duplicate < 0 || fstat(position->duplicate, &state) < 0 || !(S_ISREG(state.st_mode) || (position->stream && (S_ISFIFO(state.st_mode) || S_ISSOCK(state.st_mode))) ||
			(position->stream == 6 && position->event) ||
			S_ISDIR(state.st_mode) || (null_device(&state) && !position->before)) ||
			fcntl(position->duplicate, F_GETFL) != position->flags ||
			(!position->stream && descriptor_seek(position->duplicate, position->flags, position->before, SEEK_SET) != position->before)) goto done;
		position->device = state.st_dev; position->inode = state.st_ino;
		if (S_ISREG(state.st_mode) && position->alias == (int)index &&
			((position->flags & O_PATH) || (position->flags & O_ACCMODE) == O_WRONLY || (journal && !position->outside))) {
			int reader = open(position->path, O_RDONLY | O_CLOEXEC | O_NOFOLLOW); struct stat source;
			int valid = reader >= 0 && fstat(reader, &source) == 0 && source.st_dev == state.st_dev && source.st_ino == state.st_ino;
			if (valid) position->producer = fcntl(reader, F_DUPFD_CLOEXEC, minimum);
			close(reader); if (!valid || position->producer < 0) goto done;
		}
	}
	/* Reconstruct all message barriers after every referenced OFD exists. */
	for (unsigned index = 0; index < count; index++) {
		struct file_position *position = &positions[index];
		if (position->alias != (int)index || !position->messages.count) continue;
		size_t cursor = 0;
		struct decision_job image = {.positions = positions, .position_count = count};
		for (unsigned message = 0; message < position->messages.count; message++) {
			const struct queue_message *entry = &position->messages.entries[message];
			if (entry->end > (size_t)position->content_length || entry->start < cursor || transfer(position->producer, position->content + cursor, entry->start - cursor, 1) < 0) goto done;
			unsigned rights[MAX_POSITIONS];
			for (unsigned right = 0; right < entry->rights.count; right++) {
				unsigned other = 0;
				while (other < count && positions[other].descriptor != entry->rights.fds[right]) other++;
				if (other == count) goto done;
				rights[right] = other;
			}
			struct output_event event = {.kind = 2, .data = position->content + entry->start, .length = entry->end - entry->start};
			if (transfer_message(&image, position->producer, &event, rights, entry->rights.count) < 0) goto done;
			cursor = entry->end;
		}
		if (transfer(position->producer, position->content + cursor, (size_t)position->content_length - cursor, 1) < 0) goto done;
	}
	for (unsigned index = 0; index < count; index++) {
		struct file_position *position = &positions[index];
		if (position->alias != (int)index || position->stream < 4 || position->stream > 5) continue;
		if (position->shutdown && shutdown(position->duplicate, position->shutdown == 3 ? SHUT_RDWR : position->shutdown == 2 ? SHUT_WR : SHUT_RD) < 0) goto done;
		if (position->peer_shutdown && shutdown(position->producer, position->peer_shutdown == 3 ? SHUT_RDWR : position->peer_shutdown == 2 ? SHUT_WR : SHUT_RD) < 0) goto done;
		if (position->stream == 5) { close(position->producer); position->producer = -1; }
	}
	/* Preserve pipe versus socket semantics at the existing output outlet. Only
	 * requested pipe endpoints need relays; the usual libuv route has no extra work. */
	for (unsigned index = 0; route[2] && index < 2; index++) if (route[index + 2] == 'p' && (!index || route[0] != route[1])) {
		int pair[2]; if (pipe2(pair, O_CLOEXEC) < 0) goto done;
		relays[index].source = fcntl(pair[0], F_DUPFD_CLOEXEC, minimum);
		relays[index].writer = fcntl(pair[1], F_DUPFD_CLOEXEC, minimum);
		close(pair[0]); close(pair[1]);
		if (relays[index].source < 0 || relays[index].writer < 0) goto done;
	}
	/* Nothing to settle after the run: it replaces this launcher, as a native exec would. */
	int in_place = !manifest && !route[2];
	if (!in_place) output = open(report, O_WRONLY | O_CLOEXEC | O_NOFOLLOW);
	for (unsigned index = 0; index < count; index++) for (unsigned entry = 0; entry < positions[index].locks.count; entry++)
		if (set_file_lock(positions[index].duplicate, positions[index].locks.entries[entry]) < 0) goto done;
	if (!in_place && (output < 0 || prctl(PR_SET_CHILD_SUBREAPER, 1) < 0)) goto done;
	pid_t root = in_place ? 0 : fork();
	if (root < 0) goto done;
	if (!root) {
		close(output);
		if (close_input && close(0) < 0 && errno != EBADF) _exit(70);
		for (unsigned index = 0; index < count; index++)
			if (positions[index].installed && dup2(positions[index].duplicate, positions[index].descriptor) < 0) _exit(70);
		for (unsigned index = 0; index < 2; index++) if (relays[index].writer >= 0 && dup2(relays[index].writer, (int)index + 1) < 0) _exit(70);
		if (relays[0].writer >= 0 && route[0] == route[1] && dup2(1, 2) < 0) _exit(70);
		for (unsigned index = 0; index < 2; index++) { close(relays[index].source); close(relays[index].writer); }
		for (unsigned index = 0; index < count; index++) { close(positions[index].duplicate); close(positions[index].producer); }
		for (int fd = 1; fd <= 2; fd++) {
			int flags = output_flags[fd - 1] < 0 ? 0 : fcntl(fd, F_GETFL);
			if (flags < 0 || (output_flags[fd - 1] >= 0 && fcntl(fd, F_SETFL, (flags & ~(O_APPEND | O_NONBLOCK)) | (output_flags[fd - 1] & (O_APPEND | O_NONBLOCK))) < 0)) _exit(70);
		}
		if (stack_limit) {
			struct rlimit limit; if (getrlimit(RLIMIT_STACK, &limit) < 0) _exit(70);
			limit.rlim_cur = *stack_limit; if (setrlimit(RLIMIT_STACK, &limit) < 0) _exit(70);
		}
		execv(executable, command);
		_exit(errno == ENOENT ? 127 : 126);
	}
	if (descriptor_report(output, positions, count, journal, 1) < 0 || dprintf(output, "RUNNING %d\n", root) < 0) { kill(root, SIGKILL); goto done; }
	/* Journaling owns observations, not endpoints. Retain only references representing
	 * real owners outside the private subtree; target aliases/forks own their own FDs. */
	if (journal) for (unsigned index = 0; index < count; index++) {
		struct file_position *position = &positions[index];
		if (!position->stream) {
			struct stat state;
			if (fstat(position->duplicate, &state) < 0) goto done;
			if (S_ISREG(state.st_mode) && !position->outside) { close(position->duplicate); position->duplicate = -1; }
			continue;
		}
		int access = (position->flags & O_ACCMODE) + 1;
		if (!(position->outside & access)) { close(position->duplicate); position->duplicate = -1; }
		if (position->peer_descriptor >= 0 || (position->stream < 4 && !(position->outside & (3 ^ access)))) {
			close(position->producer); position->producer = -1;
		}
	}
	for (unsigned index = 0; index < 2; index++) {
		if (relays[index].source >= 0) {
			if (pthread_create(&relays[index].thread, NULL, relay_output, &relays[index])) { kill(root, SIGKILL); goto done; }
			relays[index].started = 1;
		}
	}
	for (;;) {
		int status;
		pid_t child = waitpid(-1, &status, 0);
		if (child == root) root_status = status;
		if (child >= 0 || errno == EINTR) continue;
		if (errno != ECHILD || root_status < 0) goto done;
		break;
	}
	/* Only our relay reads these private pipes. Keep the writer OFD until every
	 * descendant exits, seal its final status, then release it before joining EOF. */
	for (unsigned index = 0; route[2] && index < 2; index++) if (route[index + 2] == 'p') {
		int fd = relays[index && route[0] == route[1] ? 0 : index].writer;
		struct stat state;
		output_state[index].flags = fcntl(fd, F_GETFL);
		if (output_state[index].flags < 0 || fstat(fd, &state) < 0 || !S_ISFIFO(state.st_mode)) goto done;
		output_state[index].inode = state.st_ino;
	}
	for (unsigned index = 0; index < 2; index++) { close(relays[index].writer); relays[index].writer = -1; }
	for (unsigned index = 0; index < 2; index++) if (relays[index].started) {
		void *failed; int joined = pthread_join(relays[index].thread, &failed); relays[index].started = 0;
		if (joined || failed) goto done;
	}
	if (ftruncate(output, 0) < 0 || lseek(output, 0, SEEK_SET) != 0 || descriptor_report(output, positions, count, journal, 0) < 0) goto done;
	for (unsigned index = 0; index < 2; index++) if (output_state[index].flags >= 0 &&
		dprintf(output, "O %u %d %ju\n", index + 1, output_state[index].flags, output_state[index].inode) < 0) goto done;
	/* A nameless file still belongs to its inherited OFDs. Publish its final image once, before releasing the pins. */
	size_t detached_bytes = 0;
	for (unsigned index = 0; index < count; index++) {
		struct file_position *position = &positions[index]; struct stat state, named;
		if (position->stream) continue;
		int observer = position->duplicate >= 0 ? position->duplicate : position->producer;
		if (observer < 0 && position->alias != (int)index) continue;
		if (fstat(observer, &state) < 0) goto done;
		if (!S_ISREG(state.st_mode) || (state.st_nlink && *position->path && lstat(position->path, &named) == 0 && named.st_dev == state.st_dev && named.st_ino == state.st_ino)) continue;
		unsigned previous = 0;
		while (previous < index && (positions[previous].device != position->device || positions[previous].inode != position->inode)) previous++;
		if (previous < index) continue;
		if (state.st_size < 0 || (uint64_t)state.st_size > MAX_INPUT_BYTES - detached_bytes) goto done;
		size_t size = (size_t)state.st_size; detached_bytes += size;
		unsigned char *bytes = malloc(size + 1); if (!bytes) goto done;
		int reader = position->producer >= 0 ? position->producer : position->duplicate;
		int valid = pread(reader, bytes, size, 0) == (ssize_t)size && dprintf(output, "F %d %ju %ju %ju %jd %ld %zu\n",
			position->descriptor, (uintmax_t)state.st_mode, (uintmax_t)state.st_uid, (uintmax_t)state.st_gid,
			(intmax_t)state.st_mtim.tv_sec, state.st_mtim.tv_nsec, size) >= 0 && transfer(output, bytes, size, 1) == 0 && transfer(output, "\n", 1, 1) == 0;
		free(bytes); if (!valid) goto done;
	}
	result = WIFEXITED(root_status) ? WEXITSTATUS(root_status) : 128 + WTERMSIG(root_status);
done:
	if (input) fclose(input);
	for (unsigned index = 0; index < 2; index++) {
		if (relays[index].started) { pthread_cancel(relays[index].thread); pthread_join(relays[index].thread, NULL); }
		close(relays[index].source); close(relays[index].writer);
	}
	if (output >= 0 && close(output) < 0) result = 70;
	for (unsigned index = 0; index < initialized; index++) { free(positions[index].path); free(positions[index].content); free(positions[index].messages.entries); free(positions[index].locks.entries); close(positions[index].duplicate); close(positions[index].producer); }
	if (root_status >= 0 && WIFSIGNALED(root_status)) { signal(WTERMSIG(root_status), SIG_DFL); raise(WTERMSIG(root_status)); }
	return result;
}

int main(int argc, char **argv) {
	if (argc > 1 && !strcmp(argv[1], "--hold-pins")) return hold_pins(argc, argv);
	int observing = argc > 1 && (!strcmp(argv[1], "--attach") || !strcmp(argv[1], "--attach-skip-code") || !strcmp(argv[1], "--probe-observation"));
	if ((PI_SPEC_AUTHORITY_ONLY ? !observing : getauxval(AT_SECURE)) || (observing && prctl(PR_SET_DUMPABLE, 0, 0, 0, 0) < 0)) return 64;
	if (argc == 2 && !strcmp(argv[1], "--probe-observation")) {
		struct process_credentials credentials; unsigned long long extra = 1ULL << CAP_SYS_PTRACE;
		return process_credentials(getpid(), &credentials) == 0 && !(credentials.permitted & ~extra) &&
			!(credentials.effective & ~extra) && !credentials.inheritable && !credentials.ambient &&
			(credentials.no_new_privileges || (credentials.effective & extra)) ? 0 : 77;
	}
	if (argc == 3 && !strcmp(argv[1], "--directory-image")) {
		int fd = open(argv[2], O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
		if (fd < 0) return 74;
		unsigned char *bytes; int size = directory_bytes(fd, &bytes);
		close(fd);
		int result = size >= 0 && fwrite(bytes, 1, (size_t)size, stdout) == (size_t)size && fflush(stdout) == 0 ? 0 : 74;
		free(bytes); return result;
	}
	if (argc >= 2 && (!strcmp(argv[1], "--attach") || !strcmp(argv[1], "--attach-skip-code"))) {
		int probe = !strcmp(argv[1], "--attach-skip-code");
		if (probe && (argc != 3 || !*argv[2] || strspn(argv[2], "0123456789") != strlen(argv[2]) || strtoul(argv[2], NULL, 10) > 255)) return 64;
		char *socket = getenv("PI_SPEC_HELD_EXEC_SOCKET"), *token = getenv("PI_SPEC_HELD_EXEC_TOKEN"), *id = getenv("PI_SPEC_HELD_EXEC_ID");
		char *descriptors = getenv("PI_SPEC_HELD_EXEC_DESCRIPTORS"), *skip = getenv("PI_SPEC_HELD_EXEC_ROOT");
		if (!probe && (!socket || !token || !id)) return 64;
		return trace(socket, token, id, probe, probe ? (unsigned)atoi(argv[2]) : 0,
			descriptors && !strcmp(descriptors, "1") ? 1 : descriptors && !strcmp(descriptors, "2") ? 2 : 0, !!skip);
	}
	if (argc >= 2 && (!strcmp(argv[1], "--exec") || !strcmp(argv[1], "--exec-closed-input") || !strcmp(argv[1], "--exec-fds"))) {
		int descriptors = !strcmp(argv[1], "--exec-fds"), name = descriptors ? 5 : 3, output_flags[2] = {-1, -1};
		/* ",<stdout>,<stderr>": status flags the target's outputs carry natively, its parent's own (make's O_APPEND). */
		char *suffix = argc > 2 ? strchr(argv[2], ',') : NULL;
		if (suffix && (*suffix++ = 0, sscanf(suffix, "%d,%d", &output_flags[0], &output_flags[1]) != 2 || output_flags[0] < 0 || output_flags[1] < 0)) return 64;
		char *stack = argc > 2 ? strchr(argv[2], ':') : NULL;
		rlim_t stack_limit = 0;
		if (stack) {
			*stack++ = 0; char *end = stack + strlen(stack); errno = 0;
			stack_limit = !strcmp(stack, "unlimited") ? RLIM_INFINITY : strtoull(stack, &end, 10);
			if ((!isdigit((unsigned char)*stack) && strcmp(stack, "unlimited")) || *end || errno) return 64;
		}
		if (argc < name + 2 || strspn(argv[2], "012") != 2 ||
			(strlen(argv[2]) != 2 && (!descriptors || strlen(argv[2]) != 4 || strspn(argv[2] + 2, "ps") != 2 ||
				(argv[2][0] == argv[2][1] && argv[2][2] != argv[2][3])))) return 64;
		/* Save stdout's source before changing either endpoint, including swapped routes.
		 * Route 0 discards into /dev/null; Node fills vacant stdio, so the device opens above it. */
		int discard = memchr(argv[2], '0', 2) ? open("/dev/null", O_WRONLY | O_CLOEXEC) : -1;
		int output = fcntl(argv[2][0] == '0' ? discard : argv[2][0] - '0', F_DUPFD_CLOEXEC, 3);
		if (output < 0) return 70;
		int routed = dup2(argv[2][1] == '0' ? discard : argv[2][1] - '0', 2) >= 0 && dup2(output, 1) >= 0;
		close(output); close(discard);
		if (!routed) return 70;
		if (reset_signals() < 0) return 70;
		char *executable = argv[name + 1], *manifest = descriptors ? argv[3] : NULL, *report = descriptors ? argv[4] : NULL;
		argv[name + 1] = argv[name];
		return execute_descriptors(manifest, report, executable, argv + name + 1, argv[2],
			!strcmp(argv[1], "--exec-closed-input"), output_flags, stack ? &stack_limit : NULL);
	}
	if (argc == 4 && !strcmp(argv[1], "--probe-context")) return probe_context(argv[2], argv[3]);
	if (argc == 2 && !strcmp(argv[1], "--probe-clean-fds")) {
		int extra = has_unmodeled_descriptors(3);
		return extra < 0 ? 70 : extra ? 65 : 0;
	}
	return 64;
}
