#define _GNU_SOURCE
#include <dirent.h>
#include <elf.h>
#include <linux/prctl.h>
#include <sys/uio.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/eventfd.h>
#include <limits.h>
#include <inttypes.h>
#include <linux/audit.h>
#include <linux/kcmp.h>
#include <linux/magic.h>
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
struct decision_job {
	struct process_image *image;
	pthread_t thread;
	pid_t pid;
	const char *socket_path, *token, *execution_id;
	int channel[2], connection, outputs[3], result, pidfd;
	struct output_event *events;
	unsigned count, resource_count;
	struct file_position *positions;
	unsigned position_count;
	struct descriptor_domain *domain;
	struct traced_process *process;
	char *context;
	struct { dev_t device; ino_t inode; int access; } *references;
	unsigned reference_count;
	int reference_state, needs_tracking;
	struct file_position *captures;
	unsigned capture_count;
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

/* A continuation carries private userspace state, never another process's PID or
 * kernel handles. The existing resource transaction supplies the authoritative
 * OFDs before this image resumes at its blocked read. */
#define IMAGE_MAPS 128
#define IMAGE_BYTES (64UL * 1024 * 1024)
#define IMAGE_SCRATCH 0x100000000UL
struct image_map {
	uint64_t start, end, offset, size, device, inode;
	unsigned prot, file;
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
	char path[64]; snprintf(path, sizeof(path), "/proc/%d/mem", pid);
	int fd = open(path, (writing ? O_RDWR : O_RDONLY) | O_CLOEXEC);
	if (fd < 0) return -1;
	size_t moved = 0;
	while (moved < bytes) {
		ssize_t size = writing ? pwrite(fd, (char *)data + moved, bytes - moved, (off_t)(address + moved)) :
			pread(fd, (char *)data + moved, bytes - moved, (off_t)(address + moved));
		if (size < 0 && errno == EINTR) continue;
		if (size <= 0) break;
		moved += (size_t)size;
	}
	close(fd); return moved == bytes ? 0 : -1;
}
static int image_call(pid_t pid, unsigned long at, long number, unsigned long a, unsigned long b,
	unsigned long c, unsigned long d, unsigned long e, unsigned long f, long *result) {
	struct user_regs_struct saved, call;
	if (ptrace(PTRACE_GETREGS, pid, 0, &saved) < 0) return -1;
	call = saved;
	unsigned long original = 0; int bootstrap = !at, status;
	if (bootstrap) {
		at = saved.rip; errno = 0; original = (unsigned long)ptrace(PTRACE_PEEKTEXT, pid, at, 0);
		if (errno || ptrace(PTRACE_POKETEXT, pid, at, (original & ~0xffffffUL) | 0xcc050fUL) < 0) return -1;
	}
	call.rip = at; call.rax = number; call.orig_rax = (unsigned long)-1;
	call.rdi = a; call.rsi = b; call.rdx = c; call.r10 = d; call.r8 = e; call.r9 = f;
	int unmapping = at == IMAGE_SCRATCH && number == SYS_munmap && a == IMAGE_SCRATCH;
	if (ptrace(PTRACE_SETREGS, pid, 0, &call) < 0) return -1;
	for (unsigned step = 0; step < (unmapping ? 2U : 1U); step++)
		if (ptrace(unmapping ? PTRACE_SYSCALL : PTRACE_CONT, pid, 0, 0) < 0 || image_wait(pid, &status) < 0 ||
			WSTOPSIG(status) != (unmapping ? SIGTRAP | 0x80 : SIGTRAP) || (unsigned)status >> 16) return -1;
	if (ptrace(PTRACE_GETREGS, pid, 0, &call) < 0 || call.rip != at + (unmapping ? 2 : 3)) return -1;
	if (bootstrap && ptrace(PTRACE_POKETEXT, pid, at, original) < 0) return -1;
	if (ptrace(PTRACE_SETREGS, pid, 0, &saved) < 0) return -1;
	*result = (long)call.rax; return 0;
}
static int image_syscall(pid_t pid, long number, unsigned long a, unsigned long b,
	unsigned long c, unsigned long d, unsigned long e, unsigned long f, long expected) {
	long result;
	return image_call(pid, IMAGE_SCRATCH, number, a, b, c, d, e, f, &result) == 0 && result == expected ? 0 : -1;
}
static int image_scratch(pid_t pid) {
	long result; unsigned char code[] = {0x0f, 0x05, 0xcc};
	return image_call(pid, 0, SYS_mmap, IMAGE_SCRATCH, 8192, PROT_READ | PROT_WRITE | PROT_EXEC,
		MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED_NOREPLACE, (unsigned long)-1, 0, &result) == 0 && result == IMAGE_SCRATCH &&
		image_memory(pid, IMAGE_SCRATCH, code, sizeof(code), 1) == 0 ? 0 : -1;
}
static int image_maps(pid_t pid, struct image_header *header, int capturing) {
	char path[64], line[8192]; snprintf(path, sizeof(path), "/proc/%d/smaps", pid);
	FILE *input = fopen(path, "re"); if (!input) return -1;
	int valid = 0, checked = 1;
	struct image_map *map = NULL;
	while (fgets(line, sizeof(line), input)) {
		if (!strncmp(line, "VmFlags:", 8)) {
			checked = 1;
			if (!capturing || !map || image_special(map)) continue;
			char *saved;
			for (char *flag = strtok_r(line + 8, " \n", &saved); flag; flag = strtok_r(NULL, " \n", &saved)) {
				if (!strcmp(flag, "gd")) { if (strcmp(map->path, "[stack]")) goto done; }
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
static int image_mm(pid_t pid, struct image_header *header) {
	char path[64], data[8192]; unsigned long fields[53] = {0};
	snprintf(path, sizeof(path), "/proc/%d/stat", pid);
	FILE *file = fopen(path, "re"); if (!file) return -1;
	int valid = fgets(data, sizeof(data), file) != NULL; fclose(file); if (!valid) return -1;
	char *position = strrchr(data, ')'), *saved; if (!position) return -1;
	unsigned index = 3;
	for (char *part = strtok_r(position + 2, " ", &saved); part && index < 53; part = strtok_r(NULL, " ", &saved), index++)
		if (index != 3) fields[index] = strtoull(part, NULL, 10);
	long brk;
	if (index < 52 || fields[20] != 1 || image_call(pid, IMAGE_SCRATCH, SYS_brk, 0, 0, 0, 0, 0, 0, &brk) < 0) return -1;
	header->mm = (struct prctl_mm_map){.start_code = fields[26], .end_code = fields[27], .start_stack = fields[28],
		.start_data = fields[45], .end_data = fields[46], .start_brk = fields[47], .brk = (unsigned long)brk,
		.arg_start = fields[48], .arg_end = fields[49], .env_start = fields[50], .env_end = fields[51], .exe_fd = (unsigned)-1};
	snprintf(path, sizeof(path), "/proc/%d/auxv", pid); int fd = open(path, O_RDONLY | O_CLOEXEC); if (fd < 0) return -1;
	ssize_t size = read(fd, header->auxv, sizeof(header->auxv)); close(fd);
	if (size <= 0 || size >= (ssize_t)sizeof(header->auxv)) return -1;
	header->auxv_size = (size_t)size; return 0;
}
static int image_read_backing(int fd, const struct image_map *map, void *buffer) {
	size_t length = (size_t)(map->end - map->start), bytes = map->offset >= map->size ? 0 :
		(size_t)(map->size - map->offset < length ? map->size - map->offset : length), moved = 0;
	memset(buffer, 0, length);
	while (moved < bytes) {
		ssize_t size = pread(fd, (char *)buffer + moved, bytes - moved, (off_t)(map->offset + moved));
		if (size < 0 && errno == EINTR) continue;
		if (size <= 0) return -1;
		moved += (size_t)size;
	}
	return 0;
}
/* pagemap's file-page bit is available without PFNs. Equal bytes alone cannot
 * distinguish an untouched file page from a private page written back to its old value. */
static int image_cow(pid_t pid, uint64_t start, size_t length, unsigned char *cow) {
	char path[64]; snprintf(path, sizeof(path), "/proc/%d/pagemap", pid);
	int fd = open(path, O_RDONLY | O_CLOEXEC); if (fd < 0) return -1;
	uint64_t pages[256]; size_t count = length / 4096, moved = 0;
	while (moved < count) {
		size_t chunk = count - moved < 256 ? count - moved : 256;
		ssize_t size = pread(fd, pages, chunk * sizeof(*pages), (off_t)((start / 4096 + moved) * sizeof(*pages)));
		if (size < 0 && errno == EINTR) continue;
		if (size != (ssize_t)(chunk * sizeof(*pages))) break;
		for (size_t i = 0; i < chunk; i++) {
			if (!(pages[i] & (UINT64_C(1) << 63))) { close(fd); return -1; }
			cow[moved + i] = !(pages[i] & (UINT64_C(1) << 61));
		}
		moved += chunk;
	}
	close(fd); return moved == count ? 0 : -1;
}
static int image_descriptors(pid_t pid, struct process_image *image) {
	char path[64], line[256]; snprintf(path, sizeof(path), "/proc/%d/fd", pid);
	DIR *directory = opendir(path); if (!directory) return -1;
	struct dirent *entry; int valid = 0;
	while ((entry = readdir(directory))) {
		if (*entry->d_name == '.') continue;
		if (image->fd_count == MAX_HANDLES) goto done;
		char *end; long fd = strtol(entry->d_name, &end, 10);
		if (*end || fd < 0 || fd > INT_MAX) goto done;
		struct stat state; snprintf(path, sizeof(path), "/proc/%d/fd/%ld", pid, fd);
		if (stat(path, &state) < 0) goto done;
		unsigned index = image->fd_count++;
		image->fds[index].fd = (int)fd; image->fds[index].flags = -1; image->fds[index].offset = -1;
		image->fds[index].device = state.st_dev; image->fds[index].inode = state.st_ino;
		snprintf(path, sizeof(path), "/proc/%d/fdinfo/%ld", pid, fd); FILE *info = fopen(path, "re"); if (!info) goto done;
		while (fgets(line, sizeof(line), info)) {
			unsigned flags; int64_t offset;
			if (sscanf(line, "flags: %o", &flags) == 1 && flags <= INT_MAX) image->fds[index].flags = (int)flags;
			if (sscanf(line, "pos: %" SCNd64, &offset) == 1) image->fds[index].offset = offset;
		}
		int failed = ferror(info); fclose(info);
		if (failed || image->fds[index].flags < 0 || image->fds[index].offset < 0) goto done;
	}
	valid = 1;
done:
	closedir(directory); return valid ? 0 : -1;
}

/* Called by the already authorized strace owner; no new ptracer exception or
 * sandbox permission is needed. The private process is retired on either result. */
unsigned pi_process_image_protocol(void) { return 34; }
int pi_process_image_frontier(long number) {
	return number == SYS_read || number == SYS_readv || number == SYS_recvfrom || number == SYS_recvmsg ||
		number == SYS_write || number == SYS_writev || number == SYS_sendto || number == SYS_sendmsg;
}
static int image_status(pid_t pid, int consumer) {
	char path[64], line[256]; snprintf(path, sizeof(path), "/proc/%d/status", pid);
	FILE *status = fopen(path, "re"); if (!status) return -1;
	unsigned seen = 0; int valid = 1;
	while (fgets(line, sizeof(line), status)) {
		unsigned value; unsigned long pending;
		if (sscanf(line, "Threads: %u", &value) == 1) { seen |= 1; valid &= value == 1; }
		if (sscanf(line, "Seccomp: %u", &value) == 1) { seen |= 2; valid &= !consumer || !value; }
		if (sscanf(line, "SigPnd: %lx", &pending) == 1) { seen |= 4; valid &= !pending; }
		if (sscanf(line, "ShdPnd: %lx", &pending) == 1) { seen |= 8; valid &= !pending; }
	}
	valid &= !ferror(status) && seen == 15; fclose(status); return valid ? 0 : -1;
}
int pi_capture_process_image(pid_t pid, const char *path, unsigned long watched_tid) {
	struct process_image *image = image_new(); int result = -1, output = -1;
	if (!image) return -1;
	struct image_header *header = &image->header;
	if (ptrace(PTRACE_GETREGS, pid, 0, &header->registers) < 0 || header->registers.cs != 0x33 ||
		!pi_process_image_frontier(header->registers.orig_rax) || ((long)header->registers.rax != -512 && (long)header->registers.rax != -514) ||
		image_status(pid, 0) < 0) goto done;
	unsigned long original; unsigned char bootstrap_cow[2];
	uint64_t bootstrap = header->registers.rip & ~UINT64_C(4095);
	size_t bootstrap_length = ((header->registers.rip + sizeof(original) - 1) & ~UINT64_C(4095)) - bootstrap + 4096;
	if (image_memory(pid, header->registers.rip, &original, sizeof(original), 0) < 0 ||
		image_cow(pid, bootstrap, bootstrap_length, bootstrap_cow) < 0) goto done;
	header->registers.rax = header->registers.orig_rax; header->registers.orig_rax = (unsigned long)-1; header->registers.rip -= 2;
	if (image_descriptors(pid, image) < 0 || image_scratch(pid) < 0 || image_mm(pid, header) < 0) goto done;
	struct iovec xstate = {.iov_base = header->xstate, .iov_len = sizeof(header->xstate)};
	if (ptrace(PTRACE_GETREGSET, pid, NT_X86_XSTATE, &xstate) < 0 ||
		ptrace(PTRACE_GETSIGMASK, pid, sizeof(header->sigmask), &header->sigmask) < 0) goto done;
	header->xstate_size = xstate.iov_len;
	for (unsigned signal = 1; signal <= 64; signal++) if (signal != SIGKILL && signal != SIGSTOP) {
		if (image_syscall(pid, SYS_rt_sigaction, signal, 0, IMAGE_SCRATCH + 4096, 8, 0, 0, 0) < 0 ||
			image_memory(pid, IMAGE_SCRATCH + 4096, &header->actions[signal], sizeof(header->actions[signal]), 0) < 0) goto done;
	}
	if (image_syscall(pid, SYS_sigaltstack, 0, IMAGE_SCRATCH + 4096, 0, 0, 0, 0, 0) < 0 ||
		image_memory(pid, IMAGE_SCRATCH + 4096, &header->alternate, sizeof(header->alternate), 0) < 0 || (header->alternate.ss_flags & SS_ONSTACK) ||
		image_syscall(pid, SYS_prctl, PR_GET_TID_ADDRESS, IMAGE_SCRATCH + 4096, 0, 0, 0, 0, 0) < 0 ||
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
	header->magic = UINT64_C(0x50494d4147453033);
	output = open(path, O_WRONLY | O_CLOEXEC | O_CREAT | O_EXCL | O_NOFOLLOW, 0600);
	if (output < 0 || dprintf(output, "PIIMAGE3 %d %u %zu %" PRIu64 "\n", pid, image->fd_count, sizeof(*header), header->length) < 0) goto done;
	for (unsigned i = 0; i < image->fd_count; i++) if (dprintf(output, "%d %d %" PRId64 " %ju %ju %d\n", image->fds[i].fd,
		image->fds[i].flags, image->fds[i].offset, image->fds[i].device, image->fds[i].inode, image->fds[i].fd) < 0) goto done;
	if (transfer(output, header, sizeof(*header), 1) == 0 && transfer(output, image->data, (size_t)header->length, 1) == 0) result = 0;
done:
	if (output >= 0 && close(output) < 0) result = -1;
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
	if (image_line(&input, line, sizeof(line)) < 0 || sscanf(line, "PIIMAGE3 %d %u %zu %" SCNu64, &image->producer,
		&image->fd_count, &header_size, &bytes) != 4 || image->producer <= 0 || image->fd_count > MAX_HANDLES ||
		header_size != sizeof(*header) || bytes > IMAGE_BYTES) goto fail;
	for (unsigned i = 0; i < image->fd_count; i++) if (image_line(&input, line, sizeof(line)) < 0 ||
		sscanf(line, "%d %d %" SCNd64 " %ju %ju %d", &image->fds[i].fd, &image->fds[i].flags,
			&image->fds[i].offset, &image->fds[i].device, &image->fds[i].inode, &image->fds[i].source) != 6 ||
		image->fds[i].fd < 0 || image->fds[i].fd == INT_MAX || image->fds[i].source < 0 || image->fds[i].flags < 0 || image->fds[i].offset < 0) goto fail;
	if (input.remaining != sizeof(*header) + bytes) goto fail;
	memcpy(header, input.cursor, sizeof(*header));
	if (header->magic != UINT64_C(0x50494d4147453033) || header->length != bytes ||
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
			(i && header->maps[i - 1].end > map->start) || map->prot > 7 || map->file != (unsigned)(*map->path == '/')) goto fail;
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
				char path[64]; snprintf(path, sizeof(path), "/proc/self/fd/%d", job->captures[pin].duplicate);
				image->files[i] = open(path, O_RDONLY | O_CLOEXEC); break;
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

/* Duplicate from the original OFDs before replacing any slot, so cycles retain
 * identity. Qualification uses the same pinned OFDs in a disposable child. */
static int image_handles(pid_t pid, struct decision_job *job) {
	struct process_image *image = job->image, *current = image_new();
	int result = -1, minimum = 3;
	long pins[MAX_HANDLES];
	if (!current || image_descriptors(pid, current) < 0) goto done;
	for (unsigned i = 0; i < current->fd_count; i++) if (current->fds[i].fd >= minimum) minimum = current->fds[i].fd + 1;
	for (unsigned i = 0; i < image->fd_count; i++) if (image->fds[i].fd >= minimum) minimum = image->fds[i].fd + 1;
	for (unsigned i = 0; i < image->fd_count; i++) {
		int source = image->fds[i].source;
		if (pid != job->pid && source >= 3) {
			unsigned p = 0;
			while (p < job->position_count && job->positions[p].descriptor != source) p++;
			if (p == job->position_count) goto done;
			source = job->positions[p].duplicate;
		}
		if (image_call(pid, IMAGE_SCRATCH, SYS_fcntl, source, F_DUPFD_CLOEXEC, minimum, 0, 0, 0, &pins[i]) < 0 || pins[i] < minimum || pins[i] >= INT_MAX) goto done;
		minimum = (int)pins[i] + 1;
	}
	for (unsigned i = 0; i < image->fd_count; i++) if (image_syscall(pid, SYS_dup3, (unsigned long)pins[i], image->fds[i].fd,
		image->fds[i].flags & O_CLOEXEC, 0, 0, 0, image->fds[i].fd) < 0) goto done;
	for (unsigned i = 0; i < current->fd_count; i++) {
		unsigned next = 0;
		while (next < image->fd_count && image->fds[next].fd != current->fds[i].fd) next++;
		if (next == image->fd_count && image_syscall(pid, SYS_close, current->fds[i].fd, 0, 0, 0, 0, 0, 0) < 0) goto done;
	}
	for (unsigned i = 0; i < image->fd_count; i++) if (image_syscall(pid, SYS_close, (unsigned long)pins[i], 0, 0, 0, 0, 0, 0) < 0) goto done;
	result = 0;
done:
	image_free(current); return result;
}
static int image_restore(pid_t pid, struct decision_job *job) {
	struct process_image *image = job->image;
	struct image_header *header = &image->header, *initial = calloc(1, sizeof(*initial));
	int result = -1; size_t offset = 0;
	if (!initial || image_maps(pid, initial, 0) < 0 || image_scratch(pid) < 0) goto done;
	/* A forked qualification child may inherit an rseq registration. Exec clears it
	 * in the actual consumer; unregister before removing either old image. */
	if (ptrace((enum __ptrace_request)0x420f, pid, sizeof(initial->rseq), &initial->rseq) != sizeof(initial->rseq)) goto done;
	if (initial->rseq.pointer && image_syscall(pid, SYS_rseq, initial->rseq.pointer, initial->rseq.size, 1, initial->rseq.signature, 0, 0, 0) < 0) goto done;
	for (unsigned i = 0; i < initial->count; i++) if (!image_special(&initial->maps[i])) {
		const struct image_map *map = &initial->maps[i];
		if (image_syscall(pid, SYS_munmap, map->start, map->end - map->start, 0, 0, 0, 0, 0) < 0) goto done;
	}
	for (unsigned i = 0; i < initial->count; i++) if (image_special(&initial->maps[i]) && strcmp(initial->maps[i].path, "[vsyscall]")) {
		const struct image_map *map = &initial->maps[i]; unsigned target = 0;
		while (target < header->count && strcmp(header->maps[target].path, map->path)) target++;
		if (target == header->count) goto done;
		const struct image_map *next = &header->maps[target]; if (map->end - map->start != next->end - next->start) goto done;
		if (map->start != next->start && image_syscall(pid, SYS_mremap, map->start, map->end - map->start,
			next->end - next->start, MREMAP_MAYMOVE | MREMAP_FIXED, next->start, 0, (long)next->start) < 0) goto done;
	}
	for (unsigned i = 0; i < header->count; i++) {
		struct image_map *map = &header->maps[i]; if (image_special(map)) continue;
		long fd = -1; int flags = MAP_PRIVATE | MAP_FIXED;
		if (map->file) {
			char path[64]; snprintf(path, sizeof(path), "/proc/%d/fd/%d", getpid(), image->files[i]);
			if (image_memory(pid, IMAGE_SCRATCH + 4096, path, strlen(path) + 1, 1) < 0 ||
				image_call(pid, IMAGE_SCRATCH, SYS_openat, (unsigned long)AT_FDCWD, IMAGE_SCRATCH + 4096, O_RDONLY | O_CLOEXEC, 0, 0, 0, &fd) < 0 || fd < 0) goto done;
		} else { flags |= MAP_ANONYMOUS; if (!strcmp(map->path, "[stack]")) flags |= MAP_GROWSDOWN; }
		size_t length = (size_t)(map->end - map->start);
		if (image_syscall(pid, SYS_mmap, map->start, length, PROT_READ | PROT_WRITE, flags, (unsigned long)fd, map->offset, (long)map->start) < 0 ||
			(fd >= 0 && image_syscall(pid, SYS_close, (unsigned long)fd, 0, 0, 0, 0, 0, 0) < 0)) goto done;
		if (!map->file) { if (image_memory(pid, map->start, image->data + offset, length, 1) < 0) goto done; }
		else for (size_t page = 0; page < length / 4096; page++) if (image->data[offset + 2 * length + page] &&
			image_memory(pid, map->start + page * 4096, image->data + offset + page * 4096, 4096, 1) < 0) goto done;
		if (image_syscall(pid, SYS_mprotect, map->start, length, map->prot, 0, 0, 0, 0) < 0) goto done;
		offset += length * (1 + map->file) + map->file * (length / 4096);
	}
	if (header->clear_tid) {
		pid_t original;
		if (image_memory(pid, header->clear_tid, &original, sizeof(original), 0) < 0 || original != image->producer ||
			image_memory(pid, header->clear_tid, &pid, sizeof(pid), 1) < 0) goto done;
	}
	if (image_syscall(pid, SYS_set_tid_address, header->clear_tid, 0, 0, 0, 0, 0, pid) < 0 ||
		(header->robust && image_syscall(pid, SYS_set_robust_list, header->robust, header->robust_length, 0, 0, 0, 0, 0) < 0) ||
		(header->rseq.pointer && image_syscall(pid, SYS_rseq, header->rseq.pointer, header->rseq.size, 0, header->rseq.signature, 0, 0, 0) < 0)) goto done;
	if (image_handles(pid, job) < 0) goto done;
	for (unsigned signal = 1; signal <= 64; signal++) if (signal != SIGKILL && signal != SIGSTOP) {
		if (image_memory(pid, IMAGE_SCRATCH + 4096, &header->actions[signal], sizeof(header->actions[signal]), 1) < 0 ||
			image_syscall(pid, SYS_rt_sigaction, signal, IMAGE_SCRATCH + 4096, 0, 8, 0, 0, 0) < 0) goto done;
	}
	struct prctl_mm_map mm = header->mm; mm.auxv = (void *)(IMAGE_SCRATCH + 4096); mm.auxv_size = header->auxv_size;
	if (image_memory(pid, IMAGE_SCRATCH + 4096, &header->alternate, sizeof(header->alternate), 1) < 0 ||
		image_syscall(pid, SYS_sigaltstack, IMAGE_SCRATCH + 4096, 0, 0, 0, 0, 0, 0) < 0 ||
		image_memory(pid, IMAGE_SCRATCH + 4096, header->auxv, header->auxv_size, 1) < 0 ||
		image_memory(pid, IMAGE_SCRATCH + 256, &mm, sizeof(mm), 1) < 0 ||
		image_syscall(pid, SYS_prctl, PR_SET_MM, PR_SET_MM_MAP, IMAGE_SCRATCH + 256, sizeof(mm), 0, 0, 0) < 0 ||
		ptrace(PTRACE_SETREGS, pid, 0, &header->registers) < 0 ||
		image_syscall(pid, SYS_munmap, IMAGE_SCRATCH, 8192, 0, 0, 0, 0, 0) < 0) goto done;
	struct iovec xstate = {.iov_base = header->xstate, .iov_len = header->xstate_size};
	if (ptrace(PTRACE_SETREGSET, pid, NT_X86_XSTATE, &xstate) < 0 ||
		ptrace(PTRACE_SETSIGMASK, pid, sizeof(header->sigmask), &header->sigmask) < 0 ||
		ptrace(PTRACE_SETREGS, pid, 0, &header->registers) < 0) goto done;
	result = 0;
done:
	free(initial); return result;
}

static int image_qualify(struct decision_job *job) {
	pid_t child = fork(); if (child < 0) return -1;
	if (!child) {
		if (prctl(PR_SET_PDEATHSIG, SIGKILL) < 0 || ptrace(PTRACE_TRACEME, 0, 0, 0) < 0 || raise(SIGSTOP)) _exit(70);
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
	char path[64], line[512]; snprintf(path, sizeof(path), "/proc/%ld/fdinfo/%d", (long)pid, fd);
	FILE *file = fopen(path, "re"); if (!file) return -1;
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
	char name[64], line[256]; unsigned id = 0, found = 0; uint64_t count = 0; unsigned mode = 0;
	snprintf(name, sizeof(name), "/proc/%ld/fdinfo/%d", (long)pid, fd);
	FILE *file = fopen(name, "re");
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
	char name[64]; struct stat state;
	snprintf(name, sizeof(name), "/proc/%ld/fd/%d", (long)pid, fd);
	if (stat(name, &state) < 0) { domain->incomplete = 1; return 0; }
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
	int flags = descriptor_flags(process->pid, fd);
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

/* Read a separate directory OFD so snapshots never consume the shared cursor.
 * Kernel cookies and inode numbers remain raw 64-bit values. Padding is not data. */
static int directory_bytes(int fd, unsigned char **bytes) {
	char path[64]; struct stat before, after; int result = -1;
	*bytes = NULL;
	snprintf(path, sizeof(path), "/proc/self/fd/%d", fd);
	int reader = open(path, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
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
		char name[64]; snprintf(name, sizeof(name), "/proc/self/fd/%d", fd);
		reader = open(name, O_RDONLY | O_CLOEXEC | O_NONBLOCK);
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

/* A captured queue owns its references through the common stream journal. Other
 * queues keep their rights alive independently of the exec image's FD table. */
static int queue_references(struct decision_job *job, pid_t pid, int fd, const struct stat *state) {
	for (unsigned index = 0; index < job->capture_count; index++)
		if (job->captures[index].device == (uintmax_t)state->st_dev && job->captures[index].inode == (uintmax_t)state->st_ino) return 0;
	struct decision_job source = {.pid = pid, .pidfd = -1};
	int pin = duplicate_tracee_fd(&source, (unsigned)fd), eof;
	close(source.pidfd);
	if (pin < 0) return -1;
	unsigned char *bytes; struct queue_messages messages = {0};
	int valid = socket_bytes(pin, &bytes, &eof, &messages, owned_pin(job->domain, pin), &job->needs_tracking) >= 0;
	free(bytes); close(pin);
	for (unsigned message = 0; valid && message < messages.count; message++) for (unsigned index = 0; valid && index < messages.entries[message].rights.count; index++) {
		int reference = messages.entries[message].rights.fds[index];
		struct stat entry; int flags = fcntl(reference, F_GETFL);
		if (flags < 0 || fstat(reference, &entry) < 0 || job->reference_count == MAX_OBJECTS) { valid = 0; break; }
		if (!S_ISFIFO(entry.st_mode) && !S_ISSOCK(entry.st_mode)) continue;
		job->references[job->reference_count].device = entry.st_dev;
		job->references[job->reference_count].inode = entry.st_ino;
		job->references[job->reference_count++].access = (flags & O_ACCMODE) + 1;
	}
	close_messages(&messages);
	return valid ? 0 : -1;
}

/* References outside the exec image survive its closes. No snapshot pin is an owner. */
static int outside_references(struct decision_job *job, int pin) {
	struct descriptor_domain *domain = job->domain;
	if (!domain) return 3;
	if (!domain->enabled) {
		/* Learning has no ancestry proof. Snapshot its stopped tree once for the
		 * prediction; adoption must independently prove the same graph with tracking. */
		if (!job->reference_state) {
			job->reference_state = -1;
			job->references = calloc(MAX_OBJECTS, sizeof(*job->references));
			if (!job->references) return 3;
			for (struct traced_process *process = *domain->processes; process; process = process->next) {
				if (process == job->process || process->historical) continue;
				int fds[MAX_HANDLES], count = descriptor_numbers(process->pid, fds, MAX_HANDLES);
				if (count < 0) return 3;
				for (int index = 0; index < count; index++) {
					char name[64]; struct stat state;
					snprintf(name, sizeof(name), "/proc/%ld/fd/%d", (long)process->pid, fds[index]);
					if (stat(name, &state) < 0) return 3;
					if (!S_ISFIFO(state.st_mode) && !S_ISSOCK(state.st_mode)) continue;
					int flags = descriptor_flags(process->pid, fds[index]);
					if (flags < 0 || job->reference_count == MAX_OBJECTS) return 3;
					job->references[job->reference_count].device = state.st_dev;
					job->references[job->reference_count].inode = state.st_ino;
					job->references[job->reference_count++].access = (flags & O_ACCMODE) + 1;
					if (S_ISSOCK(state.st_mode) && queue_references(job, process->pid, fds[index], &state) < 0) return 3;
				}
			}
			job->reference_state = 1;
		}
		struct stat state; int access = 0;
		if (job->reference_state < 0 || fstat(pin, &state) < 0) return 3;
		for (unsigned index = 0; index < job->reference_count; index++)
			if (job->references[index].device == state.st_dev && job->references[index].inode == state.st_ino) access |= job->references[index].access;
		return access;
	}
	if (domain->escaped || domain->uncertain || domain->incomplete) return 3;
	struct resource_object *object = pin_object(domain, pin);
	if (!object || object->foreign || !object->internal || !job->process->table || !job->process->table->complete) return 3;
	int access = 0;
	for (struct traced_process *process = *domain->processes; process; process = process->next) {
		if (process == job->process || process->historical || !process->table) continue;
		if (process->table->active || !process->table->complete) return 3;
		for (unsigned index = 0; index < process->table->count; index++) {
			struct descriptor_origin entry = process->table->entries[index];
			if (entry.object == (unsigned)(object - domain->objects) + 1) access |= entry.access;
		}
	}
	if (!job->reference_state) {
		job->reference_state = -1;
		job->references = calloc(MAX_OBJECTS, sizeof(*job->references));
		if (!job->references) return 3;
		for (unsigned index = 0; index < domain->object_count; index++) {
			const struct resource_object *queue = &domain->objects[index];
			if (!queue->messages) continue;
			int found = 0;
			for (unsigned capture = 0; capture < job->capture_count; capture++)
				if (job->captures[capture].device == (uintmax_t)queue->device && job->captures[capture].inode == (uintmax_t)queue->inode) found = 1;
			for (struct traced_process *process = *domain->processes; !found && process; process = process->next) {
				if (process->historical || !process->table) continue;
				for (unsigned entry = 0; !found && entry < process->table->count; entry++) if (process->table->entries[entry].object == index + 1) {
					struct stat state = {.st_dev = queue->device, .st_ino = queue->inode};
					if (queue_references(job, process->pid, process->table->entries[entry].fd, &state) < 0) return 3;
					found = 1;
				}
			}
			/* An endpoint with no installed handle may still own a cyclic queue.
			 * Only kernel-confirmed destruction can remove it from this proof. */
			struct socket_state state;
			if (!found && socket_state(queue->inode, &state) != 1) return 3;
		}
		job->reference_state = 1;
	}
	if (job->reference_state < 0) return 3;
	for (unsigned index = 0; index < job->reference_count; index++)
		if (job->references[index].device == object->device && job->references[index].inode == object->inode) access |= job->references[index].access;
	return access & 3;
}

static void discard_captures(struct decision_job *job) {
	for (unsigned index = 0; index < job->capture_count; index++) {
		struct file_position *capture = &job->captures[index];
		close(capture->duplicate); free(capture->content); free(capture->messages.entries); free(capture->locks.entries);
	}
	free(job->captures); job->captures = NULL; job->capture_count = 0;
}

/* An observation pin is not an owner. Only a kernel-confirmed shared OFD in the
 * frozen tree grants a keeper; unknown message owners leave the proof incomplete. */
static int outside_description(struct decision_job *job, int pin) {
	struct descriptor_domain *domain = job->domain;
	if (!domain || (domain->enabled && !owned_pin(domain, pin))) return -1;
	unsigned object = domain->enabled ? (unsigned)(pin_object(domain, pin) - domain->objects) + 1 : 0;
	for (struct traced_process *process = *domain->processes; process; process = process->next) {
		if (process == job->process || process->historical) continue;
		int fds[MAX_HANDLES], count;
		if (domain->enabled) {
			if (!process->table || !process->table->complete || process->table->active) return -1;
			count = (int)process->table->count;
		} else count = descriptor_numbers(process->pid, fds, MAX_HANDLES);
		if (count < 0) return -1;
		for (int index = 0; index < count; index++) {
			if (domain->enabled && process->table->entries[index].object != object) continue;
			int fd = domain->enabled ? process->table->entries[index].fd : fds[index];
			long same = syscall(SYS_kcmp, process->pid, getpid(), KCMP_FILE, fd, pin);
			if (same < 0) return -1;
			if (!same) return 1;
		}
	}
	/* Discovery may predict this shape from the stopped tree. Adoption independently
	 * proves closure with syscall tracking, just like queue owner discovery. */
	for (unsigned index = 0; index < domain->object_count; index++) if (domain->objects[index].messages) return -1;
	return 0;
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
	int installed[MAX_HANDLES], total = descriptor_numbers(job->pid, installed, MAX_HANDLES), next = 3;
	if (total < 0 || !(job->captures = calloc(MAX_POSITIONS, sizeof(*job->captures)))) return -1;
	for (int index = 0; index < total; index++) {
		int descriptor = installed[index];
		if (descriptor == INT_MAX) goto fail;
		if (descriptor >= next) next = descriptor + 1;
		int pin = duplicate_tracee_fd(job, (unsigned)descriptor);
		struct stat state;
		if (pin < 0) goto fail;
		if (fstat(pin, &state) < 0) { close(pin); goto fail; }
		struct resource_object *object = pin_object(job->domain, pin);
		unsigned event = !(state.st_mode & S_IFMT) ? event_state(getpid(), pin, NULL, NULL) : 0;
		if (!S_ISREG(state.st_mode) && !S_ISDIR(state.st_mode) &&
			!((null_device(&state) || event || ((S_ISFIFO(state.st_mode) || S_ISSOCK(state.st_mode)) &&
				(!job->domain->enabled || (object && (object->pipe || object->channel))))) && descriptor != 1 && descriptor != 2)) { close(pin); continue; }
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
	size_t used = 0;
	for (unsigned index = 0; index < job->capture_count; index++) {
		struct file_position *capture = &job->captures[index];
		int pin = capture->duplicate, owned = capture->installed ? !job->domain->escaped && !job->domain->uncertain && job->process->table &&
			!job->process->table->active && descriptor_origin(job->process, capture->descriptor).id != 0 : owned_pin(job->domain, pin);
		struct stat state; if (fstat(pin, &state) < 0) goto fail;
		int length = snprintf(line + used, capacity - used,
			"%s{\"fd\":%d,\"alias\":%d,\"device\":\"%ju\",\"inode\":\"%ju\",\"flags\":%d,\"offset\":%s%jd%s,\"owned\":%s%s",
			index ? "," : "", capture->descriptor, capture->alias, capture->device, capture->inode, capture->flags,
			capture->before > 9007199254740991LL ? "\"" : "", (intmax_t)capture->before, capture->before > 9007199254740991LL ? "\"" : "", owned ? "true" : "false",
			capture->event ? ",\"type\":\"eventfd\"" : capture->stream ? (S_ISSOCK(state.st_mode) ? ",\"type\":\"socket\"" : ",\"type\":\"pipe\"") : null_device(&state) ? ",\"type\":\"null\"" : capture->directory ? ",\"type\":\"directory\"" : "");
		if (length < 0 || (size_t)length + 512 >= capacity - used) goto fail;
		used += (size_t)length;
		if (!capture->installed) used += (size_t)sprintf(line + used, ",\"pin\":%d", pin);
		if (S_ISREG(state.st_mode)) {
			if (capture->alias == capture->descriptor) capture->outside = outside_description(job, pin) == 0 ? 0 : 3;
			else for (unsigned previous = 0; previous < index; previous++) if (job->captures[previous].descriptor == capture->alias) capture->outside = job->captures[previous].outside;
			used += (size_t)sprintf(line + used, ",\"outside\":%d", capture->outside);
		}
		if (capture->locks.count) {
			if (capture->locks.count * 128UL + 512 >= capacity - used) goto fail;
			used += (size_t)sprintf(line + used, ",\"locks\":[");
			for (unsigned index = 0; index < capture->locks.count; index++) {
				struct ofd_lock *lock = &capture->locks.entries[index];
				used += (size_t)sprintf(line + used, "%s{\"kind\":%d,\"type\":%d,\"start\":\"%" PRId64 "\",\"length\":\"%" PRId64 "\"}", index ? "," : "", lock->kind, lock->type, lock->start, lock->length);
			}
			line[used++] = ']';
		}
		if (capture->event) {
			uint64_t value; unsigned semaphore;
			if (event_state(getpid(), pin, &value, &semaphore) != capture->event) goto fail;
			used += (size_t)sprintf(line + used, ",\"counter\":{\"id\":%u,\"value\":\"%" PRIu64 "\",\"semaphore\":%u}", capture->event - 1, value, semaphore);
		} else if ((capture->directory && !(capture->flags & O_PATH)) || capture->stream) {
			if ((size_t)capture->content_length * 2 + 512 + capture->messages.count * (64 + MAX_POSITIONS * 16) >= capacity - used) goto fail;
			used += (size_t)sprintf(line + used, ",\"%s\":\"", capture->directory ? "directoryHex" : "queueHex");
			for (int64_t byte = 0; byte < capture->content_length; byte++) {
				line[used++] = "0123456789abcdef"[capture->content[byte] >> 4]; line[used++] = "0123456789abcdef"[capture->content[byte] & 15];
			}
			line[used++] = '"';
			if (capture->stream) used += (size_t)sprintf(line + used, ",\"eof\":%s,\"capacity\":%d,\"outside\":%d", capture->eof ? "true" : "false", capture->capacity, outside_references(job, pin));
			if (capture->messages.count) {
				used += (size_t)sprintf(line + used, ",\"messages\":[");
				for (unsigned message = 0; message < capture->messages.count; message++) {
					const struct queue_message *entry = &capture->messages.entries[message];
					used += (size_t)sprintf(line + used, "%s{\"start\":%zu,\"end\":%zu,\"rights\":[", message ? "," : "", entry->start, entry->end);
					for (unsigned right = 0; right < entry->rights.count; right++) used += (size_t)sprintf(line + used, "%s%d", right ? "," : "", entry->rights.fds[right]);
					used += (size_t)sprintf(line + used, "]}");
				}
				line[used++] = ']';
			}
			if (capture->stream >= 4) used += (size_t)sprintf(line + used, ",\"socket\":{\"shutdown\":%d,\"peerShutdown\":%d,\"peerInode\":%u,\"peerQueued\":%d,\"allocated\":%d,\"type\":%u}",
				capture->shutdown, capture->peer_shutdown, capture->peer_inode, capture->peer_queued, capture->allocated, capture->socket_type);
		}
		line[used++] = '}';
	}
	line[used] = 0;
	return 0;
fail:
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
				unsigned other = 0;
				while (other < job->position_count && job->positions[other].descriptor != expected->rights.fds[right]) other++;
				matches = other < job->position_count && syscall(SYS_kcmp, getpid(), getpid(), KCMP_FILE, actual->rights.fds[right], job->positions[other].duplicate) == 0;
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
static int closed_lock_object(struct decision_job *job, const struct file_position *position) {
	struct descriptor_domain *domain = job->domain;
	struct resource_object *object = pin_object(domain, position->duplicate);
	if (!owned_pin(domain, position->duplicate) || !object->anonymous) return 0;
	/* Queue-owned descriptions must be added to this closure before granting it. */
	for (unsigned index = 0; index < domain->object_count; index++) if (domain->objects[index].messages) return 0;
	for (struct traced_process *process = *domain->processes; process; process = process->next) {
		if (process->historical) continue;
		if (!process->table || !process->table->complete || process->table->active) return 0;
		for (unsigned index = 0; index < process->table->count; index++) {
			struct descriptor_origin origin = process->table->entries[index];
			if (origin.object != (unsigned)(object - domain->objects) + 1) continue;
			unsigned represented = 0;
			while (represented < job->position_count && syscall(SYS_kcmp, process->pid, getpid(), KCMP_FILE, origin.fd, job->positions[represented].duplicate) != 0) represented++;
			if (represented == job->position_count) return 0;
			struct file_locks locks = {0};
			int valid = capture_file_locks(process->pid, origin.fd, &locks) == 0;
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
		if (!complete_view && !closed_lock_object(job, position)) return -1;
		struct stat state; struct statfs filesystem; struct file_locks current = {0};
		if (fstat(position->duplicate, &state) < 0 || !S_ISREG(state.st_mode) || fstatfs(position->duplicate, &filesystem) < 0 ||
			(filesystem.f_type != EXT4_SUPER_MAGIC && filesystem.f_type != TMPFS_MAGIC && filesystem.f_type != RAMFS_MAGIC)) return -1;
		int valid = capture_file_locks(getpid(), position->duplicate, &current) == 0 && current.count == position->locks.count;
		for (unsigned entry = 0; valid && entry < current.count; entry++) valid = !compare_locks(&current.entries[entry], &position->locks.entries[entry]);
		free(current.entries); if (!valid) return -1;
		unsigned previous = 0; while (previous < index && !same_object(position, &job->positions[previous])) previous++;
		int object = previous < index ? fds[previous] : memfd_create("pi-resource-locks", MFD_CLOEXEC);
		if (object < 0) return -1;
		char path[64]; snprintf(path, sizeof(path), "/proc/self/fd/%d", object);
		fds[index] = open(path, (position->flags & (O_ACCMODE | O_PATH)) | O_CLOEXEC);
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
				unsigned other = 0;
				while (other < job->position_count && job->positions[other].descriptor != captured->rights.fds[right]) other++;
				if (other == job->position_count) goto done;
				control->rights[right] = other;
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
				unsigned fd; memcpy(&fd, event->data + 4 + 4 * right, 4); rights[right] = 0;
				while (rights[right] < job->position_count && job->positions[rights[right]].descriptor != (int)fd) rights[right]++;
				if (rights[right] == job->position_count) goto done;
			}
			decoded.kind -= 8; decoded.data += 4 + 4 * right_count; decoded.length -= 4 + 4 * right_count;
		}
		if (moving || copying) {
			unsigned fd;
			if (event->length < 4) goto done;
			memcpy(&fd, event->data, 4);
			for (unsigned other = 0; other < job->position_count; other++) if (job->positions[other].descriptor == (int)fd) source = (int)other;
			if (source < 0) goto done;
			decoded.kind = 2; decoded.data += 4; decoded.length -= 4;
		}
		unsigned handle = 0;
		while (handle < job->position_count && job->positions[handle].descriptor != (int)event->fd) handle++;
		if (handle == job->position_count) goto done;
		const struct file_position *position = &job->positions[handle];
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
				if (event->kind != 4 || event->length != 1 || event->data[0] != 3) goto done;
				if (selected[handle]) {
					if (!apply && outside_description(job, position->duplicate) != (position->outside ? 1 : 0)) goto done;
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
			if (position->event) goto done;
			if (event->length != 1 || !event->data[0] || event->data[0] > 3 ||
				(outside_references(job, position->duplicate) & event->data[0])) goto done;
			for (unsigned cursor = 0; cursor < message_count; cursor++) if (messages[cursor].object != -1)
				for (unsigned right = 0; right < messages[cursor].count; right++) {
					unsigned reference = messages[cursor].rights[right];
					if (queues[reference].owner == object && (((queues[reference].flags & O_ACCMODE) + 1) & event->data[0])) goto done;
				}
			/* The held image exits after commit; replay closes are logical until then.
			 * Snapshot pins must not turn these last-reference observations into live peers. */
			if (position->stream >= 4) {
				queues[object].shutdown = 3;
				if (peer >= 0 && position->socket_type != SOCK_DGRAM) {
					queues[peer].eof = 1; queues[peer].shutdown = 3;
					if (queues[object].length || packet_head) queues[peer].error = ECONNRESET;
				}
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
	struct process_image *current = image_new();
	if (!current) return -1;
	int valid = image_descriptors(job->pid, current) == 0 && job->process->table &&
		job->process->table->references == 1 && !job->process->table->active, input = 0;
	for (unsigned i = 0; valid && i < current->fd_count; i++) {
		if (current->fds[i].fd < 3) continue;
		unsigned p = 0;
		while (p < job->position_count && (!job->positions[p].installed || job->positions[p].descriptor != current->fds[i].fd)) p++;
		valid = p < job->position_count;
	}
	for (unsigned i = 0; valid && i < job->image->fd_count; i++) {
		int source = job->image->fds[i].source, fd = job->image->fds[i].fd;
		unsigned original = 0;
		while (original < current->fd_count && current->fds[original].fd != source) original++;
		if (original == current->fd_count) { valid = 0; break; }
		for (unsigned previous = 0; previous < i; previous++) if (job->image->fds[previous].fd == fd) valid = 0;
		struct file_position *position = NULL;
		for (unsigned p = 0; p < job->position_count; p++) if (job->positions[p].installed && job->positions[p].descriptor == source) position = &job->positions[p];
		if (position) {
			valid &= (job->image->fds[i].flags & ~O_CLOEXEC) == position->after_flags &&
				job->image->fds[i].offset == (position->stream ? 0 : position->after);
			if ((unsigned long)fd == job->image->header.registers.rdi && position->stream) input = 1;
		} else valid &= fd < 3 && source == fd && job->image->fds[i].flags == current->fds[original].flags;
	}
	image_free(current);
	return valid && input && image_status(job->pid, 1) == 0 && image_qualify(job) == 0 ? 0 : -1;
}

static void image_descriptor_table(struct decision_job *job) {
	struct descriptor_table *table = job->process->table;
	struct descriptor_origin final[MAX_HANDLES]; unsigned count = 0;
	for (unsigned i = 0; i < job->image->fd_count; i++) for (unsigned old = 0; old < table->count; old++)
		if (table->entries[old].fd == job->image->fds[i].source) {
			final[count] = table->entries[old]; final[count].fd = job->image->fds[i].fd;
			final[count++].cloexec = !!(job->image->fds[i].flags & O_CLOEXEC); break;
		}
	memcpy(table->entries, final, count * sizeof(*final)); table->count = count;
	job->process->call_epoch = ++table->epoch; table->generation++;
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
		captured = descriptor_context(job, descriptors, MAX_REQUEST_BYTES) == 0;
		pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
	}
	int request_length = snprintf(line, sizeof(line),
		"{\"version\":1,\"token\":\"%s\",\"execution\":\"%s\",\"pid\":%ld,\"tracer\":%ld%s",
		job->token, job->execution_id, (long)job->pid, (long)getpid(),
		captured ? ",\"descriptors\":[" : job->needs_tracking ? ",\"trackQueues\":true" : "");
	int sent = request_length > 0 && request_length < (int)sizeof(line) && transfer(connection, line, (size_t)request_length, 1) == 0 &&
		(!captured || transfer(connection, descriptors, strlen(descriptors), 1) == 0) && transfer(connection, captured ? "]}\n" : "}\n", captured ? 3 : 2, 1) == 0;
	free(descriptors); job->context = NULL;
	if (!sent || read_line(connection, line, sizeof(line)) < 0) return -1;
	if (!strcmp(line, "C")) return -1;
	if (!strcmp(line, "F")) return -2;
	if (!strcmp(line, "O")) return connection;
	if (sscanf(line, "P %u %u %zu %u %u %zu %u %u", &code, &job->count, &total, &job->position_count, &job->resource_count,
		&image_bytes, &physical_length, &source_length) != 8 || code > 256 || (code == 256) != (image_bytes != 0) ||
		image_bytes > IMAGE_BYTES + sizeof(struct image_header) + MAX_LINE || physical_length >= PATH_MAX || source_length >= PATH_MAX ||
		(!image_bytes && (physical_length || source_length)) ||
		job->count > MAX_OUTPUT_EVENTS || total > MAX_OUTPUT_BYTES || job->position_count > MAX_POSITIONS || job->resource_count > 1024) return -1;
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
	job->events = calloc(job->count + job->resource_count + 1, sizeof(*job->events));
	if (!job->events) return -1;
	for (unsigned index = 0; index < job->resource_count + job->count; index++) {
		size_t length, requested = 0;
		unsigned fd, kind = 0;
		if (read_line(connection, line, sizeof(line)) < 0 || (index < job->resource_count
			? sscanf(line, "Q %u %u %zu %zu", &kind, &fd, &length, &requested) != 4 || kind > 13 || length > MAX_INPUT_BYTES || requested > MAX_INPUT_BYTES || (kind == 2 && requested < length)
			: sscanf(line, "O %u %zu", &fd, &length) != 2 || (fd != 1 && fd != 2)) || length > total - received) return -1;
		struct output_event *event = &job->events[index];
		event->fd = fd; event->kind = kind;
		event->length = length; event->requested = requested;
		if (index >= job->resource_count && job->outputs[fd] < 0) {
			/* Publish acquired descriptors before a cancellation point can retire the job. */
			pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
			job->outputs[fd] = open_tracee_output(job, fd);
			pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
			if (job->outputs[fd] < 0) return -1;
		}
		if (length && (!(event->data = malloc(length)) || transfer(connection, event->data, length, 0) < 0)) return -1;
		received += length;
	}
	if (image_bytes) {
		char physical_root[PATH_MAX] = {0}, source_root[PATH_MAX] = {0};
		job->image = image_new();
		if (!job->image || !(job->image->allocation = malloc(image_bytes)) ||
			transfer(connection, physical_root, physical_length, 0) < 0 || transfer(connection, source_root, source_length, 0) < 0 ||
			transfer(connection, job->image->allocation, image_bytes, 0) < 0) return -1;
		if (*physical_root != '/' || *source_root != '/' || strlen(physical_root) != physical_length || strlen(source_root) != source_length ||
			image_load(job, image_bytes, physical_root, source_root) < 0) goto decline;
		received += image_bytes + physical_length + source_length;
	}
	if (received != total) return -1;
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
			char path[64]; struct stat state;
			snprintf(path, sizeof(path), "/proc/self/fd/%d", position->duplicate);
			pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
			position->writer = open(path, O_WRONLY | O_CLOEXEC);
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
	image_free(job->image);
	release_position_fds(job);
	close(job->channel[0]); close(job->channel[1]); close(job->connection); close(job->pidfd);
	for (unsigned fd = 1; fd <= 2; fd++) close(job->outputs[fd]);
	if (job->positions) for (unsigned index = 0; index < job->position_count; index++) {
		free(job->positions[index].content); free(job->positions[index].path); free(job->positions[index].messages.entries); free(job->positions[index].locks.entries);
	}
	free(job->positions);
	free(job->references);
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
	struct descriptor_domain domain = {.enabled = descriptors == 1};
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
				if (code == 257 && job->image && item->armed) {
					int stopped;
					reply = ptrace(PTRACE_SYSCALL, item->pid, 0, 0) == 0 && image_wait(item->pid, &stopped) == 0 &&
						WSTOPSIG(stopped) == (SIGTRAP | 0x80) && image_restore(item->pid, job) == 0 ? 'Y' : 'N';
					if (reply == 'Y') image_descriptor_table(job);
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
					if (ptrace(item->listening ? PTRACE_LISTEN : !domain.enabled || domain.escaped ? PTRACE_CONT : PTRACE_SYSCALL, item->pid, 0, item->delivered) < 0 && errno != ESRCH) goto fatal;
					item->stopped = 0; item->delivered = 0; item->listening = 0;
				}
			}
		}
		pid_t pid = waitpid(-1, &status, __WALL | WNOHANG | __WNOTHREAD);
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
			if (domain.enabled && !domain.escaped) {
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
					if (start_decision(item, socket_path, token, execution_id, NULL) == 0) goto held;
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
static int execute_descriptors(const char *manifest, const char *report, char *executable, char **command, const char *route) {
	struct file_position positions[MAX_POSITIONS];
	struct output_relay relays[2] = {{.source = -1, .writer = -1, .destination = 1}, {.source = -1, .writer = -1, .destination = 2}};
	char line[MAX_LINE];
	unsigned count = 0, close_input = 0, journal = 0, initialized = 0, inherited = 3;
	int result = 70, minimum = 3, output = -1, root_status = -1;
	FILE *input = fopen(manifest, "re");
	if (!input) return result;
	if (!fgets(line, sizeof(line), input) || sscanf(line, "FD6 %u %u %u", &count, &close_input, &journal) != 3 ||
		count > MAX_POSITIONS || close_input > 1 || journal > 1) goto done;
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
	while (fgets(line, sizeof(line), input)) {
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
	if (ferror(input)) goto done;
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
					snprintf(line, sizeof(line), "/proc/self/fd/%d", image->duplicate);
					fd = open(line, position->flags | O_CLOEXEC);
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
	output = open(report, O_WRONLY | O_CLOEXEC | O_NOFOLLOW);
	for (unsigned index = 0; index < count; index++) for (unsigned entry = 0; entry < positions[index].locks.count; entry++)
		if (set_file_lock(positions[index].duplicate, positions[index].locks.entries[entry]) < 0) goto done;
	if (output < 0 || prctl(PR_SET_CHILD_SUBREAPER, 1) < 0) goto done;
	pid_t root = fork();
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
		execv(executable, command);
		_exit(errno == ENOENT ? 127 : 126);
	}
	if (dprintf(output, "RUN1 %d\n", root) < 0) { kill(root, SIGKILL); goto done; }
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
		close(relays[index].writer); relays[index].writer = -1;
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
	for (unsigned index = 0; index < 2; index++) if (relays[index].started) {
		void *failed; int joined = pthread_join(relays[index].thread, &failed); relays[index].started = 0;
		if (joined || failed) goto done;
	}
	if (ftruncate(output, 0) < 0 || lseek(output, 0, SEEK_SET) != 0 || dprintf(output, "FD4 %u\n", count) < 0) goto done;
	for (unsigned index = 0; index < count; index++) {
		struct file_position *position = &positions[index];
		int flags = position->duplicate < 0 ? position->flags : fcntl(position->duplicate, F_GETFL);
		off_t offset;
		if (position->stream && !journal) {
			unsigned char *bytes;
			int eof, remaining = pipe_bytes(position->duplicate, &bytes, &eof);
			const struct file_position *image = &positions[position->object];
			offset = image->content_length - remaining;
			int valid = remaining >= 0 && (position->stream >= 4 || eof == (position->stream == 1)) && offset >= 0 && (!remaining || !memcmp(bytes, image->content + offset, (size_t)remaining));
			free(bytes); if (!valid) goto done;
		} else offset = position->stream ? 0 : position->duplicate < 0 ? position->before : descriptor_seek(position->duplicate, flags, 0, SEEK_CUR);
		if (offset < 0 || flags < 0 || dprintf(output, "%d %d %jd %ju %ju\n",
			position->descriptor, flags, (intmax_t)offset, position->device, position->inode) < 0) goto done;
	}
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
	int dispatched = image_dispatch(argc, argv);
	if (dispatched >= 0) return dispatched;
	if (argc == 2 && !strcmp(argv[1], "--protocol-version")) {
		printf("%u\n", pi_process_image_protocol());
		return 0;
	}
	if (argc >= 2 && (!strcmp(argv[1], "--exec") || !strcmp(argv[1], "--exec-closed-input") || !strcmp(argv[1], "--exec-fds"))) {
		int descriptors = !strcmp(argv[1], "--exec-fds");
		if (argc < (descriptors ? 7 : 5) || strspn(argv[2], "12") != 2 ||
			(strlen(argv[2]) != 2 && (!descriptors || strlen(argv[2]) != 4 || strspn(argv[2] + 2, "ps") != 2 ||
				(argv[2][0] == argv[2][1] && argv[2][2] != argv[2][3])))) return 64;
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
			return execute_descriptors(argv[3], argv[4], executable, argv + 6, argv[2]);
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
