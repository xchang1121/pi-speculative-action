#define _GNU_SOURCE
#include "linux-execution-domain.h"

#define PI_ENTRY_OPERATIONS(X) X(mkdir) X(mknod) X(rmdir) X(unlink) X(symlink)
#define PI_INODE_OPERATIONS(X) X(setattr) X(setxattr) X(removexattr) X(set_acl) X(remove_acl)

#ifdef PI_BPF_OBJECT
#include <linux/bpf.h>
#include <linux/fs.h>
#include <asm/unistd.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_core_read.h>

struct pt_regs { unsigned long orig_ax, di, si, dx, cx, r10, r8; } __attribute__((preserve_access_index));
struct super_block { unsigned int s_dev; unsigned long s_magic; } __attribute__((preserve_access_index));
struct atomic_t { int counter; } __attribute__((preserve_access_index));
struct atomic64_t { long counter; } __attribute__((preserve_access_index));
struct rw_semaphore { struct atomic64_t count; } __attribute__((preserve_access_index));
struct address_space { struct atomic_t i_mmap_writable; } __attribute__((preserve_access_index));
struct inode { unsigned short i_mode; unsigned long i_ino; struct super_block *i_sb;
	struct atomic_t i_writecount, i_dio_count; struct address_space *i_mapping; struct rw_semaphore i_rwsem;
} __attribute__((preserve_access_index));
struct dentry { struct dentry *d_parent; struct inode *d_inode; } __attribute__((preserve_access_index));
struct path { void *mnt; struct dentry *dentry; } __attribute__((preserve_access_index));
struct vfsmount { struct dentry *mnt_root; } __attribute__((preserve_access_index));
struct mount { struct mount *mnt_parent; struct dentry *mnt_mountpoint; struct vfsmount mnt; } __attribute__((preserve_access_index));
struct file { struct path f_path; struct inode *f_inode; void *private_data; unsigned int f_flags; } __attribute__((preserve_access_index));
struct socket { short type; } __attribute__((preserve_access_index));
struct eventfd_ctx { int id; } __attribute__((preserve_access_index));
struct fdtable { unsigned int max_fds; struct file **fd; } __attribute__((preserve_access_index));
struct files_struct { struct fdtable *fdt; } __attribute__((preserve_access_index));
struct vm_area_struct { unsigned long vm_flags; struct file *vm_file; } __attribute__((preserve_access_index));
struct ns_common { unsigned int inum; } __attribute__((preserve_access_index));
struct mnt_namespace { struct ns_common ns; } __attribute__((preserve_access_index));
struct nsproxy { struct mnt_namespace *mnt_ns; } __attribute__((preserve_access_index));
struct task_struct { struct nsproxy *nsproxy; struct files_struct *files; struct task_struct *group_leader, *real_parent; unsigned long long start_boottime; int pid; } __attribute__((preserve_access_index));
struct { __uint(type, BPF_MAP_TYPE_HASH); __uint(max_entries, PI_DOMAIN_LIMIT); __type(key, __u64); __type(value, struct pi_domain_state); } domains SEC(".maps");
struct { __uint(type, BPF_MAP_TYPE_HASH); __uint(max_entries, PI_DOMAIN_LIMIT); __type(key, __u64); __type(value, __u64); } membership SEC(".maps");
/* Sparse evidence tables retain their full capacity without allocating every
 * unused value at startup. Any allocation failure still marks proof loss. */
struct pending_write { struct pi_domain_record objects[8]; unsigned asynchronous, mount_paths; __u64 mount_namespace; };
struct { __uint(type, BPF_MAP_TYPE_HASH); __uint(map_flags, BPF_F_NO_PREALLOC); __uint(max_entries, PI_DOMAIN_RECORDS); __type(key, __u64); __type(value, struct pending_write); } writing SEC(".maps");
struct { __uint(type, BPF_MAP_TYPE_HASH); __uint(map_flags, BPF_F_NO_PREALLOC); __uint(max_entries, PI_DOMAIN_RECORDS); __type(key, struct pi_domain_record); __type(value, struct pi_object_version); } versions SEC(".maps");
struct { __uint(type, BPF_MAP_TYPE_ARRAY); __uint(max_entries, 1); __type(key, __u32); __type(value, struct pi_version_clock); } clock SEC(".maps");

struct descriptor_call { __u64 owner, channel; __u32 number, active, uncertain; };
struct { __uint(type, BPF_MAP_TYPE_HASH); __uint(map_flags, BPF_F_NO_PREALLOC); __uint(max_entries, PI_DOMAIN_RECORDS); __type(key, __u32); __type(value, struct descriptor_call); } descriptor_calls SEC(".maps");
struct { __uint(type, BPF_MAP_TYPE_HASH); __uint(map_flags, BPF_F_NO_PREALLOC); __uint(max_entries, PI_DOMAIN_RECORDS); __type(key, __u64); __type(value, struct pi_domain_file); } file_owners SEC(".maps");
struct { __uint(type, BPF_MAP_TYPE_HASH); __uint(map_flags, BPF_F_NO_PREALLOC); __uint(max_entries, PI_DOMAIN_RECORDS); __type(key, __u64); __type(value, __u64); } anonymous_inodes SEC(".maps");
struct { __uint(type, BPF_MAP_TYPE_ARRAY); __uint(max_entries, 1); __type(key, __u32); __type(value, __u64); } file_sequence SEC(".maps");

static __u64 next_file(void) {
	__u32 zero = 0; __u64 *sequence = bpf_map_lookup_elem(&file_sequence, &zero);
	return sequence ? __sync_fetch_and_add(sequence, 1) + 1 : 0;
}
static struct pi_domain_state *descriptor_state(__u64 *owner) {
	__u64 cgroup = bpf_get_current_cgroup_id(), *value = bpf_map_lookup_elem(&membership, &cgroup);
	if (!value) return 0;
	*owner = *value; return bpf_map_lookup_elem(&domains, value);
}
struct metadata_role { __u64 owner, task, started, broker; };
struct metadata_call { __u64 owner, task, id, arguments[5]; __u32 number, secure; };
struct metadata_notification { __u64 owner, id; };
struct metadata_notification_key { __u64 broker, started, notification; };
struct { __uint(type, BPF_MAP_TYPE_HASH); __uint(map_flags, BPF_F_NO_PREALLOC); __uint(max_entries, PI_DOMAIN_RECORDS); __type(key, __u32); __type(value, struct metadata_role); } metadata_roles SEC(".maps");
struct { __uint(type, BPF_MAP_TYPE_HASH); __uint(map_flags, BPF_F_NO_PREALLOC); __uint(max_entries, PI_DOMAIN_RECORDS); __type(key, __u32); __type(value, struct metadata_call); } metadata_calls SEC(".maps");
struct { __uint(type, BPF_MAP_TYPE_HASH); __uint(map_flags, BPF_F_NO_PREALLOC); __uint(max_entries, PI_DOMAIN_RECORDS); __type(key, struct metadata_notification_key); __type(value, struct metadata_notification); } metadata_notifications SEC(".maps");
struct { __uint(type, BPF_MAP_TYPE_RINGBUF); __uint(max_entries, 1 << 24); } metadata_events SEC(".maps");
struct { __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY); __uint(max_entries, 1); __type(key, __u32); __type(value, struct pi_domain_event); } metadata_scratch SEC(".maps");
struct linux_binprm { struct file *file; } __attribute__((preserve_access_index));

static struct pi_domain_state *metadata_state(__u64 *owner) {
	struct pi_domain_state *state = descriptor_state(owner);
	return state && state->metadata_inode ? state : 0;
}
static __u64 metadata_task(struct pi_domain_state *state) {
	struct bpf_pidns_info process;
	if (bpf_get_ns_current_pid_tgid(state->namespace_device, state->namespace_inode, &process, sizeof(process))) {
		__sync_fetch_and_or(&state->metadata_lost, 1); return 0;
	}
	return ((__u64)process.tgid << 32) | process.pid;
}
static struct pi_domain_event *metadata_begin(__u64 owner, __u64 id, __u64 kind, struct pi_domain_state *state) {
	__u32 zero = 0; struct pi_domain_event *event = bpf_map_lookup_elem(&metadata_scratch, &zero);
	if (!event) { __sync_fetch_and_or(&state->metadata_lost, 2); return 0; }
	event->owner = owner; event->id = id; event->kind = kind; event->time = bpf_ktime_get_ns();
	event->task = metadata_task(state); event->size = 0; event->error = 0; return event;
}
static void metadata_emit(struct pi_domain_event *event, struct pi_domain_state *state) {
	if (event->error) event->size = 0;
	if (event->size > sizeof(event->bytes) || bpf_ringbuf_output(&metadata_events, event, 48 + event->size, 0)) __sync_fetch_and_or(&state->metadata_lost, 4);
	else __sync_fetch_and_add(&state->metadata_records, 1);
}
static int metadata_number(unsigned long number) {
	return number == __NR_stat || number == __NR_lstat || number == __NR_fstat || number == __NR_newfstatat || number == __NR_statx || number == __NR_pread64 || number == __NR_pwrite64;
}
static int metadata_tracee(__u64 owner) {
	struct task_struct *leader = BPF_CORE_READ((struct task_struct *)bpf_get_current_task_btf(), group_leader);
	__u32 pid = BPF_CORE_READ(leader, pid); struct metadata_role *role = bpf_map_lookup_elem(&metadata_roles, &pid);
	return !role || role->owner != owner || role->task != (__u64)leader || role->started != BPF_CORE_READ(leader, start_boottime);
}
static int metadata_enter(struct pt_regs *registers, unsigned secure) {
	unsigned long number = BPF_CORE_READ(registers, orig_ax); if (!metadata_number(number)) return 0;
	__u64 owner = 0; struct pi_domain_state *state = metadata_state(&owner); if (!state || !metadata_tracee(owner)) return 0;
	__u32 pid = (__u32)metadata_task(state); __u64 task = (__u64)bpf_get_current_task_btf();
	struct metadata_call *previous = bpf_map_lookup_elem(&metadata_calls, &pid);
	if (previous && !secure && previous->secure && previous->task == task && previous->number == number) return 0;
	if (previous) __sync_fetch_and_or(&state->metadata_lost, 8);
	struct metadata_call call = {.owner = owner, .task = task, .id = next_file(), .number = number, .secure = secure,
		.arguments = {BPF_CORE_READ(registers, di), BPF_CORE_READ(registers, si), BPF_CORE_READ(registers, dx), BPF_CORE_READ(registers, r10), BPF_CORE_READ(registers, r8)}};
	if (!call.id || bpf_map_update_elem(&metadata_calls, &pid, &call, BPF_ANY)) { __sync_fetch_and_or(&state->metadata_lost, 16); return 0; }
	struct pi_domain_event *event = metadata_begin(owner, call.id, 8, state); if (!event) return 0;
	struct { __u64 reserved, number, arguments[5]; } entry = {.number = number};
	__builtin_memcpy(entry.arguments, call.arguments, sizeof(entry.arguments));
	__builtin_memcpy(event->bytes, &entry, sizeof(entry)); event->size = sizeof(entry); metadata_emit(event, state); return 0;
}
SEC("fentry/__secure_computing") int metadata_secure(unsigned long long *context) {
	(void)context; return metadata_enter((void *)bpf_task_pt_regs(bpf_get_current_task_btf()), 1);
}
SEC("raw_tp/sys_enter") int metadata_ordinary(struct bpf_raw_tracepoint_args *context) { return metadata_enter((void *)context->args[0], 0); }
SEC("raw_tp/sys_exit") int metadata_return(struct bpf_raw_tracepoint_args *context) {
	struct pt_regs *registers = (void *)context->args[0]; unsigned long number = BPF_CORE_READ(registers, orig_ax);
	if (!metadata_number(number)) return 0;
	__u64 owner = 0; struct pi_domain_state *state = metadata_state(&owner); if (!state || !metadata_tracee(owner)) return 0;
	__u32 pid = (__u32)metadata_task(state); struct metadata_call *call = bpf_map_lookup_elem(&metadata_calls, &pid);
	if (!call || call->owner != owner || call->task != (__u64)bpf_get_current_task_btf() || call->number != number) { __sync_fetch_and_or(&state->metadata_lost, 32); return 0; }
	struct pi_domain_event *event = metadata_begin(owner, call->id, 6, state); if (!event) return 0;
	struct returned { __s64 number, result; __u64 address; __u32 size, pad; unsigned char bytes[256]; };
	struct returned *result = (void *)event->bytes;
	result->number = number; result->result = context->args[1]; result->pad = 0;
	result->address = call->arguments[number == __NR_newfstatat ? 2 : number == __NR_statx ? 4 : 1];
	result->size = result->result < 0 || number == __NR_pread64 || number == __NR_pwrite64 ? 0 : number == __NR_statx ? 256 : 144;
	event->size = 32 + result->size;
	if (result->size) event->error = bpf_probe_read_user(result->bytes, result->size, (void *)result->address);
	metadata_emit(event, state); bpf_map_delete_elem(&metadata_calls, &pid); return 0;
}
SEC("fentry/security_inode_getattr") int metadata_descriptor(unsigned long long *context) {
	__u64 owner = 0; struct pi_domain_state *state = metadata_state(&owner); if (!state) return 0;
	__u32 pid = (__u32)metadata_task(state); struct metadata_call *call = bpf_map_lookup_elem(&metadata_calls, &pid);
	if (!call || call->owner != owner || (call->number != __NR_fstat && !(call->number == __NR_newfstatat && (call->arguments[3] & 0x1000)) && !(call->number == __NR_statx && (call->arguments[2] & 0x1000)))) return 0;
	struct pi_domain_event *event = metadata_begin(owner, call->id, 7, state); if (!event) return 0;
	long length = bpf_d_path((struct path *)context[0], (char *)event->bytes, sizeof(event->bytes));
	if (length < 0) event->error = length; else event->size = length;
	metadata_emit(event, state); return 0;
}
SEC("raw_tp/sched_process_exec") int metadata_exec(struct bpf_raw_tracepoint_args *context) {
	__u64 owner = 0; struct pi_domain_state *state = metadata_state(&owner); if (!state) return 0;
	struct task_struct *task = (void *)bpf_get_current_task_btf(); __u32 pid = BPF_CORE_READ(task, pid);
	struct metadata_role *role = bpf_map_lookup_elem(&metadata_roles, &pid);
	if (role && role->owner == owner && !role->task) { role->task = (__u64)task; role->started = BPF_CORE_READ(task, start_boottime); }
	struct task_struct *parent = BPF_CORE_READ(task, real_parent); __u32 parent_pid = BPF_CORE_READ(parent, pid);
	struct metadata_role *launcher = bpf_map_lookup_elem(&metadata_roles, &parent_pid);
	int direct = launcher && launcher->owner == owner && !launcher->broker && launcher->task == (__u64)parent && launcher->started == BPF_CORE_READ(parent, start_boottime);
	int current = role && role->owner == owner && role->task == (__u64)task && role->started == BPF_CORE_READ(task, start_boottime) && !role->broker;
	struct inode *inode = BPF_CORE_READ((struct linux_binprm *)context->args[2], file, f_inode);
	if ((direct || current) && BPF_CORE_READ(inode, i_ino) == state->metadata_inode && BPF_CORE_READ(inode, i_sb, s_dev) == state->metadata_device) {
		struct metadata_role broker = {.owner = owner, .task = (__u64)task, .started = BPF_CORE_READ(task, start_boottime), .broker = 1};
		if (bpf_map_update_elem(&metadata_roles, &pid, &broker, BPF_ANY)) __sync_fetch_and_or(&state->metadata_lost, 64);
	} else if (role && role->broker) bpf_map_delete_elem(&metadata_roles, &pid);
	return 0;
}
SEC("raw_tp/sched_process_exit") int metadata_exit(void *context) {
	(void)context; __u32 pid = (__u32)bpf_get_current_pid_tgid(); bpf_map_delete_elem(&metadata_roles, &pid);
	__u64 owner = 0; struct pi_domain_state *state = metadata_state(&owner);
	if (state) {
		pid = (__u32)metadata_task(state); struct metadata_call *call = bpf_map_lookup_elem(&metadata_calls, &pid);
		if (call && call->owner == owner) {
			struct pi_domain_event *event = metadata_begin(owner, call->id, 9, state); if (event) metadata_emit(event, state);
			bpf_map_delete_elem(&metadata_calls, &pid);
		}
	}
	return 0;
}
struct metadata_input { __u64 owner, id, size; void *data; struct pi_domain_state *state; };
static long metadata_input_frame(__u32 index, void *context) {
	struct metadata_input *input = context; unsigned offset = index * 4096;
	if (offset >= input->size) return 1;
	struct pi_domain_event *event = metadata_begin(input->owner, input->id, (offset ? 11 : 10) | (input->size - offset <= 4096 ? 128 : 0), input->state);
	if (!event) return 1;
	event->size = input->size - offset < 4096 ? input->size - offset : 4096;
	event->error = bpf_probe_read_user(event->bytes, event->size, input->data + offset);
	metadata_emit(event, input->state); return event->error ? 1 : 0;
}
SEC("uprobe") int metadata_broker(struct pt_regs *registers) {
	__u64 owner = 0; struct pi_domain_state *state = metadata_state(&owner); if (!state) return 0;
	struct task_struct *leader = BPF_CORE_READ((struct task_struct *)bpf_get_current_task_btf(), group_leader);
	__u32 pid = BPF_CORE_READ(leader, pid); struct metadata_role *role = bpf_map_lookup_elem(&metadata_roles, &pid);
	if (!role || role->owner != owner || !role->broker || role->task != (__u64)leader || role->started != BPF_CORE_READ(leader, start_boottime)) return 0;
	__u64 notification = BPF_CORE_READ(registers, di), kind = BPF_CORE_READ(registers, si), size = BPF_CORE_READ(registers, cx), id = 0;
	struct metadata_notification_key key = {role->task, role->started, notification};
	void *data = (void *)BPF_CORE_READ(registers, dx);
	if (kind == 10) {
		/* Input aliases belong to this broker's original domain record. Long
		 * symlink walks use bounded frames of the same authenticated journal. */
		id = next_file();
		if (!id || !size || size > 2 * 1024 * 1024) { __sync_fetch_and_or(&state->metadata_lost, 512); return 0; }
		struct metadata_input input = {owner, id, size, data, state};
		if (bpf_loop(512, metadata_input_frame, &input, 0) < 0) __sync_fetch_and_or(&state->metadata_lost, 512);
		return 0;
	}
	if (kind == 0) {
		__u32 target, number;
		if (size != 80 || bpf_probe_read_user(&target, 4, data + 8) || bpf_probe_read_user(&number, 4, data + 16)) { __sync_fetch_and_or(&state->metadata_lost, 128); return 0; }
		if (!metadata_number(number)) return 0;
		struct metadata_call *call = bpf_map_lookup_elem(&metadata_calls, &target); if (!call || call->owner != owner || call->number != number) return 0;
		for (unsigned index = 0; index < 5; index++) {
			__u64 argument;
			if (bpf_probe_read_user(&argument, sizeof(argument), data + 32 + index * 8) || argument != call->arguments[index]) return 0;
		}
		struct metadata_notification entry = {.owner = owner, .id = call->id}; id = call->id;
		if (bpf_map_update_elem(&metadata_notifications, &key, &entry, BPF_ANY)) __sync_fetch_and_or(&state->metadata_lost, 256);
	} else {
		struct metadata_notification *entry = bpf_map_lookup_elem(&metadata_notifications, &key);
		if (!entry || entry->owner != owner) return 0; id = entry->id;
	}
	struct pi_domain_event *event = metadata_begin(owner, id, kind, state); if (!event) return 0;
	if (size > sizeof(event->bytes)) event->error = -7;
	else { event->size = size; event->error = bpf_probe_read_user(event->bytes, size, data); }
	if (kind == 4 || kind == 5) bpf_map_delete_elem(&metadata_notifications, &key);
	metadata_emit(event, state); return 0;
}

static struct file *task_file(unsigned long fd) {
	struct fdtable *table = BPF_CORE_READ((struct task_struct *)bpf_get_current_task_btf(), files, fdt);
	struct file **files = table ? BPF_CORE_READ(table, fd) : 0, *file = 0;
	if (files && fd < BPF_CORE_READ(table, max_fds)) bpf_probe_read_kernel(&file, sizeof(file), files + fd);
	return file;
}
static void descriptor_completed(__u32 task, long result) {
	struct descriptor_call *call = bpf_map_lookup_elem(&descriptor_calls, &task);
	if (!call) return;
	struct pi_domain_state *state = bpf_map_lookup_elem(&domains, &call->owner);
	if (state) {
		if (call->uncertain && result >= 0) __sync_fetch_and_or(&state->descriptor_escaped, 1);
		if (call->active) __sync_fetch_and_sub(&state->descriptor_active, 1);
	}
	bpf_map_delete_elem(&descriptor_calls, &task);
}
SEC("raw_tp/sched_process_exec") int descriptor_exec(struct bpf_raw_tracepoint_args *context) {
	descriptor_completed((__u32)context->args[1], 0); return 0;
}
SEC("fentry/security_task_alloc") int descriptor_clone(unsigned long long *args) {
	__u64 owner = 0; struct pi_domain_state *state = descriptor_state(&owner);
	if (state && (args[1] & 0x00800000)) __sync_fetch_and_or(&state->descriptor_escaped, 1);
	return 0;
}
SEC("raw_tp/sys_enter") int descriptor_enter(struct bpf_raw_tracepoint_args *context) {
	__u32 task = (__u32)bpf_get_current_pid_tgid(); __u64 owner = 0;
	struct pi_domain_state *state = descriptor_state(&owner);
	if (!state || state->descriptor_escaped || state->descriptor_lost) return 0;
	struct pt_regs *registers = (void *)context->args[0]; unsigned long number = context->args[1];
	unsigned long first = BPF_CORE_READ(registers, di), second = BPF_CORE_READ(registers, si), third = BPF_CORE_READ(registers, dx);
	int message = number == __NR_recvmsg || number == __NR_recvmmsg || number == __NR_sendmsg || number == __NR_sendmmsg;
	int escape = number >= 0x40000000 || number == __NR_io_setup || number == __NR_io_submit || number == __NR_io_uring_setup ||
		number == __NR_io_uring_enter || number == __NR_io_uring_register || number == __NR_ptrace || number == __NR_process_vm_writev ||
		number == __NR_splice || number == __NR_tee || (number == __NR_unshare && (first & ~0x400UL)) || number == __NR_setns ||
		(number == __NR_clone && (first & 0x00800000)) || (number == __NR_pipe2 && (second & ~(02000000UL | 04000UL))) ||
		(number == __NR_fcntl && second == 4 && (third & 040000)) || (number == __NR_sendmsg && (third & 1)) ||
		((number == __NR_sendto || number == __NR_sendmmsg) && (BPF_CORE_READ(registers, r10) & 1));
	if (escape) { __sync_fetch_and_or(&state->descriptor_escaped, 1); return 0; }
	__u64 key = (__u64)task_file(first); struct pi_domain_file *channel = message ? bpf_map_lookup_elem(&file_owners, &key) : 0;
	int internal = channel && channel->owner == owner && channel->channel;
	struct descriptor_call call = {.owner = owner, .number = number,
		.channel = number == __NR_socketpair ? next_file() : 0,
		.uncertain = number == __NR_ioctl || number == __NR_connect || number == __NR_bind || (message && !internal) || (number == __NR_sendto && BPF_CORE_READ(registers, r8))};
	call.active = call.uncertain || number == __NR_execve || number == __NR_execveat ||
		number == __NR_close || number == __NR_close_range || number == __NR_dup || number == __NR_dup2 || number == __NR_dup3 ||
		number == __NR_open || number == __NR_openat || number == __NR_openat2 || number == __NR_creat || number == __NR_memfd_create ||
		number == __NR_eventfd || number == __NR_eventfd2 || number == __NR_pipe || number == __NR_pipe2 || number == __NR_socketpair ||
		number == __NR_recvmsg || number == __NR_recvmmsg || number == __NR_pidfd_getfd || number == __NR_unshare ||
		(number == __NR_fcntl && (second == 0 || second == 2 || second == 1030));
	if (!call.active) return 0;
	if (bpf_map_update_elem(&descriptor_calls, &task, &call, BPF_NOEXIST)) __sync_fetch_and_or(&state->descriptor_lost, 1);
	else __sync_fetch_and_add(&state->descriptor_active, 1);
	return 0;
}
SEC("fexit/security_file_alloc") int descriptor_created(unsigned long long *args) {
	if (args[1]) return 0;
	__u32 task = (__u32)bpf_get_current_pid_tgid(); __u64 owner = 0, key = args[0];
	struct pi_domain_state *state = descriptor_state(&owner);
	if (!state) return 0;
	struct descriptor_call *call = bpf_map_lookup_elem(&descriptor_calls, &task);
	struct pi_domain_file file = {.id = next_file(), .owner = owner, .channel = call ? call->channel : 0,
		.attributes = call && call->number == __NR_memfd_create ? 1ULL << 32 : call && (call->number == __NR_eventfd || call->number == __NR_eventfd2) ? 1ULL << 33 : 0};
	if (!file.id || bpf_map_update_elem(&file_owners, &key, &file, BPF_NOEXIST)) __sync_fetch_and_or(&state->descriptor_lost, 1);
	return 0;
}
static void describe_file(struct pi_domain_file *entry, struct file *file) {
	if (!file) return;
	struct inode *inode = BPF_CORE_READ(file, f_inode); unsigned mode = BPF_CORE_READ(inode, i_mode) & 0170000;
	__u64 key = (__u64)inode, *owner = bpf_map_lookup_elem(&anonymous_inodes, &key);
	if (owner && *owner == entry->owner) entry->attributes |= 1ULL << 32;
	void *private = BPF_CORE_READ(file, private_data);
	entry->device = BPF_CORE_READ(inode, i_sb, s_dev); entry->inode = BPF_CORE_READ(inode, i_ino);
	entry->flags = BPF_CORE_READ(file, f_flags);
	entry->attributes = (entry->attributes & ~0xffffffffULL) | mode;
	if (mode == 0140000) entry->attributes |= (__u64)(unsigned short)BPF_CORE_READ((struct socket *)private, type) << 16;
	if (entry->attributes & (1ULL << 33)) entry->event = BPF_CORE_READ((struct eventfd_ctx *)private, id) + 1;
}
SEC("fentry/fd_install") int descriptor_installed(unsigned long long *args) {
	__u64 key = args[1]; struct pi_domain_file *entry = bpf_map_lookup_elem(&file_owners, &key);
	if (entry) {
		if (entry->attributes & (1ULL << 32)) {
			__u64 inode = (__u64)BPF_CORE_READ((struct file *)key, f_inode);
			if (bpf_map_update_elem(&anonymous_inodes, &inode, &entry->owner, BPF_NOEXIST) && !bpf_map_lookup_elem(&anonymous_inodes, &inode)) {
				struct pi_domain_state *state = bpf_map_lookup_elem(&domains, &entry->owner);
				if (state) __sync_fetch_and_or(&state->descriptor_lost, 1);
			}
		}
		describe_file(entry, (void *)key);
	}
	return 0;
}
SEC("fentry/security_inode_free") int anonymous_released(unsigned long long *args) {
	__u64 key = args[0]; bpf_map_delete_elem(&anonymous_inodes, &key); return 0;
}
SEC("fentry/security_file_free") int descriptor_released(unsigned long long *args) {
	__u64 key = args[0]; bpf_map_delete_elem(&file_owners, &key); return 0;
}
struct seq_file;
struct bpf_iter_meta { struct seq_file *seq; __u64 session_id, seq_num; };
struct bpf_iter__task_file { struct bpf_iter_meta *meta; struct task_struct *task; __u32 fd; struct file *file; };
SEC("iter/task_file") int descriptor_query(struct bpf_iter__task_file *context) {
	struct task_struct *task = context->task;
	if (!context->file || !task) return 0;
	__u64 key = (__u64)context->file; struct pi_domain_file *entry = bpf_map_lookup_elem(&file_owners, &key);
	struct pi_domain_file result = entry ? *entry : (struct pi_domain_file){};
	result.descriptor = context->fd; result.task = BPF_CORE_READ(task, pid); describe_file(&result, context->file);
	return bpf_seq_write(context->meta->seq, &result, sizeof(result)) < 0;
}
SEC("iter/task_file") int admission_query(struct bpf_iter__task_file *context) {
	struct file *file = context->file;
	if (!file || !context->task) return 0;
	struct inode *inode = BPF_CORE_READ(file, f_inode);
	struct pi_domain_admission result = {.descriptor = context->fd,
		.device = BPF_CORE_READ(inode, i_sb, s_dev), .inode = BPF_CORE_READ(inode, i_ino),
		.filesystem = BPF_CORE_READ(inode, i_sb, s_magic), .mode = BPF_CORE_READ(inode, i_mode),
		.writers = BPF_CORE_READ(inode, i_writecount.counter), .mappings = BPF_CORE_READ(inode, i_mapping, i_mmap_writable.counter),
		.direct_io = BPF_CORE_READ(inode, i_dio_count.counter), .inode_lock = BPF_CORE_READ(inode, i_rwsem.count.counter)};
	return bpf_seq_write(context->meta->seq, &result, sizeof(result)) < 0;
}

static struct pi_version_clock *version_clock(void) { __u32 zero = 0; return bpf_map_lookup_elem(&clock, &zero); }
static void advance(__u64 *value, __u64 next, struct pi_version_clock *clock) {
	__u64 previous = *value;
	for (unsigned attempt = 0; attempt < 32; attempt++) {
		if (previous >= next) return;
		__u64 actual = __sync_val_compare_and_swap(value, previous, next);
		if (actual == previous) return;
		previous = actual;
	}
	__sync_fetch_and_or(&clock->lost, 2);
}
static struct pending_write *pending_write(__u64 task) {
	struct pending_write empty = {}, *pending = bpf_map_lookup_elem(&writing, &task);
	if (!pending) { bpf_map_update_elem(&writing, &task, &empty, BPF_NOEXIST); pending = bpf_map_lookup_elem(&writing, &task); }
	if (!pending) { struct pi_version_clock *clock = version_clock(); if (clock) __sync_fetch_and_or(&clock->lost, 4); }
	return pending;
}

/* Every physical object has one version, independent of the number of execution
 * domains reading it. In-flight and mapped writes keep that object dirty. */
static __always_inline int changed(struct pi_domain_record key, unsigned uncertain, unsigned long filesystem) {
	if (!key.second) return 0;
	struct pi_version_clock *clock = version_clock(); if (!clock) return 0;
	struct pi_object_version empty = {}, *object = bpf_map_lookup_elem(&versions, &key);
	if (!object) { bpf_map_update_elem(&versions, &key, &empty, BPF_NOEXIST); object = bpf_map_lookup_elem(&versions, &key); }
	if (!object) { __sync_fetch_and_or(&clock->lost, 8); return 0; }
	if (uncertain & 64) {
		/* A released inode number can identify a new object. No old mapping can
		 * survive that lifetime boundary, but outstanding syscall counters drain. */
		if (__sync_lock_test_and_set(&object->uncertain, 0)) __sync_fetch_and_sub(&clock->uncertain, 1);
		object->born = bpf_ktime_get_boot_ns(); object->filesystem = filesystem; uncertain &= ~64U;
	}
	if (object->born) uncertain &= ~32U;
	struct pending_write *pending = pending_write(bpf_get_current_pid_tgid());
	if (!pending) return 0;
	unsigned binding = uncertain & 8; uncertain &= ~8U;
	if (pending->asynchronous) uncertain |= 16;
	if (uncertain && !__sync_fetch_and_or(&object->uncertain, uncertain)) __sync_fetch_and_add(&clock->uncertain, 1);
	unsigned retained = 0;
	for (unsigned index = 0; index < 8; index++) {
		if (pending->objects[index].type == key.type && pending->objects[index].first == key.first && pending->objects[index].second == key.second) { retained = 1; break; }
		if (!pending->objects[index].second) { pending->objects[index] = key; __sync_fetch_and_add(&object->active, 1); __sync_fetch_and_add(&clock->active, 1); retained = 1; break; }
	}
	if (!retained) { __sync_fetch_and_or(&clock->lost, 16); return 0; }
	__u64 next = __sync_fetch_and_add(&clock->version, 1) + 1;
	if (!next) __sync_fetch_and_or(&clock->lost, 64);
	advance(&object->version, next, clock);
	if (binding) advance(&object->binding, next, clock);
	return 0;
}

static int inode_watch(struct inode *inode, unsigned uncertain) {
	unsigned mode = inode ? BPF_CORE_READ(inode, i_mode) & 0170000 : 0;
	return mode == 0100000 || mode == 0040000 || mode == 0120000 ?
		changed((struct pi_domain_record){1, BPF_CORE_READ(inode, i_sb, s_dev), BPF_CORE_READ(inode, i_ino)}, uncertain,
			uncertain & 64 ? BPF_CORE_READ(inode, i_sb, s_magic) : 0) : 0;
}
static int watch(struct dentry *entry, unsigned uncertain) { return entry ? inode_watch(BPF_CORE_READ(entry, d_inode), uncertain) : 0; }
static int file_watch(struct file *file, unsigned uncertain) { return file ? watch(BPF_CORE_READ(file, f_path.dentry), uncertain) : 0; }
static int path_watch(struct path *path) { return path ? watch(BPF_CORE_READ(path, dentry), 0) : 0; }

static void completed(int asynchronous, long result);
static int asynchronous_context(unsigned active) {
	__u64 task = bpf_get_current_pid_tgid();
	struct pending_write *pending = active ? pending_write(task) : bpf_map_lookup_elem(&writing, &task);
	if (pending) pending->asynchronous = active;
	if (!active) completed(1, 0);
	return 0;
}
SEC("fentry/io_issue_sqe") int ring_enter(void *context) { return asynchronous_context(1); }
SEC("fexit/io_issue_sqe") int ring_exit(void *context) { return asynchronous_context(0); }
SEC("fentry/io_submit_one") int aio_enter(void *context) { return asynchronous_context(1); }
SEC("fexit/io_submit_one") int aio_exit(void *context) { return asynchronous_context(0); }
SEC("fentry/security_inode_init_security") int created_inode(unsigned long long *args) { return inode_watch((void *)args[0], 64); }
SEC("fentry/security_file_permission") int permission(unsigned long long *args) {
	__u64 owner = 0; struct pi_domain_state *state = metadata_state(&owner);
	if (state) {
		__u32 pid = (__u32)metadata_task(state); struct metadata_call *call = bpf_map_lookup_elem(&metadata_calls, &pid);
		if (call && call->owner == owner && call->task == (__u64)bpf_get_current_task_btf() && (call->number == __NR_pread64 || call->number == __NR_pwrite64)) {
			struct pi_domain_event *event = metadata_begin(owner, call->id, 7, state);
			if (event) {
				long length = bpf_d_path(&((struct file *)args[0])->f_path, (char *)event->bytes, sizeof(event->bytes));
				if (length < 0) event->error = length; else event->size = length;
				metadata_emit(event, state);
			}
		}
	}
	return args[1] & 2 ? file_watch((void *)args[0], 0) : 0;
}
SEC("fentry/security_file_truncate") int truncate_file(unsigned long long *args) { return file_watch((void *)args[0], 0); }
static int ioctl_watch(unsigned long long *args) {
	unsigned command = args[1];
	return command == 0x5401 || command == 0x5405 || command == 0x5413 || command == 0x541b || command == 0x5450 || command == 0x5451 ? 0 :
		file_watch((void *)args[0], command == FICLONE || command == FICLONERANGE ? 0 : 2);
}
SEC("fentry/security_file_ioctl") int ioctl_file(unsigned long long *args) { return ioctl_watch(args); }
SEC("fentry/security_file_ioctl_compat") int ioctl_compat(unsigned long long *args) { return ioctl_watch(args); }
SEC("fentry/security_mmap_file") int map_file(unsigned long long *args) {
	unsigned count = bpf_get_func_arg_cnt(args); __u64 protection = 0, flags = 0;
	if ((count != 3 && count != 4) || bpf_get_func_arg(args, count - 2, &protection) || bpf_get_func_arg(args, count - 1, &flags)) {
		struct pi_version_clock *clock = version_clock(); if (clock) __sync_fetch_and_or(&clock->lost, 256); return 0;
	}
	return (protection & 2) && (flags & 1) ? file_watch((void *)args[0], 4) : 0;
}
SEC("fentry/security_file_mprotect") int protect_file(unsigned long long *args) { return (args[2] & 2) && (BPF_CORE_READ((struct vm_area_struct *)args[0], vm_flags) & 8) ? file_watch(BPF_CORE_READ((struct vm_area_struct *)args[0], vm_file), 4) : 0; }
/* The preliminary atime predicate can succeed on a read-only mount; the
 * update path runs only after the kernel has acquired mount write access. */
SEC("fentry/inode_update_timestamps") int access_time(unsigned long long *args) { return inode_watch((void *)args[0], 0); }
SEC("kprobe") int entries_watch(struct pt_regs *args) { path_watch((void *)BPF_CORE_READ(args, di)); return watch((void *)BPF_CORE_READ(args, si), 0); }
SEC("fentry/security_path_truncate") int path_truncate(unsigned long long *args) { return path_watch((void *)args[0]); }
SEC("fentry/security_path_chmod") int path_chmod(unsigned long long *args) { return watch(BPF_CORE_READ((struct path *)args[0], dentry), 8); }
SEC("fentry/security_path_chown") int path_chown(unsigned long long *args) { return watch(BPF_CORE_READ((struct path *)args[0], dentry), 8); }
/* Imported names may predate observation and already have a mapped writer. */
SEC("fentry/security_path_rename") int path_rename(unsigned long long *args) { path_watch((void *)args[0]); watch((void *)args[1], 32 | 8); path_watch((void *)args[2]); return watch((void *)args[3], 8); }
SEC("fentry/security_path_link") int path_link(unsigned long long *args) { watch((void *)args[0], 32); return path_watch((void *)args[1]); }
SEC("kprobe") int inodes_watch(struct pt_regs *args) { return watch((void *)BPF_CORE_READ(args, si), 8); }
struct mount_change { struct pi_domain_inode nodes[64]; __u64 namespace; unsigned count, unknown; };
struct { __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY); __uint(max_entries, 1); __type(key, __u32); __type(value, struct mount_change); } mounts SEC(".maps");
struct mount_match { struct pi_domain_state *state; struct mount_change *change; };
static long match_mount(unsigned long index, struct mount_match *match) {
	struct pi_domain_state *state = match->state; struct mount_change *change = match->change;
	if (index >= 64 || (index >= change->count && index >= state->ancestor_count)) return 1;
	unsigned matched = 0;
	for (unsigned root = 0; root < PI_DOMAIN_ROOTS; root++) {
		if (root >= state->root_count) break;
		if (index < change->count && state->roots[root].device == change->nodes[index].device && state->roots[root].inode == change->nodes[index].inode) matched = 1;
	}
	if (index < state->ancestor_count && change->count && state->ancestors[index].device == change->nodes[0].device && state->ancestors[index].inode == change->nodes[0].inode) matched = 1;
	if (matched) __sync_fetch_and_or(&state->uncertain, 128);
	return matched;
}
static long invalidate_mount(void *map, __u64 *id, struct pi_domain_state *state, void *context) {
	__u32 zero = 0; struct mount_change *change = bpf_map_lookup_elem(&mounts, &zero);
	if (!change || state->mount_namespace != change->namespace) return 0;
	if (change->unknown) __sync_fetch_and_or(&state->uncertain, 128);
	else { struct mount_match match = {state, change}; bpf_loop(64, match_mount, &match, 0); }
	return 0;
}
static void mount_binding(struct dentry *entry) {
	struct inode *inode = BPF_CORE_READ(entry, d_inode);
	__u64 namespace = BPF_CORE_READ((struct task_struct *)bpf_get_current_task_btf(), nsproxy, mnt_ns, ns.inum);
	changed((struct pi_domain_record){(namespace << 32) | 3, BPF_CORE_READ(inode, i_sb, s_dev), BPF_CORE_READ(inode, i_ino)}, 8, 0);
}
static int mount_watch(struct dentry *entry) {
	mount_binding(entry);
	__u32 zero = 0; struct mount_change *change = bpf_map_lookup_elem(&mounts, &zero);
	if (!change) return 0;
	change->namespace = BPF_CORE_READ((struct task_struct *)bpf_get_current_task_btf(), nsproxy, mnt_ns, ns.inum);
	change->count = 0; change->unknown = 1;
	for (unsigned index = 0; index < 64; index++) {
		if (!entry) break;
		struct inode *inode = BPF_CORE_READ(entry, d_inode);
		change->nodes[index] = (struct pi_domain_inode){ BPF_CORE_READ(inode, i_sb, s_dev), BPF_CORE_READ(inode, i_ino) };
		change->count++;
		struct dentry *parent = BPF_CORE_READ(entry, d_parent);
		if (parent == entry) { change->unknown = 0; break; }
		entry = parent;
	}
	bpf_for_each_map_elem(&domains, invalidate_mount, NULL, 0); return 0;
}
SEC("fentry/security_sb_mount") int mount_path(unsigned long long *args) { return mount_watch(BPF_CORE_READ((struct path *)args[1], dentry)); }
static int mounted_watch(struct vfsmount *vfsmount) {
	if (!vfsmount) return 0;
	struct mount *mount = (void *)vfsmount - bpf_core_field_offset(struct mount, mnt);
	mount_binding(BPF_CORE_READ(vfsmount, mnt_root));
	return mount_watch(BPF_CORE_READ(mount, mnt_mountpoint));
}
SEC("fentry/security_sb_umount") int unmount_path(unsigned long long *args) { return mounted_watch((void *)args[0]); }
SEC("fentry/security_move_mount") int move_mount_path(unsigned long long *args) { mount_watch(BPF_CORE_READ((struct path *)args[0], dentry)); return mount_watch(BPF_CORE_READ((struct path *)args[1], dentry)); }
SEC("raw_tp/sys_enter") int namespace_change(struct bpf_raw_tracepoint_args *context) {
	unsigned long call = context->args[1];
	if (call != __NR_mount_setattr && call != __NR_pivot_root) return 0;
	struct pending_write *pending = pending_write(bpf_get_current_pid_tgid());
	struct pi_version_clock *clock = version_clock();
	if (!pending || !clock) return 0;
	pending->mount_namespace = BPF_CORE_READ((struct task_struct *)bpf_get_current_task_btf(), nsproxy, mnt_ns, ns.inum);
	struct pi_domain_record key = {2, 0, pending->mount_namespace}; struct pi_object_version empty = {};
	struct pi_object_version *object = bpf_map_lookup_elem(&versions, &key);
	if (!object) { bpf_map_update_elem(&versions, &key, &empty, BPF_NOEXIST); object = bpf_map_lookup_elem(&versions, &key); }
	if (!object) { __sync_fetch_and_or(&clock->lost, 128); return 0; }
	__sync_fetch_and_add(&object->active, 1); __sync_fetch_and_add(&clock->active, 1); __sync_fetch_and_add(&clock->version, 1);
	return 0;
}
SEC("fexit/user_path_at") int resolved_mount_path(unsigned long long *args) {
	__u64 task = bpf_get_current_pid_tgid(); struct pending_write *pending = bpf_map_lookup_elem(&writing, &task);
	if (pending && pending->mount_namespace && !args[4]) {
		pending->mount_paths++; return mount_watch(BPF_CORE_READ((struct path *)args[3], dentry));
	}
	return 0;
}

static void completed(int asynchronous, long result) {
	__u64 task = bpf_get_current_pid_tgid();
	struct pending_write *pending = bpf_map_lookup_elem(&writing, &task);
	struct pi_version_clock *clock = version_clock();
	if (pending && clock) {
		if (pending->mount_namespace) {
			struct pi_domain_record key = {2, 0, pending->mount_namespace};
			struct pi_object_version *object = bpf_map_lookup_elem(&versions, &key);
			__u64 next = __sync_fetch_and_add(&clock->version, 1) + 1;
			if (object) {
				if (result >= 0 && !pending->mount_paths) advance(&object->version, next, clock);
				__sync_fetch_and_sub(&object->active, 1); __sync_fetch_and_sub(&clock->active, 1);
			} else __sync_fetch_and_or(&clock->lost, 128);
			pending->mount_namespace = 0;
		}
		unsigned remaining = 0;
		for (unsigned index = 0; index < 8; index++) {
			struct pi_object_version *object = bpf_map_lookup_elem(&versions, &pending->objects[index]);
			if (object && pending->objects[index].second) {
				if (asynchronous && !(object->uncertain & 16)) { remaining++; continue; }
				advance(&object->version, __sync_fetch_and_add(&clock->version, 1) + 1, clock);
				__sync_fetch_and_sub(&object->active, 1); __sync_fetch_and_sub(&clock->active, 1);
				pending->objects[index] = (struct pi_domain_record){};
			}
		}
		if (!remaining) bpf_map_delete_elem(&writing, &task);
	}
}
SEC("raw_tp/sched_process_exit") int task_exit(void *context) {
	completed(0, -1); descriptor_completed((__u32)bpf_get_current_pid_tgid(), -1); return 0;
}
SEC("raw_tp/sys_exit") int observe(struct bpf_raw_tracepoint_args *context) {
	completed(0, (long)context->args[1]); descriptor_completed((__u32)bpf_get_current_pid_tgid(), (long)context->args[1]);
	unsigned long call = BPF_CORE_READ((struct pt_regs *)context->args[0], orig_ax);
	if (call != __NR_getpid && call != __NR_getppid && call != __NR_getsid && call != __NR_getpgid) return 0;
	__u64 owner = 0; struct pi_domain_state *state = metadata_state(&owner);
	if (!state || !metadata_tracee(owner)) return 0;
	struct pi_domain_event *event = metadata_begin(owner, next_file(), 12, state); if (!event) return 0;
	struct { __u64 number; __s64 result; } result = {call, context->args[1]};
	__builtin_memcpy(event->bytes, &result, sizeof(result)); event->size = sizeof(result); metadata_emit(event, state);
	return 0;
}
char LICENSE[] SEC("license") = "GPL";

#else
#include <bpf/bpf.h>
#include <bpf/libbpf.h>
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <limits.h>
#include <poll.h>
#include <pwd.h>
#include <signal.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/prctl.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>
#include <time.h>
#include <linux/magic.h>

#ifndef PI_DOMAIN_BUILD
#error PI_DOMAIN_BUILD must identify both collector sources and the protocol
#endif

struct metadata_binary { struct bpf_link *link; uint64_t device, inode; unsigned references; int file; };
struct domain {
	uint64_t id, cgroup, watched; pid_t owner; int observing, trace;
	unsigned char *events; size_t event_bytes; uint64_t event_count, event_lost;
	struct metadata_binary *broker; char path[PATH_MAX];
};
static struct domain groups[PI_DOMAIN_LIMIT];
static struct metadata_binary metadata_binaries[PI_DOMAIN_LIMIT];
static int domain_map = -1, membership_map = -1, version_map = -1, clock_map = -1, file_map = -1, role_map = -1, metadata_call_map = -1, metadata_notification_map = -1;
static uint64_t domain_sequence; static size_t trace_bytes;
static struct bpf_program *file_query, *broker_query, *input_query;
static char group_root[128];
static volatile sig_atomic_t stopped;
static void stop(int signal) { stopped = signal; }

static int write_value(const char *directory, const char *name, const char *value) {
	char path[PATH_MAX];
	if (snprintf(path, sizeof(path), "%s/%s", directory, name) >= (int)sizeof(path)) return EINVAL;
	int file = open(path, O_WRONLY | O_CLOEXEC);
	if (file < 0) return errno;
	ssize_t length = (ssize_t)strlen(value), written;
	do { written = write(file, value, (size_t)length); } while (written < 0 && errno == EINTR);
	int error = written == length ? 0 : errno ? errno : EIO;
	close(file); return error;
}

static int descendant(pid_t pid, pid_t owner) {
	for (unsigned depth = 0; depth < 256 && pid > 1; depth++) {
		if (pid == owner) return 1;
		char path[64], line[256]; snprintf(path, sizeof(path), "/proc/%d/status", pid);
		FILE *file = fopen(path, "re"); if (!file) return 0;
		int parent = 0;
		while (fgets(line, sizeof(line), file)) if (sscanf(line, "PPid:%d", &parent) == 1) break;
		fclose(file); if (parent == pid) return 0; pid = parent;
	}
	return 0;
}

static int producing(pid_t pid) {
	char path[64], line[PATH_MAX + 64]; snprintf(path, sizeof(path), "/proc/%d/cgroup", pid);
	FILE *file = fopen(path, "re"); if (!file) return -1;
	int answer = -1;
	while (fgets(line, sizeof(line), file)) if (!strncmp(line, "0::", 3)) {
		const char *root = group_root + strlen("/sys/fs/cgroup");
		answer = !strncmp(line + 3, root, strlen(root)) && line[3 + strlen(root)] == '/'; break;
	}
	fclose(file); return answer;
}

static int remove_group(const char *path, const char *destination) {
	for (unsigned attempt = 0;; attempt++) {
		if (destination) {
			char members[PATH_MAX + 16], pid[32]; snprintf(members, sizeof(members), "%s/cgroup.procs", path);
			FILE *tasks = fopen(members, "re"); if (!tasks) return errno == ENOENT ? 0 : errno;
			int error = 0;
			while (!error && fgets(pid, sizeof(pid), tasks)) {
				error = write_value(destination, "cgroup.procs", pid); if (error == ESRCH) error = 0;
			}
			if (!error && ferror(tasks)) error = EIO;
			fclose(tasks); if (error) return error;
		}
		if (!rmdir(path) || errno == ENOENT) return 0;
		if (errno != EBUSY || attempt == 100) return errno;
		usleep(1000);
	}
}

static void release_metadata_binary(struct metadata_binary *binary) {
	if (binary->link) { bpf_link__destroy(binary->link); close(binary->file); }
	memset(binary, 0, sizeof(*binary));
}

static int retire(struct domain *group) {
	if (!group->id) return 0;
	if (bpf_map_delete_elem(domain_map, &group->id) && errno != ENOENT) return errno;
	if (bpf_map_delete_elem(membership_map, &group->cgroup) && errno != ENOENT) return errno;
	/* The observer borrows the Actor tree. Returning it to the application's
	 * parent domain must not cancel a native background process. */
	int error = write_value(group->path, group->observing ? "cgroup.freeze" : "cgroup.kill", "1");
	if (error && error != ENOENT) return error;
	if ((error = remove_group(group->path, group->observing ? group_root : NULL))) {
		if (group->observing) (void)write_value(group->path, "cgroup.freeze", "0");
		return error;
	}
	/* Pending notifications can outlive a killed task; they never outlive their
	 * execution owner. Both map values begin with that owner's unique identity. */
	for (unsigned map = 0; map < 2; map++) {
		int file = map ? metadata_notification_map : metadata_call_map, have = 0;
		uint64_t key[3], next[3], value[16]; size_t key_size = map ? 24 : 4;
		for (unsigned count = 0; count < PI_DOMAIN_RECORDS; count++) {
			if (bpf_map_get_next_key(file, have ? key : NULL, next)) break;
			if (!bpf_map_lookup_elem(file, next, value) && value[0] == group->id) bpf_map_delete_elem(file, next);
			else { memcpy(key, next, key_size); have = 1; }
		}
	}
	trace_bytes -= group->event_bytes;
	if (group->events) munmap(group->events, PI_DOMAIN_TRACE_BYTES);
	if (group->trace >= 0) close(group->trace);
	if (group->broker) group->broker->references--;
	memset(group, 0, sizeof(*group)); return 0;
}

static int metadata_record(void *context, void *data, size_t bytes) {
	(void)context; struct pi_domain_event *event = data;
	if (bytes < 48 || bytes != 48 + event->size || event->size > sizeof(event->bytes)) return -EPROTO;
	for (unsigned index = 0; index < PI_DOMAIN_LIMIT; index++) {
		struct domain *group = &groups[index]; if (group->id != event->owner) continue;
		size_t length = (bytes + 7) & ~(size_t)7;
		if (!group->events || length > PI_DOMAIN_TRACE_BYTES - trace_bytes) { group->event_lost = 1; return 0; }
		memcpy(group->events + group->event_bytes, event, bytes); group->event_bytes += length; trace_bytes += length; group->event_count++; return 0;
	}
	return 0;
}

static int metadata_open(struct domain *group, const char *name, struct pi_domain_state *state) {
	if (group->events) return EALREADY;
	int file = open(name, O_PATH | O_CLOEXEC), error = 0, writable = -1; struct stat info;
	if (file < 0) return errno;
	if (fstat(file, &info) || !S_ISREG(info.st_mode)) { error = errno ? errno : EINVAL; goto done; }
	for (unsigned index = 0; index < PI_DOMAIN_LIMIT; index++) if (metadata_binaries[index].link && metadata_binaries[index].device == info.st_dev && metadata_binaries[index].inode == info.st_ino) { group->broker = &metadata_binaries[index]; break; }
	char descriptor[64];
	if (!group->broker) {
		/* The collector owns probes; executions only borrow them. Reuse a vacant
		 * slot or retire an idle binary when the bounded table is full. */
		for (unsigned index = 0; index < PI_DOMAIN_LIMIT; index++) {
			struct metadata_binary *binary = &metadata_binaries[index];
			if (!binary->link) { group->broker = binary; break; }
			if (!group->broker && !binary->references) group->broker = binary;
		}
		if (!group->broker) { error = ENOSPC; goto done; }
		release_metadata_binary(group->broker);
		snprintf(descriptor, sizeof(descriptor), "/proc/self/fd/%d", file);
		LIBBPF_OPTS(bpf_uprobe_opts, options, .func_name = "pi_observe_record");
		struct bpf_link *link = bpf_program__attach_uprobe_opts(broker_query, -1, descriptor, 0, &options);
		if (libbpf_get_error(link)) { group->broker = NULL; error = EOPNOTSUPP; goto done; }
		*group->broker = (struct metadata_binary){.link = link, .device = info.st_dev, .inode = info.st_ino, .file = file}; file = -1;
	}
	group->broker->references++;
	writable = memfd_create("pi-execution-records", MFD_CLOEXEC | MFD_ALLOW_SEALING);
	if (writable < 0 || ftruncate(writable, PI_DOMAIN_TRACE_BYTES)) { error = errno; goto done; }
	void *mapped = mmap(NULL, PI_DOMAIN_TRACE_BYTES, PROT_READ | PROT_WRITE, MAP_SHARED, writable, 0);
	if (mapped == MAP_FAILED) { error = errno; goto done; } group->events = mapped;
	if (fcntl(writable, F_ADD_SEALS, F_SEAL_SEAL | F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_FUTURE_WRITE)) { error = errno; goto done; }
	snprintf(descriptor, sizeof(descriptor), "/proc/self/fd/%d", writable);
	group->trace = open(descriptor, O_RDONLY | O_CLOEXEC); if (group->trace < 0) { error = errno; goto done; }
	state->metadata_device = ((uint64_t)major(info.st_dev) << 20) | minor(info.st_dev); state->metadata_inode = info.st_ino;
	if (bpf_map_update_elem(domain_map, &group->id, state, BPF_EXIST)) error = errno;
done:
	if (file >= 0) close(file);
	if (writable >= 0) close(writable);
	if (error) {
		if (group->trace >= 0) { close(group->trace); group->trace = -1; }
		if (group->events) { munmap(group->events, PI_DOMAIN_TRACE_BYTES); group->events = NULL; }
		if (group->broker) group->broker->references--;
		group->broker = NULL;
	}
	return error;
}

static int iterate_files(unsigned target, struct bpf_program *query, void *record, size_t size,
	int (*visit)(void *, void *), void *context) {
	union bpf_iter_link_info info = {.task.tid = target};
	LIBBPF_OPTS(bpf_iter_attach_opts, options, .link_info = &info, .link_info_len = sizeof(info));
	struct bpf_link *link = bpf_program__attach_iter(query, &options);
	int error = (int)-libbpf_get_error(link); if (error) return error;
	int iterator = bpf_iter_create(bpf_link__fd(link));
	if (iterator < 0) error = errno;
	for (unsigned count = 0; !error && count < PI_DOMAIN_RECORDS; count++) {
		ssize_t bytes;
		do { bytes = read(iterator, record, size); } while (bytes < 0 && errno == EINTR);
		if (!bytes) break;
		if (bytes != (ssize_t)size) { error = bytes < 0 ? errno : EPROTO; break; }
		int result = visit(record, context);
		if (result) { error = result < 0 ? -result : 0; break; }
		if (count + 1 == PI_DOMAIN_RECORDS) error = EOVERFLOW;
	}
	if (iterator >= 0) close(iterator);
	bpf_link__destroy(link);
	return error;
}
struct file_inspection { unsigned target, *task; int descriptor; struct pi_domain_packet *message; };
static int inspected_file(void *record, void *context) {
	struct pi_domain_file *current = record; struct file_inspection *inspection = context;
	struct pi_domain_packet *message = inspection->message;
	if (inspection->task) { *inspection->task = current->task; return 1; }
	if (inspection->descriptor >= 0 && current->descriptor != (unsigned)inspection->descriptor) return 0;
	if (message->count + sizeof(*current) / 8 > PI_DOMAIN_VALUES) return -EOVERFLOW;
	current->device = makedev(current->device >> 20, current->device & 0xfffff); current->task = inspection->target;
	memcpy(message->values + message->count, current, sizeof(*current)); message->count += sizeof(*current) / 8;
	return inspection->descriptor >= 0;
}
static int inspect_files(unsigned target, int descriptor, struct pi_domain_packet *message, unsigned *task) {
	struct pi_domain_file current; struct file_inspection inspection = {target, task, descriptor, message}; message->count = 2;
	int error = iterate_files(target, file_query, &current, sizeof(current), inspected_file, &inspection);
	return error ? error : descriptor >= 0 && message->count == 2 ? ENOENT : 0;
}
struct input_inspection { struct pi_domain_packet *message; unsigned found; };
static int inspected_input(void *record, void *context) {
	struct pi_domain_admission *current = record; struct input_inspection *inspection = context;
	struct pi_domain_packet *message = inspection->message;
	for (unsigned index = 0; index < message->count; index += 3) {
		if (message->values[index] != current->descriptor) continue;
		dev_t device = makedev(current->device >> 20, current->device & 0xfffff);
		if (message->values[index + 1] != device || message->values[index + 2] != current->inode ||
			(current->filesystem != EXT4_SUPER_MAGIC && current->filesystem != TMPFS_MAGIC) ||
			((current->mode & 0170000) == 0100000 && (current->writers > 0 || current->mappings || current->direct_io)) ||
			(current->inode_lock & 1)) return -EAGAIN;
		struct pi_domain_record key = {1, current->device, current->inode}; struct pi_object_version object = {0};
		if (bpf_map_lookup_elem(version_map, &key, &object) && errno != ENOENT) return -errno;
		/* These flags describe possible writers, not permanent taint. The current
		 * census closes them without erasing the event that invalidates older views. */
		if (object.active || (object.uncertain & ~(4U | 16U | 32U))) return -EAGAIN;
		inspection->found++;
	}
	return inspection->found == message->count / 3;
}

static int respond(struct pi_domain_packet *message, const struct ucred *peer, pid_t application, int *descriptor, struct ring_buffer *events) {
	if (message->count > PI_DOMAIN_VALUES || !descendant(peer->pid, application)) return EACCES;
	int active = producing(peer->pid);
	if (active < 0 || (active && message->operation != PI_DOMAIN_JOIN)) return EACCES;
	struct domain *group = NULL;
	for (unsigned index = 0; index < PI_DOMAIN_LIMIT; index++) if (groups[index].id == message->domain && message->domain) group = &groups[index];
	if (message->operation == PI_DOMAIN_HELLO) {
		if (message->count || message->domain) return EINVAL;
		message->domain = PI_DOMAIN_ABI; message->count = 8;
		memcpy(message->values, PI_DOMAIN_BUILD, 64); return 0;
	}
	if (message->operation == PI_DOMAIN_OPEN) {
		if (message->count || message->domain) return EINVAL;
		if (domain_sequence == UINT64_MAX) return EOVERFLOW;
		for (unsigned index = 0; index < PI_DOMAIN_LIMIT; index++) if (!groups[index].id) { group = &groups[index]; break; }
		if (!group) return ENOSPC;
		group->trace = -1;
		snprintf(group->path, sizeof(group->path), "%s/domain-%u", group_root, (unsigned)(group - groups));
		struct stat info;
		if (mkdir(group->path, 0755)) return errno;
		if (stat(group->path, &info)) { int error = errno; rmdir(group->path); return error; }
		/* Kernel object numbers can be recycled. Retained descriptions and tasks
		 * name this non-repeating owner, including after its cgroup has gone. */
		group->id = ++domain_sequence; group->cgroup = info.st_ino; group->owner = peer->pid;
		struct stat namespace;
		if (stat("/proc/self/ns/pid", &namespace)) { int error = errno; retire(group); return error; }
		struct pi_domain_state state = {.namespace_device = namespace.st_dev, .namespace_inode = namespace.st_ino};
		char mount_namespace[64]; snprintf(mount_namespace, sizeof(mount_namespace), "/proc/%d/ns/mnt", peer->pid);
		if (stat(mount_namespace, &namespace)) { int error = errno; retire(group); return error; }
		state.mount_namespace = namespace.st_ino;
		if (bpf_map_update_elem(domain_map, &group->id, &state, BPF_NOEXIST)) { int error = errno; retire(group); return error; }
		if (bpf_map_update_elem(membership_map, &group->cgroup, &group->id, BPF_NOEXIST)) { int error = errno; retire(group); return error; }
		message->domain = group->id; return 0;
	}
	if (!group || (message->operation != PI_DOMAIN_JOIN && group->owner != peer->pid)) return EACCES;
	if (message->operation == PI_DOMAIN_JOIN) {
		/* A producing task cannot migrate itself out of an already observed domain. */
		if (active || message->count || !descendant(peer->pid, group->owner) || peer->pid == group->owner) return EACCES;
		if (group->events) {
			struct { uint64_t owner, task, started, broker; } role = {.owner = group->id};
			unsigned target = 0; int error = inspect_files(peer->pid, -1, message, &target); message->count = 0;
			if (error || !target) return error ? error : ESRCH;
			if (bpf_map_update_elem(role_map, &target, &role, BPF_NOEXIST)) return errno;
		}
		char pid[32]; snprintf(pid, sizeof(pid), "%d", peer->pid);
		return write_value(group->path, "cgroup.procs", pid);
	}
	if (message->operation == PI_DOMAIN_CLOSE) return message->count ? EINVAL : retire(group);
	struct pi_domain_state state;
	if (bpf_map_lookup_elem(domain_map, &group->id, &state)) return errno;
	if (message->operation == PI_DOMAIN_TRACE) {
		if (!message->count || message->count * 8 > PATH_MAX || strnlen((char *)message->values, message->count * 8) == message->count * 8) return EINVAL;
		int error = metadata_open(group, (char *)message->values, &state); if (error) return error;
		*descriptor = group->trace; message->count = 1; message->values[0] = 0; return 0;
	}
	if (message->operation == PI_DOMAIN_EVENTS) {
		if (message->count || !group->events) return EINVAL;
		/* The sampled production count precedes this drain. Concurrent children may
		 * append more, but cannot make the sampled prefix incomplete. */
		for (unsigned attempt = 0; group->event_count < state.metadata_records && attempt < 8; attempt++)
			if (ring_buffer__consume(events) < 0) return EIO;
		if (group->event_count < state.metadata_records) return EAGAIN;
		message->count = 4; message->values[0] = group->event_bytes; message->values[1] = state.metadata_records;
		message->values[2] = state.metadata_lost | group->event_lost; message->values[3] = group->event_count; return 0;
	}
	if (message->operation == PI_DOMAIN_TRACK) {
		if (message->count != 1 || !message->values[0] || message->values[0] > INT_MAX || group->observing) return EINVAL;
		unsigned target = message->values[0]; char path[64], line[256]; int tracer = 0;
		snprintf(path, sizeof(path), "/proc/%u/status", target); FILE *status = fopen(path, "re");
		if (!status) return errno;
		while (fgets(line, sizeof(line), status)) if (sscanf(line, "TracerPid:%d", &tracer) == 1) break;
		fclose(status);
		if (tracer != peer->pid || !descendant(target, application) || producing(target) != 0) return EACCES;
		snprintf(line, sizeof(line), "%u", target); group->observing = 1;
		int error = write_value(group->path, "cgroup.procs", line); if (error) return error;
		message->count = 0; return 0;
	}
	if (message->operation == PI_DOMAIN_FILE) {
		if (message->count != 2 || !message->values[0] || message->values[0] > INT_MAX || (message->values[1] > INT_MAX && message->values[1] != ULLONG_MAX)) return EINVAL;
		unsigned target = message->values[0]; int descriptor = message->values[1] == ULLONG_MAX ? -1 : (int)message->values[1];
		if (target != (unsigned)peer->pid && !descendant(target, application)) return EACCES;
		int error = inspect_files(target, descriptor, message, NULL); if (error) return error;
		if (bpf_map_lookup_elem(domain_map, &group->id, &state)) return errno;
		message->values[0] = state.descriptor_lost | state.descriptor_escaped; message->values[1] = state.descriptor_active;
		return 0;
	}
	if (message->operation == PI_DOMAIN_QUEUES) {
		if (message->count) return EINVAL;
		uint64_t key = 0, next; int have = 0; message->count = 2;
		while (!bpf_map_get_next_key(file_map, have ? &key : NULL, &next)) {
			struct pi_domain_file file;
			if (!bpf_map_lookup_elem(file_map, &next, &file) && file.owner == group->id && file.channel && (file.attributes & 0170000) == 0140000) {
				if (message->count + 2 > PI_DOMAIN_VALUES) return EOVERFLOW;
				message->values[message->count++] = makedev(file.device >> 20, file.device & 0xfffff);
				message->values[message->count++] = file.inode;
			}
			key = next; have = 1;
		}
		if (errno != ENOENT || bpf_map_lookup_elem(domain_map, &group->id, &state)) return errno;
		message->values[0] = state.descriptor_lost | state.descriptor_escaped; message->values[1] = state.descriptor_active; return 0;
	}
	unsigned zero = 0; struct pi_version_clock clock;
	if (bpf_map_lookup_elem(clock_map, &zero, &clock)) return errno;
	if (message->operation == PI_DOMAIN_WATCH) {
		if (state.root_count || !message->count) return EINVAL;
		const char *input = (const char *)message->values; size_t bytes = message->count * sizeof(*message->values), offset = 0;
		while (offset < bytes && input[offset]) {
			size_t length = strnlen(input + offset, bytes - offset); char resolved[PATH_MAX]; struct stat info;
			if (length == bytes - offset) return EINVAL;
			if (length >= PATH_MAX) return ENAMETOOLONG;
			if (state.root_count >= PI_DOMAIN_ROOTS) return ENOSPC;
			if (!realpath(input + offset, resolved) || stat(resolved, &info)) return errno;
			if (strcmp(input + offset, resolved)) return EINVAL;
			if (!S_ISDIR(info.st_mode)) return ENOTDIR;
			if (info.st_uid != peer->uid || (info.st_mode & 0022)) return EACCES;
			offset += length + 1;
			for (unsigned depth = 0;; depth++) {
				if (depth >= PI_DOMAIN_ANCESTORS) return E2BIG;
				if (stat(resolved, &info)) return errno;
				struct pi_domain_inode node = {((uint64_t)major(info.st_dev) << 20) | minor(info.st_dev), info.st_ino};
				if (!depth) state.roots[state.root_count] = node;
				else {
					unsigned index = 0;
					while (index < state.ancestor_count && (state.ancestors[index].device != node.device || state.ancestors[index].inode != node.inode)) index++;
					if (index == PI_DOMAIN_ANCESTORS) return E2BIG;
					if (index == state.ancestor_count) state.ancestors[state.ancestor_count++] = node;
				}
				if (!strcmp(resolved, "/")) break;
				char *slash = strrchr(resolved, '/'); if (!slash) return EINVAL; if (slash == resolved) slash++; *slash = 0;
			}
			state.root_count++;
		}
		if (!state.root_count) return EINVAL;
		group->watched = clock.version;
		if (bpf_map_update_elem(domain_map, &group->id, &state, BPF_EXIST)) return errno;
		message->count = 0; return 0;
	}
	if (message->operation == PI_DOMAIN_ADMIT) {
		if (!state.root_count || !message->count || message->count % 3) return EINVAL;
		if (clock.lost || state.uncertain) return EAGAIN;
		uint64_t observed[PI_DOMAIN_VALUES / 3];
		for (unsigned index = 0; index < message->count; index += 3) {
			dev_t device = message->values[index + 1];
			struct pi_domain_record key = {1, ((uint64_t)major(device) << 20) | minor(device), message->values[index + 2]};
			struct pi_object_version object = {0};
			if (bpf_map_lookup_elem(version_map, &key, &object) && errno != ENOENT) return errno;
			observed[index / 3] = object.version;
		}
		struct pi_domain_admission current; struct input_inspection inspection = {message, 0};
		int error = iterate_files(peer->pid, input_query, &current, sizeof(current), inspected_input, &inspection);
		if (error || inspection.found != message->count / 3) return error ? error : EAGAIN;
		for (unsigned index = 0; index < message->count; index += 3) {
			dev_t device = message->values[index + 1];
			struct pi_domain_record key = {1, ((uint64_t)major(device) << 20) | minor(device), message->values[index + 2]};
			struct pi_object_version object = {0};
			if (bpf_map_lookup_elem(version_map, &key, &object) && errno != ENOENT) return errno;
			if (object.version != observed[index / 3] || object.active || (object.uncertain & ~(4U | 16U | 32U))) return EAGAIN;
		}
		struct pi_version_clock checked;
		if (bpf_map_lookup_elem(clock_map, &zero, &checked)) return errno;
		if (checked.lost) return EAGAIN;
		message->count = 0; return 0;
	}
	if (message->operation == PI_DOMAIN_VERSION || message->operation == PI_DOMAIN_CHANGES) {
		for (unsigned index = 0; index < state.ancestor_count; index++) {
			struct pi_domain_inode ancestor = state.ancestors[index]; struct pi_object_version observed;
			struct pi_domain_record key = {1, ancestor.device, ancestor.inode};
			if (!bpf_map_lookup_elem(version_map, &key, &observed)) { if (observed.binding > group->watched) state.uncertain |= 8; }
			else if (errno != ENOENT) return errno;
		}
	}
	if (message->operation == PI_DOMAIN_VERSION) {
		if (message->count || !state.root_count) return EINVAL;
		message->count = 3; message->values[0] = clock.version; message->values[1] = 0; message->values[2] = state.uncertain | clock.lost; return 0;
	}
	if (message->operation == PI_DOMAIN_CHANGES) {
		if (!state.root_count || !message->count || (message->count - 1) % 3) return EINVAL;
		if (state.uncertain || clock.lost) return EAGAIN;
		uint64_t since = message->values[0], version = clock.version;
		/* A retained directory supplies discovery: changing its entries changes that
		 * directory's version. Both proofs and workspace captures query only their
		 * known objects, independent of unrelated objects in the global map. */
		for (unsigned index = 1; index < message->count; index += 3) {
			dev_t device = message->values[index]; uint64_t flags = message->values[index + 2];
			if (!flags || flags & ~3ULL || !message->values[index + 1]) return EINVAL;
			message->values[index + 2] = 0;
			struct pi_domain_record key = {1, ((uint64_t)major(device) << 20) | minor(device), message->values[index + 1]};
			for (unsigned mount = 0; mount < 2; mount++) {
				struct pi_object_version object = {0};
				if (mount) key.type = (state.mount_namespace << 32) | 3;
				if (bpf_map_lookup_elem(version_map, &key, &object) && errno != ENOENT) return errno;
				if ((flags & 1 && (object.version > since || object.active || (object.uncertain & ~(4U | 16U | 32U)))) ||
					(flags & 2 && object.binding > since))
					message->values[index + 2] |= 1 | (object.uncertain || object.binding > since ? 2 : 0);
			}
		}
		struct pi_domain_record key = {2, 0, state.mount_namespace}; struct pi_object_version mount = {0};
		if (bpf_map_lookup_elem(version_map, &key, &mount) && errno != ENOENT) return errno;
		if (mount.version > since || mount.active || mount.uncertain) return EAGAIN;
		if (bpf_map_lookup_elem(clock_map, &zero, &clock) || bpf_map_lookup_elem(domain_map, &group->id, &state)) return errno;
		if (clock.lost || state.uncertain) return EAGAIN;
		message->values[0] = version; return 0;
	}
	return EINVAL;
}

static void execute_application(struct passwd *user, char **command, const char *socket, pid_t owner) {
	if ((!getuid() && (initgroups(user->pw_name, user->pw_gid) || setgid(user->pw_gid) || setuid(user->pw_uid))) ||
		(owner && (prctl(PR_SET_PDEATHSIG, SIGKILL) || getppid() != owner)) ||
		setenv("HOME", user->pw_dir, 1) || (socket ? setenv("PI_SPEC_DOMAIN_SOCKET", socket, 1) : unsetenv("PI_SPEC_DOMAIN_SOCKET"))) _exit(70);
	execvp(command[0], command); _exit(127);
}

/* Privilege is limited to this foreground owner's cgroups and BPF links. The
 * application retains its original account; unavailable collection disables reuse. */
int main(int argc, char **argv) {
	if (argc == 2 && !strcmp(argv[1], "--version")) { puts(PI_DOMAIN_BUILD); return 0; }
	if (argc < 4 || strcmp(argv[1], "--user") || !*argv[2] || strspn(argv[2], "0123456789") != strlen(argv[2])) return 64;
	unsigned long uid = strtoul(argv[2], NULL, 10);
	struct passwd *user = uid && uid <= UINT_MAX ? getpwuid((uid_t)uid) : NULL;
	if (!user || user->pw_uid != uid || (getuid() && getuid() != uid)) return 64;
	int result = 70, server = -1, status = 0, reaped = 0, group_created = 0, directory_created = 0;
	pid_t application = -1;
	struct bpf_object *object = NULL; struct bpf_link *links[64] = {}; unsigned link_count = 0; struct ring_buffer *events = NULL;
	char binary[PATH_MAX], directory[] = "/tmp/pi-execution-domain-XXXXXX";
	if (getuid()) goto done;
	ssize_t size = readlink("/proc/self/exe", binary, sizeof(binary) - 7);
	if (size <= 0 || size >= (ssize_t)sizeof(binary) - 7) goto done;
	memcpy(binary + size, ".bpf.o", 7);
	object = bpf_object__open_file(binary, NULL);
	if (libbpf_get_error(object)) { object = NULL; goto done; }
	if (bpf_object__load(object)) goto done;
	domain_map = bpf_object__find_map_fd_by_name(object, "domains");
	membership_map = bpf_object__find_map_fd_by_name(object, "membership");
	version_map = bpf_object__find_map_fd_by_name(object, "versions"); clock_map = bpf_object__find_map_fd_by_name(object, "clock");
	file_map = bpf_object__find_map_fd_by_name(object, "file_owners");
	file_query = bpf_object__find_program_by_name(object, "descriptor_query");
	input_query = bpf_object__find_program_by_name(object, "admission_query");
	role_map = bpf_object__find_map_fd_by_name(object, "metadata_roles");
	metadata_call_map = bpf_object__find_map_fd_by_name(object, "metadata_calls");
	metadata_notification_map = bpf_object__find_map_fd_by_name(object, "metadata_notifications");
	broker_query = bpf_object__find_program_by_name(object, "metadata_broker");
#define ENTRY_SYMBOL(name) "security_path_" #name,
#define INODE_SYMBOL(name) "security_inode_" #name,
	const char *entry_symbols[] = {PI_ENTRY_OPERATIONS(ENTRY_SYMBOL)}, *inode_symbols[] = {PI_INODE_OPERATIONS(INODE_SYMBOL)};
	struct bpf_program *program;
	bpf_object__for_each_program(program, object) {
		if (!bpf_program__autoload(program) || program == file_query || program == input_query || program == broker_query) continue;
		const char **symbols = NULL; unsigned count = 1;
		if (!strcmp(bpf_program__name(program), "entries_watch")) { symbols = entry_symbols; count = sizeof(entry_symbols) / sizeof(*entry_symbols); }
		if (!strcmp(bpf_program__name(program), "inodes_watch")) { symbols = inode_symbols; count = sizeof(inode_symbols) / sizeof(*inode_symbols); }
		for (unsigned index = 0; index < count; index++) {
			if (link_count == sizeof(links) / sizeof(*links)) goto done;
			struct bpf_link *link = symbols ? bpf_program__attach_kprobe(program, false, symbols[index]) : bpf_program__attach(program);
			if (libbpf_get_error(link)) goto done;
			links[link_count++] = link;
		}
	}
	events = ring_buffer__new(bpf_object__find_map_fd_by_name(object, "metadata_events"), metadata_record, NULL, NULL);
	if (!events) goto done;
	snprintf(group_root, sizeof(group_root), "/sys/fs/cgroup/pi-execution-domain-%d", getpid());
	if (mkdir(group_root, 0755)) goto done;
	group_created = 1;
	if (!mkdtemp(directory)) goto done;
	directory_created = 1;
	if (chown(directory, user->pw_uid, user->pw_gid)) goto done;
	struct sockaddr_un address = {.sun_family = AF_UNIX};
	snprintf(address.sun_path, sizeof(address.sun_path), "%s/control", directory);
	server = socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
	if (server < 0 || bind(server, (struct sockaddr *)&address, sizeof(address)) || chmod(address.sun_path, 0600) ||
		chown(address.sun_path, user->pw_uid, user->pw_gid) || listen(server, 32)) goto done;
	signal(SIGTERM, stop); signal(SIGINT, stop); signal(SIGHUP, stop); signal(SIGPIPE, SIG_IGN);
	pid_t owner = getpid(); application = fork();
	if (!application) {
		if (write_value(group_root, "cgroup.procs", "0")) _exit(70);
		execute_application(user, argv + 3, address.sun_path, owner);
	}
	if (application < 0) goto done;
	while (!stopped) {
		pid_t completed = waitpid(application, &status, WNOHANG);
		if (completed == application) { reaped = 1; result = WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status); break; }
		if (completed < 0 && errno != EINTR) break;
		struct pollfd ready[2] = {{.fd = server, .events = POLLIN}, {.fd = ring_buffer__epoll_fd(events), .events = POLLIN}};
		if (poll(ready, 2, 100) <= 0) continue;
		if (ring_buffer__consume(events) < 0) break;
		if (!(ready[0].revents & POLLIN)) continue;
		int connection = accept4(server, NULL, NULL, SOCK_CLOEXEC);
		if (connection < 0) continue;
		struct timeval timeout = {.tv_sec = 1}; setsockopt(connection, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
		struct ucred peer; socklen_t length = sizeof(peer); struct pi_domain_packet message;
		struct iovec vector = {&message, sizeof(message)}; struct msghdr response = {.msg_iov = &vector, .msg_iovlen = 1};
		ssize_t received = pi_domain_message(connection, &response, 1);
		int descriptor = -1, error = received < (ssize_t)offsetof(struct pi_domain_packet, values) || received > (ssize_t)sizeof(message) ||
			message.count > PI_DOMAIN_VALUES || received != (ssize_t)(offsetof(struct pi_domain_packet, values) + message.count * sizeof(*message.values)) ||
			getsockopt(connection, SOL_SOCKET, SO_PEERCRED, &peer, &length) || length != sizeof(peer) || peer.uid != user->pw_uid
			? EINVAL : respond(&message, &peer, application, &descriptor, events);
		if (error) { message.operation = UINT_MAX; message.count = 0; message.domain = (unsigned)error; }
		vector.iov_len = offsetof(struct pi_domain_packet, values) + message.count * sizeof(*message.values);
		char control[CMSG_SPACE(sizeof(int))];
		if (!error && descriptor >= 0) {
			response.msg_control = control; response.msg_controllen = sizeof(control); struct cmsghdr *header = CMSG_FIRSTHDR(&response);
			header->cmsg_level = SOL_SOCKET; header->cmsg_type = SCM_RIGHTS; header->cmsg_len = CMSG_LEN(sizeof(int)); memcpy(CMSG_DATA(header), &descriptor, sizeof(descriptor));
		}
		(void)pi_domain_message(connection, &response, 0);
		close(connection);
	}
done:
	if (group_created && write_value(group_root, "cgroup.kill", "1")) result = 70;
	if (application > 0 && !reaped) while (waitpid(application, &status, 0) < 0 && errno == EINTR) {}
	for (unsigned index = 0; index < PI_DOMAIN_LIMIT; index++) if (retire(&groups[index])) result = 70;
	if (group_created && remove_group(group_root, NULL)) result = 70;
	if (server >= 0) close(server);
	if (directory_created) { char socket[PATH_MAX]; snprintf(socket, sizeof(socket), "%s/control", directory); unlink(socket); rmdir(directory); }
	for (unsigned index = 0; index < PI_DOMAIN_LIMIT; index++) release_metadata_binary(&metadata_binaries[index]);
	while (link_count) bpf_link__destroy(links[--link_count]);
	ring_buffer__free(events);
	bpf_object__close(object);
	if (application < 0 && !stopped) execute_application(user, argv + 3, NULL, 0);
	return stopped ? 128 + stopped : result;
}
#endif
