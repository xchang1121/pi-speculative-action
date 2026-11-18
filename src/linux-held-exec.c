#define _GNU_SOURCE
#include "linux-execution-domain.h"
#include <dirent.h>
#include <elf.h>
#include <linux/capability.h>
#include <linux/prctl.h>
#ifndef PI_SPEC_AUTHORITY_ONLY
#define PI_SPEC_AUTHORITY_ONLY 0
#endif
#include <ctype.h>
#include <endian.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <linux/audit.h>
#include <linux/kcmp.h>
#include <linux/magic.h>
#include <linux/mount.h>
#include <linux/netlink.h>
#include <linux/openat2.h>
#include <linux/rtnetlink.h>
#include <linux/sock_diag.h>
#include <linux/unix_diag.h>
#include <poll.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/auxv.h>
#include <sys/eventfd.h>
#include <sys/file.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/mount.h>
#include <sys/prctl.h>
#include <sys/ptrace.h>
#include <sys/resource.h>
#include <sys/signalfd.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/syscall.h>
#include <sys/sysmacros.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <sys/user.h>
#include <sys/vfs.h>
#include <sys/wait.h>
#include <unistd.h>

static const long options = PTRACE_O_TRACEFORK | PTRACE_O_TRACEVFORK | PTRACE_O_TRACEVFORKDONE |
                            PTRACE_O_TRACECLONE | PTRACE_O_TRACEEXEC | PTRACE_O_EXITKILL;

/* Stable Node-API types. Weak imports keep the same source usable as a static
 * tracer and as the image library loaded by programs without a Node runtime. */
typedef void *napi_env;
typedef void *napi_value;
typedef void *napi_callback_info;
typedef napi_value (*napi_callback)(napi_env, napi_callback_info);
extern int napi_get_cb_info(napi_env, napi_callback_info, size_t *, napi_value *, napi_value *,
                            void **) __attribute__((weak));
extern int napi_get_value_int32(napi_env, napi_value, int32_t *) __attribute__((weak));
extern int napi_get_buffer_info(napi_env, napi_value, void **, size_t *) __attribute__((weak));
extern int napi_create_int32(napi_env, int32_t, napi_value *) __attribute__((weak));
extern int napi_create_function(napi_env, const char *, size_t, napi_callback, void *, napi_value *)
    __attribute__((weak));
extern int napi_get_undefined(napi_env, napi_value *) __attribute__((weak));
extern int napi_call_function(napi_env, napi_value, napi_value, size_t, const napi_value *,
                              napi_value *) __attribute__((weak));
static __thread int startup_gate[2] = {-1, -1};
static __thread pid_t startup_observer;
static pthread_once_t startup_registration = PTHREAD_ONCE_INIT;
static int startup_registered;

static void startup_child(void) {
  if (startup_gate[0] < 0 || startup_observer <= 0)
    return;
  pid_t pid = getpid();
  char ready;
  struct pollfd channel = {.fd = startup_gate[0], .events = POLLIN};
  /* Only async-signal-safe operations here, before libuv's original child
   * setup. A missing observer leaves the original exec and its error channel
   * intact. */
  if (!prctl(PR_SET_PTRACER, (unsigned long)startup_observer, 0, 0, 0)) {
    if (send(channel.fd, &pid, sizeof(pid), MSG_NOSIGNAL) == sizeof(pid)) {
      int status;
      do {
        status = poll(&channel, 1, PI_HELD_POLL_MS);
      } while (status < 0 && errno == EINTR);
      if (status > 0 && read(channel.fd, &ready, 1) != 1)
        ready = 0;
    }
    (void)prctl(PR_SET_PTRACER, 0, 0, 0, 0);
  }
  close(startup_gate[0]);
  startup_gate[0] = -1;
}

static void register_startup(void) {
  startup_registered = pthread_atfork(NULL, NULL, startup_child) == 0;
}

/* One detached read-only mount tree per capture. The raw child never enters
 * libc's atfork launch gate and uses no allocator, stdio, or inherited locks.
 */
static int readonly_root(void) {
  int channel[2];
  int descriptor = -1;
  int error = 0;
  int status = 0;
  char uid[48];
  char gid[48];
  int uid_length = snprintf(uid, sizeof(uid), "0 %u 1\n", getuid());
  int gid_length = snprintf(gid, sizeof(gid), "0 %u 1\n", getgid());
  if (socketpair(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0, channel))
    return -1;
  pid_t parent = getpid();
  pid_t child = syscall(SYS_clone, SIGCHLD, 0, NULL, NULL, 0);
  if (child == 0) {
    close(channel[0]);
    if (syscall(SYS_prctl, PR_SET_PDEATHSIG, SIGKILL, 0, 0, 0) || getppid() != parent ||
        syscall(SYS_unshare, CLONE_NEWUSER | CLONE_NEWNS)) {
      error = errno ? errno : ECHILD;
      goto publish;
    }
    const char *names[] = {"/proc/self/uid_map", "/proc/self/setgroups", "/proc/self/gid_map"};
    const char *values[] = {uid, "deny", gid};
    int lengths[] = {uid_length, 4, gid_length};
    for (unsigned i = 0; i < 3; i++) {
      int fd = syscall(SYS_openat, AT_FDCWD, names[i], O_WRONLY | O_CLOEXEC, 0);
      if (fd < 0) {
        if (i == 1 && errno == ENOENT)
          continue;
        error = errno;
        goto publish;
      }
      ssize_t count = syscall(SYS_write, fd, values[i], lengths[i]);
      error = count == lengths[i] ? 0 : errno ? errno : EIO;
      close(fd);
      if (error)
        goto publish;
    }
    if (syscall(SYS_mount, NULL, "/", NULL, MS_REC | MS_PRIVATE, NULL)) {
      error = errno;
      goto publish;
    }
    descriptor =
        syscall(SYS_open_tree, AT_FDCWD, "/", OPEN_TREE_CLONE | OPEN_TREE_CLOEXEC | AT_RECURSIVE);
    if (descriptor < 0) {
      error = errno;
      goto publish;
    }
    struct mount_attr attributes = {.attr_set = MOUNT_ATTR_RDONLY};
    if (syscall(SYS_mount_setattr, descriptor, "", AT_EMPTY_PATH | AT_RECURSIVE, &attributes,
                sizeof(attributes)))
      error = errno;
  publish:;
    char control[CMSG_SPACE(sizeof(descriptor))] = {0};
    struct iovec data = {.iov_base = &error, .iov_len = sizeof(error)};
    struct msghdr message = {.msg_iov = &data, .msg_iovlen = 1};
    if (!error) {
      message.msg_control = control;
      message.msg_controllen = sizeof(control);
      struct cmsghdr *header = CMSG_FIRSTHDR(&message);
      header->cmsg_level = SOL_SOCKET;
      header->cmsg_type = SCM_RIGHTS;
      header->cmsg_len = CMSG_LEN(sizeof(descriptor));
      *(int *)CMSG_DATA(header) = descriptor;
    }
    (void)syscall(SYS_sendmsg, channel[1], &message, MSG_NOSIGNAL);
    syscall(SYS_exit_group, 0);
    __builtin_unreachable();
  }
  if (child < 0)
    error = errno;
  close(channel[1]);
  if (child < 0) {
    close(channel[0]);
    errno = error;
    return -1;
  }
  char control[CMSG_SPACE(sizeof(descriptor))] = {0};
  struct iovec data = {.iov_base = &error, .iov_len = sizeof(error)};
  struct msghdr message = {
      .msg_iov = &data, .msg_iovlen = 1, .msg_control = control, .msg_controllen = sizeof(control)};
  ssize_t count;
  do {
    count = recvmsg(channel[0], &message, MSG_CMSG_CLOEXEC);
  } while (count < 0 && errno == EINTR);
  if (count != sizeof(error) || message.msg_flags & (MSG_TRUNC | MSG_CTRUNC))
    error = EIO;
  struct cmsghdr *header = CMSG_FIRSTHDR(&message);
  if (header && header->cmsg_level == SOL_SOCKET && header->cmsg_type == SCM_RIGHTS &&
      header->cmsg_len == CMSG_LEN(sizeof(descriptor)))
    descriptor = *(int *)CMSG_DATA(header);
  close(channel[0]);
  pid_t reaped;
  do {
    reaped = waitpid(child, &status, 0);
  } while (reaped < 0 && errno == EINTR);
  if (reaped != child || !WIFEXITED(status) || WEXITSTATUS(status) || descriptor < 0)
    error = error ? error : EIO;
  if (error) {
    if (descriptor >= 0)
      close(descriptor);
    errno = error;
    return -1;
  }
  return descriptor;
}

/* Open a gate (0), bind its observer PID (>0), or release the parent ends (-1).
 * Thread-local state cannot lend one worker's launch to another callback. */
static napi_value startup_bridge(napi_env env, napi_callback_info info) {
  size_t count = 3;
  napi_value arguments[3];
  napi_value result;
  int32_t operation;
  int32_t value = -1;
  if (napi_get_cb_info(env, info, &count, arguments, NULL, NULL) || !count ||
      napi_get_value_int32(env, arguments[0], &operation))
    return NULL;
  if (count >= 2) {
    void *bytes;
    size_t length;
    if (napi_get_buffer_info(env, arguments[1], &bytes, &length) || length > PI_HELD_BRIDGE_BYTES)
      return NULL;
    if (operation == -3) {
      /* Two little-endian int64 values followed by one terminated path; keep
       * its access time.
       */
      uint64_t fields[2];
      if (length < sizeof(fields) + 2 ||
          strnlen((char *)bytes + sizeof(fields), length - sizeof(fields)) !=
              length - sizeof(fields) - 1)
        return NULL;
      memcpy(fields, bytes, sizeof(fields));
      struct timespec times[2] = {
          {.tv_nsec = UTIME_OMIT},
          {.tv_sec = (int64_t)le64toh(fields[0]), .tv_nsec = (int64_t)le64toh(fields[1])}};
      if (times[1].tv_nsec < 0 || times[1].tv_nsec > 999999999)
        return NULL;
      value = utimensat(AT_FDCWD, (char *)bytes + sizeof(fields), times, AT_SYMLINK_NOFOLLOW);
      if (value < 0 && errno == ENOENT)
        value = 0;
    } else if (operation == -5) {
      if (length)
        return NULL;
      value = readonly_root();
    } else if (operation == -6) {
      /* Root fd and open flags, followed by one terminated absolute path. */
      uint32_t fields[2];
      if (length < 10 || strnlen((char *)bytes + 8, length - 8) != length - 9)
        return NULL;
      memcpy(fields, bytes, sizeof(fields));
      if (le32toh(fields[1]) &
          ~(O_PATH | O_NOFOLLOW | O_DIRECTORY | O_NONBLOCK | O_NOATIME | O_CLOEXEC)) {
        errno = EINVAL;
        value = -1;
      } else {
        struct open_how how = {.flags = le32toh(fields[1]) | O_CLOEXEC,
                               .resolve = RESOLVE_IN_ROOT | RESOLVE_NO_MAGICLINKS};
        value =
            syscall(SYS_openat2, (int32_t)le32toh(fields[0]), (char *)bytes + 8, &how, sizeof(how));
      }
    } else if (operation == -13) {
      if (length)
        return NULL;
      value = memfd_create("pi-artifact", MFD_CLOEXEC | MFD_ALLOW_SEALING);
    } else if (operation == -14) {
      int32_t fields[2];
      if (length != sizeof(fields))
        return NULL;
      memcpy(fields, bytes, sizeof(fields));
      int fd = (int32_t)le32toh(fields[0]);
      int seals = F_SEAL_WRITE | F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_SEAL;
      value = fields[1] ? fcntl(fd, F_ADD_SEALS, seals) : fcntl(fd, F_GET_SEALS);
      if (value >= 0)
        value = fields[1] || (value & seals) == seals;
    } else if (operation == -15) {
      if (length <= sizeof(struct pi_domain_packet) ||
          strnlen((char *)bytes + sizeof(struct pi_domain_packet),
                  length - sizeof(struct pi_domain_packet)) !=
              length - sizeof(struct pi_domain_packet) - 1)
        return NULL;
      struct pi_domain_packet packet;
      memcpy(&packet, bytes, sizeof(packet));
      value = pi_domain_exchange((char *)bytes + sizeof(packet), &packet);
      if (!value)
        memcpy(bytes, &packet, sizeof(packet));
    } else if (operation == -7) {
      uint32_t fd;
      if (length < 5)
        return NULL;
      memcpy(&fd, bytes, sizeof(fd));
      struct stat entry;
      if (fstat((int32_t)le32toh(fd), &entry))
        value = -1;
      else if (!S_ISLNK(entry.st_mode)) {
        errno = EINVAL;
        value = -1;
      } else
        value = syscall(SYS_readlinkat, (int32_t)le32toh(fd), "", (char *)bytes + 4, length - 4);

    } else {
      if (length < 24)
        return NULL;
      value = syscall(SYS_getdents64, operation, bytes, length);
    }
    if (value >= 0 && (operation == -5 || operation == -6 || operation == -13)) {
      /* Native ownership never crosses this synchronous call. Node tracks the
       * adopted duplicate even if its worker terminates while the JavaScript
       * callback runs. */
      napi_value fd;
      napi_value receiver;
      int failed = count != 3 || napi_create_int32(env, value, &fd) ||
                   napi_get_undefined(env, &receiver) ||
                   napi_call_function(env, receiver, arguments[2], 1, &fd, &result);
      close(value);
      return failed ? NULL : result;
    }
    if (value < 0)
      value = -errno;
  } else if (operation == -11) {
    value = 3; /* Filesystem ABI: synchronous adoption, ext4 read-only root,
                  sealed content images. */
  } else if (!operation && startup_gate[0] < 0) {
    if (!socketpair(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0, startup_gate))
      value = startup_gate[1];
  } else if (operation > 0 && startup_gate[0] >= 0 && !startup_observer) {
    startup_observer = operation;
    close(startup_gate[1]);
    startup_gate[1] = -1;
    value = 0;
  } else if (operation == -2 && startup_gate[0] < 0) {
    value = prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0);
  } else if (operation == -1) {
    close(startup_gate[0]);
    close(startup_gate[1]);
    startup_gate[0] = startup_gate[1] = -1;
    startup_observer = 0;
    value = 0;
  }
  return napi_create_int32(env, value, &result) ? NULL : result;
}

napi_value napi_register_module_v1(napi_env env, napi_value exports) {
  (void)exports;
  napi_value result;
  if (!napi_create_function || !napi_get_cb_info || !napi_get_value_int32 || !napi_create_int32 ||
      !napi_get_buffer_info || !napi_get_undefined || !napi_call_function ||
      pthread_once(&startup_registration, register_startup) || !startup_registered)
    return NULL;
  return napi_create_function(env, "startup", (size_t)-1, startup_bridge, NULL, &result) ? NULL
                                                                                         : result;
}

struct output_event {
  unsigned fd;
  unsigned kind;
  size_t length;
  size_t requested;
  unsigned char *data;
};

struct decision_job {
  pthread_t thread;
  pid_t pid;
  const char *socket_path;
  const char *token;
  const char *execution_id;
  int channel[2];
  int connection;
  int outputs[3];
  int result;
  int pidfd;
  struct output_event *events;
  unsigned count;
  int release_barrier;
  char *context;
};

struct traced_process {
  pid_t pid;
  int fd;
  int armed;
  enum { TRACE_RUNNING, TRACE_STOPPED, TRACE_GROUP_STOP } stop;
  int pending;
  int delivered;
  int historical;
  int awaiting_parent;
  int vforking;
  struct decision_job *job;
  struct traced_process *next;
};

static int replace_with_exit(pid_t pid, unsigned code) {
#if defined(__x86_64__)
  struct user_regs_struct registers;
  if (ptrace(PTRACE_GETREGS, pid, 0, &registers) < 0)
    return -1;
  unsigned char stub[16] = {
      0xbf, code, code >> 8, code >> 16, code >> 24, 0xb8, 0xe7, 0, 0, 0, 0x0f, 0x05,
  };
  for (size_t offset = 0; offset < sizeof(stub); offset += sizeof(long)) {
    long word;
    memcpy(&word, stub + offset, sizeof(word));
    if (ptrace(PTRACE_POKETEXT, pid, registers.rip + offset, word) < 0)
      return -1;
  }
  return 0;
#else
  (void)pid;
  (void)code;
  errno = ENOTSUP;
  return -1;
#endif
}

static int transfer(int fd, void *buffer, size_t length, int writing) {
  unsigned char *cursor = buffer;
  while (length) {
    ssize_t moved = writing ? write(fd, cursor, length) : read(fd, cursor, length);
    if (moved < 0 && errno == EINTR)
      continue;
    if (moved <= 0)
      return -1;
    cursor += moved;
    length -= (size_t)moved;
  }
  return 0;
}

static int read_line(int fd, char *buffer, size_t capacity) {
  size_t length = 0;
  while (length + 1 < capacity) {
    char byte;
    if (transfer(fd, &byte, 1, 0) < 0)
      return -1;
    if (byte == '\n') {
      buffer[length] = 0;
      return 0;
    }
    buffer[length++] = byte;
  }
  errno = EMSGSIZE;
  return -1;
}

static int process_open(pid_t pid, const char *name, int flags) {
  char path[64];
  snprintf(path, sizeof(path), "/proc/%d/%s", pid, name);
  return open(path, flags);
}
static FILE *process_file(pid_t pid, const char *name) {
  char path[64];
  snprintf(path, sizeof(path), "/proc/%d/%s", pid, name);
  return fopen(path, "re");
}

struct image_header {
  struct prctl_mm_map mm;
  unsigned char auxv[PI_HELD_AUXILIARY_BYTES];
  size_t auxv_size;
};

static int image_memory(pid_t pid, uint64_t address, void *data, size_t bytes, int writing) {
  int fd = process_open(pid, "mem", (writing ? O_RDWR : O_RDONLY) | O_CLOEXEC);
  if (fd < 0)
    return -1;
  int result = lseek(fd, (off_t)address, SEEK_SET) == (off_t)address
                   ? transfer(fd, data, bytes, writing)
                   : -1;
  close(fd);
  return result;
}
/* Confinement owns CORE/NPROC; elapsed CPU and realtime budgets are not in the
 * image. */

static int image_wait(pid_t pid, int *status) {
  pid_t result;
  do {
    result = waitpid(pid, status, __WALL | __WNOTHREAD);
  } while (result < 0 && errno == EINTR);
  return result == pid && WIFSTOPPED(*status) ? 0 : -1;
}
static long image_invoke(pid_t pid, unsigned long at, long number, const unsigned long args[6]) {
  struct user_regs_struct saved;
  struct user_regs_struct call;
  if (ptrace(PTRACE_GETREGS, pid, 0, &saved) < 0)
    return -1;
  call = saved;
  unsigned long original = 0;
  int bootstrap = !at;
  int status;
  if (bootstrap) {
    at = saved.rip;
    errno = 0;
    original = (unsigned long)ptrace(PTRACE_PEEKTEXT, pid, at, 0);
    if (errno || ptrace(PTRACE_POKETEXT, pid, at, (original & ~0xffffffUL) | 0xcc050fUL) < 0)
      return -1;
  }
  call.rip = at;
  call.rax = number;
  call.orig_rax = (unsigned long)-1;
  call.rdi = args[0];
  call.rsi = args[1];
  call.rdx = args[2];
  call.r10 = args[3];
  call.r8 = args[4];
  call.r9 = args[5];
  if (ptrace(PTRACE_SETREGS, pid, 0, &call) < 0)
    return -1;
  for (unsigned step = 0; step < 1U; step++) {
    for (;;) {
      if (ptrace(PTRACE_CONT, pid, 0, 0) < 0 || image_wait(pid, &status) < 0)
        return -1;
      if ((unsigned)status >> 16 != PTRACE_EVENT_SECCOMP)
        break;
      unsigned long tag;
      if (WSTOPSIG(status) != SIGTRAP || ptrace(PTRACE_GETEVENTMSG, pid, 0, &tag) < 0 ||
          (tag && tag != 0x5049))
        return -1;
      /* The broker's direct-entry filter reserves this unused sixth argument.
       * The saved application registers are restored after the private call. */
      if (tag == 0x5049 && ptrace(PTRACE_POKEUSER, pid, offsetof(struct user_regs_struct, r9),
                                  0x5049545200000000UL) < 0)
        return -1;
    }
    if (WSTOPSIG(status) != SIGTRAP || (unsigned)status >> 16)
      return -1;
  }
  if (ptrace(PTRACE_GETREGS, pid, 0, &call) < 0 || call.rip != at + 3 ||
      (bootstrap && ptrace(PTRACE_POKETEXT, pid, at, original) < 0) ||
      ptrace(PTRACE_SETREGS, pid, 0, &saved) < 0)
    return -1;
  return (long)call.rax;
}
#define image_call(pid, at, number, ...)                                                           \
  image_invoke(pid, at, number, (unsigned long[6]){__VA_ARGS__})

static int image_mm(pid_t pid, struct image_header *header, unsigned long code) {
  char data[PI_HELD_STRING_BYTES];
  unsigned long fields[53] = {0};
  FILE *file = process_file(pid, "stat");
  if (!file)
    return -1;
  int valid = fgets(data, sizeof(data), file) != NULL;
  fclose(file);
  if (!valid)
    return -1;
  char *position = strrchr(data, ')');
  char *saved;
  if (!position)
    return -1;
  unsigned index = 3;
  for (char *part = strtok_r(position + 2, " ", &saved); part && index < 53;
       part = strtok_r(NULL, " ", &saved), index++)
    if (index != 3)
      fields[index] = strtoull(part, NULL, 10);
  long brk = (long)fields[47];
  /* A fresh ELF image has brk == start_brk until its first instruction. */
  if (index < 52 || fields[20] != 1 || (code && (brk = image_call(pid, code, SYS_brk, 0)) < 0))
    return -1;
  header->mm = (struct prctl_mm_map){.start_code = fields[26],
                                     .end_code = fields[27],
                                     .start_stack = fields[28],
                                     .start_data = fields[45],
                                     .end_data = fields[46],
                                     .start_brk = fields[47],
                                     .brk = (unsigned long)brk,
                                     .arg_start = fields[48],
                                     .arg_end = fields[49],
                                     .env_start = fields[50],
                                     .env_end = fields[51],
                                     .exe_fd = (unsigned)-1};
  int fd = process_open(pid, "auxv", O_RDONLY | O_CLOEXEC);
  if (fd < 0)
    return -1;
  ssize_t size = read(fd, header->auxv, sizeof(header->auxv));
  close(fd);
  if (size <= 0 || size >= (ssize_t)sizeof(header->auxv))
    return -1;
  header->auxv_size = (size_t)size;
  return 0;
}
/* Advance owned descriptor execution in the tracer's normal signal/event loop.
 */
static int duplicate_tracee_fd(struct decision_job *job, unsigned fd);
struct exec_files {
  unsigned refs;
  int fd;
  int pin;
  int pending;
};
struct exec_call {
  struct user_regs_struct original;
  unsigned long empty;
  unsigned long mask;
  unsigned long code;
  unsigned long command;
  struct exec_files *files;
  /* Syscall entry/exit pairs are adjacent; a restart returns to the same pair's entry. */
  enum { EXEC_LOOKUP_ENTRY = -1, EXEC_LOOKUP_EXIT, EXEC_FD_ENTRY, EXEC_FD_EXIT,
         EXEC_CLOSE_ENTRY, EXEC_CLOSE_EXIT, EXEC_MM_ENTRY = 7, EXEC_MM_EXIT,
         EXEC_NAME_ENTRY, EXEC_NAME_EXIT, EXEC_CLEANUP_ENTRY, EXEC_CLEANUP_EXIT, EXEC_RESUME } phase;
  int eligible;
  int lease;
  pid_t tid;
};
static struct exec_files *exec_files(void **context) {
  if (!*context) {
    struct exec_files *files = calloc(1, sizeof(*files));
    if (!files)
      return NULL;
    files->refs = 1;
    files->fd = files->pin = -1;
    *context = files;
  }
  return *context;
}
int pi_clone_exec_files(void **parent, void **child, int shared) {
  struct exec_files *files = exec_files(parent);
  if (!files || *child)
    return -1;
  if (shared) {
    files->refs++;
    *child = files;
  } else if (files->fd >= 0)
    return -1; /* Birth waits until the transient descriptor is gone. */
  return 0;
}
static int exec_pin(pid_t pid, int fd) {
  struct decision_job source = {.pid = pid, .pidfd = -1};
  int pin = duplicate_tracee_fd(&source, (unsigned)fd);
  int error = errno;
  close(source.pidfd);
  errno = error;
  return pin;
}
static void exec_closed(struct exec_files *files) {
  close(files->pin);
  files->fd = files->pin = -1;
  files->pending = 0;
}
static int exec_restarting(long value) {
  return value == -512 || value == -513 || value == -514 || value == -516;
}
static int exec_record_path(char *path, size_t size, const char *source, pid_t tid) {
  return source && snprintf(path, size, "%s.route.%d", source, tid) < (int)size ? 0 : -1;
}
static int exec_injected_fd(pid_t tid, const char *source) {
  char path[PATH_MAX + 64];
  FILE *file;
  if (exec_record_path(path, sizeof(path), source, tid) < 0 || !(file = fopen(path, "re")))
    return -1;
  char *field = NULL;
  size_t capacity = 0;
  int result = -1;
  for (unsigned index = 0; index < 5; index++) {
    ssize_t size = getdelim(&field, &capacity, 0, file);
    if (size < 0 || size > PI_HELD_STRING_BYTES || field[size - 1])
      break;
    if (index == 4) {
      char *end;
      long value = strtol(field, &end, 10);
      if (*field && !*end && value >= 0 && value <= INT_MAX)
        result = value;
    }
  }
  free(field);
  fclose(file);
  return result;
}
int pi_exec_call_ready(void *context) {
  struct exec_call *call = context;
  return !call || !call->lease || !flock(call->lease - 1, LOCK_EX | LOCK_NB);
}
void pi_drop_exec_files(pid_t pid, void **context, void **pending, const char *source,
                        int executed) {
  struct exec_files *files = *context;
  struct exec_call *call = *pending;
  if (call) {
    if (call->files && call->files->fd < 0 && call->phase == EXEC_LOOKUP_EXIT)
      call->files->fd = exec_injected_fd(call->tid, source);
    if (call->files && call->files->fd >= 0)
      call->files->pending = 1;
    if (executed && call->tid && call->tid != pid) {
      char from[PATH_MAX + 64];
      char to[PATH_MAX + 64];
      if (!exec_record_path(from, sizeof(from), source, call->tid) &&
          !exec_record_path(to, sizeof(to), source, pid))
        rename(from, to);
    }
    if (call->lease)
      close(call->lease - 1);
    free(call);
    *pending = NULL;
  }
  *context = NULL;
  if (files && !--files->refs) {
    exec_closed(files);
    free(files);
  }
}
static int exec_mask(pid_t pid, struct exec_call *call, int blocked) {
  unsigned long mask = ~0UL;
  if (blocked && ptrace(PTRACE_GETSIGMASK, pid, 8, &call->mask) < 0)
    return -1;
  return ptrace(PTRACE_SETSIGMASK, pid, 8, blocked ? &mask : &call->mask) == 0 ? 0 : -1;
}
static int exec_rewind(pid_t pid, const struct user_regs_struct *original, long number,
                       unsigned long a, unsigned long b) {
  struct user_regs_struct registers = *original;
  registers.rip -= 2;
  registers.orig_rax = (unsigned long)-1;
  registers.rax = number;
  registers.rdi = a;
  registers.rsi = b;
  return ptrace(PTRACE_SETREGS, pid, 0, &registers) == 0 ? 0 : -1;
}
int pi_prepare_exec_call(pid_t pid, void **context, void **file_context, const char *source) {
  struct exec_call *call = calloc(1, sizeof(*call));
  struct user_regs_struct registers;
  char path[PATH_MAX + 64];
  char name[4096];
  int memory = -1;
  int result = -1;
  if (!call || !(call->files = exec_files(file_context)) ||
      ptrace(PTRACE_GETREGS, pid, 0, &call->original) < 0)
    goto done;
  const struct user_regs_struct *original = &call->original;
  int at = original->orig_rax == SYS_execveat;
  if ((!at && original->orig_rax != SYS_execve) || original->cs != 0x33 || original->rip < 2)
    goto done;
  memory = process_open(pid, "mem", O_RDONLY | O_CLOEXEC);
  unsigned long address = at ? original->rsi : original->rdi;
  ssize_t length = memory >= 0 ? pread(memory, name, sizeof(name), address) : -1;
  if (length <= 0 || !memchr(name, 0, (size_t)length)) {
    result = 0;
    goto done;
  }
  call->empty = address + strlen(name);
  call->tid = pid;
  if (exec_record_path(path, sizeof(path), source, pid) < 0)
    goto done;
  call->lease = open(path, O_RDWR | O_CREAT | O_TRUNC | O_CLOEXEC, 0600) + 1;
  if (!call->lease || exec_mask(pid, call, 1) < 0)
    goto done;
  registers = *original;
  registers.r9 = 0x5049455800000000UL | (at ? (unsigned int)original->r8 : 0);
  if (ptrace(PTRACE_SETREGS, pid, 0, &registers) < 0)
    goto done;
  *context = call;
  call = NULL;
  result = 0;
done:
  close(memory);
  if (call && call->lease)
    close(call->lease - 1);
  free(call);
  return result;
}
/* 1 consumes an internal stop, 0 exposes the completed original syscall. */
int pi_step_exec_call(pid_t pid, void **context, void **file_context, int exiting) {
  struct exec_call *call = *context;
  if (!call) {
    struct exec_files *files = *file_context;
    if (!files || !files->pending)
      return 0;
    if (files->pin < 0)
      files->pin = exec_pin(pid, files->fd);
    long same =
        files->pin < 0 ? -1 : syscall(SYS_kcmp, pid, getpid(), KCMP_FILE, files->fd, files->pin);
    if (same) {
      if (same < 0 && errno != EBADF)
        return -1;
      exec_closed(files);
      return 0;
    }
    call = calloc(1, sizeof(*call));
    if (!call)
      return -1;
    call->files = files;
    call->phase = exiting ? EXEC_CLOSE_ENTRY : EXEC_CLEANUP_EXIT;
    files->pending = 0;
    *context = call;
    if (ptrace(PTRACE_GETREGS, pid, 0, &call->original) < 0 || exec_mask(pid, call, 1) < 0)
      return -1;
    if (exiting)
      return exec_rewind(pid, &call->original, SYS_close, files->fd, 0) == 0 ? 1 : -1;
    struct user_regs_struct cleanup = call->original;
    cleanup.rax = cleanup.orig_rax = SYS_close;
    cleanup.rdi = files->fd;
    return ptrace(PTRACE_SETREGS, pid, 0, &cleanup) == 0 ? 1 : -1;
  }
  struct user_regs_struct registers;
  if (ptrace(PTRACE_GETREGS, pid, 0, &registers) < 0)
    return -1;
  long value = (long)registers.rax;
  if (!(call->phase & 1) && exec_restarting(value)) {
    call->phase--;
    return 1;
  }
  switch (call->phase) {
  case EXEC_LOOKUP_ENTRY:
  case EXEC_FD_ENTRY:
  case EXEC_CLOSE_ENTRY:
  case EXEC_MM_ENTRY:
  case EXEC_NAME_ENTRY:
  case EXEC_CLEANUP_ENTRY:
    call->phase++;
    return 1;
  case EXEC_LOOKUP_EXIT:
    if (value > 0 && value <= INT_MAX) {
      call->files->fd = (int)value - 1;
      if (call->files->refs > 1 && (call->files->pin = exec_pin(pid, call->files->fd)) < 0)
        return -1;
      registers = call->original;
      registers.rip -= 2;
      registers.rax = SYS_execveat;
      registers.orig_rax = (unsigned long)-1;
      registers.rdi = call->files->fd;
      registers.rsi = call->empty;
      registers.rdx =
          call->original.orig_rax == SYS_execveat ? call->original.rdx : call->original.rsi;
      registers.r10 =
          call->original.orig_rax == SYS_execveat ? call->original.r10 : call->original.rdx;
      registers.r8 = AT_EMPTY_PATH;
      registers.r9 = 0x5049455900000000UL;
      call->phase = EXEC_FD_ENTRY;
      return ptrace(PTRACE_SETREGS, pid, 0, &registers) == 0 ? 1 : -1;
    }
    /* A native-path continuation already executed; errors retain the caller
     * registers. */
    __attribute__((fallthrough));
  case EXEC_FD_EXIT:
    if (!value)
      return exec_mask(pid, call, 0);
    call->original.rax = registers.rax;
    if (call->files->fd >= 0) {
      call->phase = EXEC_CLOSE_ENTRY;
      return exec_rewind(pid, &call->original, SYS_close, call->files->fd, 0) == 0 ? 1 : -1;
    }
    break;
  case EXEC_CLOSE_EXIT:
  case EXEC_CLEANUP_EXIT: {
    if (value)
      return -1;
    exec_closed(call->files);
    if (call->phase == EXEC_CLEANUP_EXIT) {
      registers = call->original;
      registers.rip -= 2;
      registers.rax = registers.orig_rax;
      registers.orig_rax = (unsigned long)-1;
      call->phase = EXEC_RESUME;
      return ptrace(PTRACE_SETREGS, pid, 0, &registers) == 0 ? 1 : -1;
    }
    break;
  }
  case EXEC_MM_EXIT:
    if (value)
      return -1;
    registers = call->original;
    registers.rax = SYS_prctl;
    registers.orig_rax = (unsigned long)-1;
    registers.rdi = PR_SET_NAME;
    registers.rsi = call->command;
    registers.rdx = registers.r10 = registers.r8 = registers.r9 = 0;
    call->phase = EXEC_NAME_ENTRY;
    return ptrace(PTRACE_SETREGS, pid, 0, &registers) == 0 ? 1 : -1;
  case EXEC_NAME_EXIT:
    if (value || ptrace(PTRACE_POKETEXT, pid, call->original.rip, call->code) < 0)
      return -1;
    break;
  case EXEC_RESUME:
    break;
  }
  if (exec_mask(pid, call, 0) < 0 || ptrace(PTRACE_SETREGS, pid, 0, &call->original) < 0)
    return -1;
  int result = call->phase == EXEC_NAME_EXIT ? 2 | (call->eligible ? 4 : 0) : 0;
  if (call->lease)
    close(call->lease - 1);
  free(call);
  *context = NULL;
  return result;
}

/* Restore the requested exec pathname after a confined fd exec, before the
 * image runs. */
int pi_restore_exec_context(pid_t pid, const char *source, const char *invoked, const char *command,
                            void **context) {
  struct user_regs_struct registers;
  struct image_header *header = calloc(1, sizeof(*header));
  unsigned long *vector = NULL;
  unsigned long argc;
  unsigned long word;
  unsigned long aux;
  unsigned long slot = 0;
  int result = -1;
  int consumed = 0;
  int eligible = 0;
  char *arguments = NULL;
  char *fields[PI_HELD_ROUTE_FIELDS] = {0};
  unsigned count = 0;
  unsigned prefix = 0;
  const char *name = NULL;
  const char *comm = NULL;
  FILE *record = NULL;
  char path[PATH_MAX + 64];
  char original[PI_HELD_STRING_BYTES];
  size_t argument_bytes = 0;
  if (snprintf(path, sizeof(path), "%s.route.%d", source, pid) >= (int)sizeof(path) ||
      !(record = fopen(path, "re")))
    goto done;
  for (; count < PI_HELD_ROUTE_FIELDS; count++) {
    size_t capacity = 0;
    ssize_t bytes = getdelim(&fields[count], &capacity, 0, record);
    if (bytes < 0)
      break;
    if (bytes > PI_HELD_STRING_BYTES || fields[count][bytes - 1])
      goto done;
  }
  if (ferror(record) || count < 6 || count == PI_HELD_ROUTE_FIELDS ||
      (strcmp(fields[0], "B") && strcmp(fields[0], "N")) || *fields[1] != '/' ||
      *fields[count - 1] != '/')
    goto done;
  name = fields[2];
  comm = fields[3];
  prefix = count - 6;
  eligible = !strcmp(fields[0], "B");
  if (!eligible) {
    if (invoked && strcmp(name, source)) {
      name = invoked;
      comm = command;
      consumed = 1;
    }
  }
  if (!comm) {
    comm = strrchr(name, '/');
    comm = comm ? comm + 1 : name;
  }
  size_t length = strlen(name) + 1;
  size_t comm_length = strlen(comm) + 1;
  if (!header || length > PI_HELD_STRING_BYTES || ptrace(PTRACE_GETREGS, pid, 0, &registers) < 0 ||
      registers.cs != 0x33 || image_mm(pid, header, 0) < 0)
    goto done;
  for (size_t at = 0; at + 16 <= header->auxv_size; at += 16)
    if (*(unsigned long *)(header->auxv + at) == AT_EXECFN) {
      slot = at + 8;
      break;
    }
  if (!slot)
    goto done;
  int memory = process_open(pid, "mem", O_RDONLY | O_CLOEXEC);
  if (memory < 0)
    goto done;
  ssize_t read = pread(memory, original, sizeof(original), *(unsigned long *)(header->auxv + slot));
  close(memory);
  if (read <= 0 || !memchr(original, 0, (size_t)read))
    goto done;
  if (!prefix && !strcmp(original, name)) {
    result = 0;
    goto done;
  }
  if (image_memory(pid, registers.rsp, &argc, 8, 0) < 0 || argc > PI_HELD_VECTOR_ENTRIES)
    goto done;
  aux = argc + 2;
  do {
    if (aux > PI_HELD_VECTOR_ENTRIES ||
        image_memory(pid, registers.rsp + 8 * aux++, &word, 8, 0) < 0)
      goto done;
  } while (word);
  size_t bytes = aux * 8 + header->auxv_size;
  vector = malloc(bytes + 8 * prefix);
  if (!vector || image_memory(pid, registers.rsp, vector, bytes, 0) < 0 ||
      memcmp(vector + aux, header->auxv, header->auxv_size))
    goto done;
  if (prefix) {
    size_t before = header->mm.arg_end - header->mm.arg_start;
    size_t added = 0;
    if (!argc || before > 4 * 1024 * 1024)
      goto done;
    for (unsigned index = 5; index + 1 < count; index++)
      added += strlen(fields[index]) + 1;
    arguments = malloc(added + before);
    if (!arguments || image_memory(pid, header->mm.arg_start, arguments + added, before, 0) < 0)
      goto done;
    size_t skipped = strnlen(arguments + added, before) + 1;
    if (skipped > before)
      goto done;
    memmove(arguments + added, arguments + added + skipped, before - skipped);
    argument_bytes = added + before - skipped;
    for (unsigned index = 5, offset = 0; index + 1 < count; index++) {
      size_t size = strlen(fields[index]) + 1;
      memcpy(arguments + offset, fields[index], size);
      offset += size;
    }
    memmove(vector + argc + prefix, vector + argc + 1, bytes - (argc + 1) * 8);
    vector[0] = argc + prefix - 1;
    aux += prefix - 1;
    bytes += 8 * (prefix - 1);
  }
  unsigned long stack =
      (registers.rsp - bytes - argument_bytes - length - comm_length - sizeof(header->mm) - 31) &
      ~15UL;
  unsigned long args = stack + bytes;
  unsigned long text = args + argument_bytes;
  unsigned long mm = (text + length + comm_length + 7) & ~7UL;
  if (prefix) {
    size_t offset = 0;
    for (unsigned long index = 1; index <= vector[0]; index++) {
      if (offset >= argument_bytes)
        goto done;
      vector[index] = args + offset;
      offset += strlen(arguments + offset) + 1;
    }
    if (offset != argument_bytes || image_memory(pid, args, arguments, argument_bytes, 1) < 0)
      goto done;
    header->mm.arg_start = args;
    header->mm.arg_end = text;
  }
  vector[aux + slot / 8] = text;
  header->mm.auxv = (void *)(stack + aux * 8);
  header->mm.auxv_size = header->auxv_size;
  header->mm.start_stack = stack;
  if (image_memory(pid, stack, vector, bytes, 1) < 0 ||
      image_memory(pid, text, (void *)name, length, 1) < 0 ||
      image_memory(pid, text + length, (void *)comm, comm_length, 1) < 0 ||
      image_memory(pid, mm, &header->mm, sizeof(header->mm), 1) < 0)
    goto done;
  struct exec_call *call = calloc(1, sizeof(*call));
  if (!call)
    goto done;
  *context = call;
  call->original = registers;
  call->original.rsp = stack;
  call->command = text + length;
  call->phase = EXEC_MM_ENTRY;
  call->eligible = eligible;
  errno = 0;
  call->code = (unsigned long)ptrace(PTRACE_PEEKTEXT, pid, registers.rip, 0);
  if (errno || exec_mask(pid, call, 1) < 0 ||
      ptrace(PTRACE_POKETEXT, pid, registers.rip, (call->code & ~0xffffUL) | 0x050fUL) < 0)
    goto done;
  registers.rax = SYS_prctl;
  registers.orig_rax = (unsigned long)-1;
  registers.rdi = PR_SET_MM;
  registers.rsi = PR_SET_MM_MAP;
  registers.rdx = mm;
  registers.r10 = sizeof(header->mm);
  registers.r8 = registers.r9 = 0;
  result = ptrace(PTRACE_SETREGS, pid, 0, &registers) == 0 ? 8 : -1;

done:
  if (record)
    fclose(record);
  for (unsigned index = 0; index < PI_HELD_ROUTE_FIELDS; index++)
    free(fields[index]);
  free(arguments);
  free(vector);
  free(header);
  return result < 0 ? result : result | consumed | (eligible ? 2 : 0);
}

/* pagemap's file-page bit is available without PFNs. Equal bytes alone cannot
 * distinguish an untouched file page from a private page written back to its
 * old value. */

static int descriptor_numbers(pid_t pid, int minimum, int *fds, unsigned capacity) {
  char path[64];
  snprintf(path, sizeof(path), "/proc/%ld/fd", (long)pid);
  DIR *directory = opendir(path);
  if (!directory)
    return -1;
  unsigned count = 0;
  int error = 0;
  int scanner = pid == getpid() ? dirfd(directory) : -1;
  for (;;) {
    errno = 0;
    struct dirent *entry = readdir(directory);
    if (!entry) {
      error = errno;
      break;
    }
    char *end;
    long fd = strtol(entry->d_name, &end, 10);
    if (!*entry->d_name || *end || fd < minimum || fd > INT_MAX || fd == scanner)
      continue;
    if (count == capacity) {
      error = E2BIG;
      break;
    }
    fds[count++] = (int)fd;
  }
  closedir(directory);
  if (error)
    errno = error;
  return error ? -1 : (int)count;
}

/* Read installed state, never a tracee's mutable result/control buffer. */

/* Called by the already authorized strace owner; no new ptracer exception or
 * sandbox permission is needed. A positive result leaves the process unchanged.
 */

struct process_credentials {
  unsigned parent;
  unsigned uid[4];
  unsigned gid[4];
  unsigned no_new_privileges;
  unsigned threads;
  unsigned seccomp;
  unsigned long long permitted;
  unsigned long long effective;
  unsigned long long inheritable;
  unsigned long long ambient;
  unsigned long long pending;
  unsigned long long shared_pending;
};

static int process_credentials(pid_t pid, struct process_credentials *value) {
  char line[256];
  FILE *input = process_file(pid, "status");
  if (!input)
    return -1;
  unsigned fields = 0;
  while (fgets(line, sizeof(line), input)) {
    if (sscanf(line, "PPid: %u", &value->parent) == 1)
      fields |= 1;
    if (sscanf(line, "Uid: %u %u %u %u", value->uid, value->uid + 1, value->uid + 2,
               value->uid + 3) == 4)
      fields |= 2;
    if (sscanf(line, "Gid: %u %u %u %u", value->gid, value->gid + 1, value->gid + 2,
               value->gid + 3) == 4)
      fields |= 4;
    if (sscanf(line, "CapPrm: %llx", &value->permitted) == 1)
      fields |= 8;
    if (sscanf(line, "CapEff: %llx", &value->effective) == 1)
      fields |= 16;
    if (sscanf(line, "CapInh: %llx", &value->inheritable) == 1)
      fields |= 32;
    if (sscanf(line, "CapAmb: %llx", &value->ambient) == 1)
      fields |= 64;
    if (sscanf(line, "NoNewPrivs: %u", &value->no_new_privileges) == 1)
      fields |= 128;
    if (sscanf(line, "Threads: %u", &value->threads) == 1)
      fields |= 256;
    if (sscanf(line, "Seccomp: %u", &value->seccomp) == 1)
      fields |= 512;
    if (sscanf(line, "SigPnd: %llx", &value->pending) == 1)
      fields |= 1024;
    if (sscanf(line, "ShdPnd: %llx", &value->shared_pending) == 1)
      fields |= 2048;
  }
  int valid = fields == 4095 && !ferror(input);
  fclose(input);
  return valid ? 0 : -1;
}

static int duplicate_tracee_fd(struct decision_job *job, unsigned fd) {
#if defined(SYS_pidfd_open) && defined(SYS_pidfd_getfd)
  if (job->pidfd < 0)
    job->pidfd = (int)syscall(SYS_pidfd_open, job->pid, 0);
  if (job->pidfd < 0) {
    char line[256];
    int group = job->pid;
    FILE *status = process_file(job->pid, "status");
    if (status) {
      while (fgets(line, sizeof(line), status))
        if (sscanf(line, "Tgid: %d", &group) == 1)
          break;
      fclose(status);
    }
    if (group != job->pid && syscall(SYS_kcmp, job->pid, group, KCMP_FILES, 0, 0) == 0)
      job->pidfd = (int)syscall(SYS_pidfd_open, group, 0);
  }

  /* Both syscalls return CLOEXEC handles; one process pin serves the entire
   * held decision. */
  if (job->pidfd >= 0)
    return (int)syscall(SYS_pidfd_getfd, job->pidfd, fd, 0);
#endif
  return -1;
}

static void free_events(struct output_event *events, unsigned count) {
  if (!events)
    return;
  for (unsigned index = 0; index < count; index++)
    free(events[index].data);
  free(events);
}

static int has_unmodeled_descriptors(int minimum) {
  /* A closed standard stream would be filled by the first file opened; bypass
   * before any is. */
  for (int fd = 0; fd < 3; fd++) {
    if (fcntl(fd, F_GETFD) < 0)
      return errno == EBADF ? 1 : -1;
  }
  return descriptor_numbers(getpid(), minimum, NULL, 0) < 0 ? (errno == E2BIG ? 1 : -1) : 0;
}

/* The broker decodes a request as UTF-8, as it decoded Node's strings. */
static void json_bytes(FILE *out, const char *value, size_t length) {
  fputc('"', out);
  for (size_t index = 0; index < length; index++) {
    unsigned char byte = (unsigned char)value[index];
    if (byte == '"' || byte == '\\')
      fprintf(out, "\\%c", byte);
    else if (byte < 0x20)
      fprintf(out, "\\u%04x", byte);
    else
      fputc(byte, out);
  }
  fputc('"', out);
}

/* The image is already installed, and stays stopped while its owner decides.
 * Pins outlive the exchange but never the target's resumed execution. */
struct broker_job {
  pid_t pid;
  pthread_t thread;
  int channel[2];
  int connection;
  int result;
  int count;
  int numbers[3];
  int files[3];
  char *request;
  char *response;
  size_t request_size;
  size_t response_size;
  struct sockaddr_un address;
};

static int broker_pipes(FILE *out, struct broker_job *job) {
  (void)out;
  for (int index = 0; index < job->count; index++)
    if (job->numbers[index] >= 3)
      return -1;
  return 0;
}
static void broker_free(struct broker_job *job) {
  for (int index = 0; index < job->count; index++)
    close(job->files[index]);
  close(job->channel[0]);
  close(job->channel[1]);
  close(job->connection);
  free(job->request);
  free(job->response);
  free(job);
}
static int broker_reply(struct broker_job *job) {
  const char *at = job->response;
  const char *end = at + job->response_size;
  while (at < end) {
    const char *newline = memchr(at, '\n', (size_t)(end - at));
    int fd;
    int value;
    int consumed = -1;
    unsigned long long length;
    if (!newline)
      return -1;
    char kind = *at;
    if (kind == 'o' && sscanf(at + 1, "%d %llu%n", &fd, &length, &consumed) == 2 &&
        at + 1 + consumed == newline && (fd == 1 || fd == 2) &&
        length <= (unsigned long long)(end - newline - 1)) {
      int pin = -1;
      for (int index = 0; index < job->count; index++)
        if (job->numbers[index] == fd)
          pin = job->files[index];
      if (pin < 0)
        return -1;
      if (kind == 'o' && transfer(pin, (void *)(newline + 1), (size_t)length, 1) < 0)
        return -1;
      at = newline + 1 + length;
      continue;
    }
    if (newline + 1 != end)
      return -1;
    if (newline == at + 1 && (*at == 'b' || *at == 'c'))
      return *at == 'b' ? -2 : -3;
    if (sscanf(at, "x %d%n", &value, &consumed) == 1 && at + consumed == newline && value >= 0 &&
        value <= 255)
      return value;
    return sscanf(at, "s %d%n", &value, &consumed) == 1 && at + consumed == newline && value > 0 &&
                   value < NSIG
               ? 256 + value
               : -1;
  }
  return -1;
}
static void *broker_decide(void *context) {
  struct broker_job *job = context;
  sigset_t blocked;
  sigfillset(&blocked);
  pthread_sigmask(SIG_SETMASK, &blocked, NULL);
  job->result = -1;
  if (connect(job->connection, (struct sockaddr *)&job->address, sizeof(job->address)) < 0 ||
      transfer(job->connection, job->request, job->request_size, 1) < 0 ||
      shutdown(job->connection, SHUT_WR) < 0)
    goto done;
  char bytes[PI_HELD_READ_BYTES];
  ssize_t size;
  while ((size = read(job->connection, bytes, sizeof(bytes))) > 0 || (size < 0 && errno == EINTR))
    if (size > 0) {
      if ((size_t)size > PI_HELD_OUTPUT_BYTES - job->response_size)
        goto done;
      char *grown = realloc(job->response, job->response_size + (size_t)size);
      if (!grown)
        goto done;
      job->response = grown;
      memcpy(grown + job->response_size, bytes, (size_t)size);
      job->response_size += (size_t)size;
    }
  if (size == 0)
    job->result = broker_reply(job);
done:
  shutdown(job->channel[1], SHUT_WR);
  return NULL;
}
void *pi_broker_event(pid_t pid, const char *source, int status, int *notification) {
  *notification = -1;
  struct broker_job *job = calloc(1, sizeof(*job));
  if (!job)
    return NULL;
  job->pid = pid;
  job->channel[0] = job->channel[1] = job->connection = -1;
  for (int index = 0; index < 3; index++)
    job->files[index] = -1;
  job->count = status < 0 ? descriptor_numbers(pid, 0, job->numbers, 3) : 0;
  struct decision_job target = {.pid = pid, .pidfd = -1};
  int captured = job->count >= 0;
  int outputs = 0;
  for (int index = 0; captured && index < job->count; index++) {
    captured =
        (job->files[index] = duplicate_tracee_fd(&target, (unsigned)job->numbers[index])) >= 0;
    if (job->numbers[index] == 1 || job->numbers[index] == 2)
      outputs |= 1 << job->numbers[index];
  }
  close(target.pidfd);
  if (!captured || (status < 0 && outputs != 6)) {
    broker_free(job);
    return NULL;
  }
  char configuration[PATH_MAX];
  char token[256];
  FILE *file = NULL;
  FILE *out = NULL;
  if (!source ||
      snprintf(configuration, sizeof(configuration), "%s.configuration", source) >=
          (int)sizeof(configuration) ||
      !(file = fopen(configuration, "re")) ||
      !fgets(job->address.sun_path, sizeof(job->address.sun_path), file) ||
      !fgets(token, sizeof(token), file))
    goto fail;
  job->address.sun_family = AF_UNIX;
  job->address.sun_path[strcspn(job->address.sun_path, "\n")] = token[strcspn(token, "\n")] = 0;
  fclose(file);
  file = NULL;
  out = open_memstream(&job->request, &job->request_size);
  if (!out)
    goto fail;
  fputs("{\"token\":", out);
  json_bytes(out, token, strlen(token));
  fprintf(out, ",\"pid\":%d,\"status\":%d,\"descriptors\":[", pid, status);
  int pipes = status < 0 ? broker_pipes(out, job) : 0;
  fputs("]}", out);
  int written = ferror(out);
  if (fclose(out))
    written = 1;
  out = NULL;
  if (pipes < 0) {
    broker_free(job);
    return NULL;
  }
  if (written || (job->connection = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0)) < 0 ||
      socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, job->channel) < 0 ||
      pthread_create(&job->thread, NULL, broker_decide, job))
    goto fail;
  *notification = job->channel[0];
  return job;
fail:
  if (file)
    fclose(file);
  if (out)
    fclose(out);
  broker_free(job);
  *notification = -2;
  return NULL;
}
/* 0 records native execution in this domain, 1 resumes, 2 exits, 2+signal
 * delivers a signal. */
int pi_broker_finish(void *context, int cancel) {
  struct broker_job *job = context;
  int result = -1;
  if (cancel)
    pthread_cancel(job->thread);
  pthread_join(job->thread, NULL);
  if (cancel || job->result == -2)
    result = 1;
  else if (job->result == -3)
    result = 0;
  else if (job->result >= 0) {
    unsigned signal = job->result > 256 ? (unsigned)job->result - 256 : 0;
    if (replace_with_exit(job->pid, signal ? 128 + signal : (unsigned)job->result) < 0)
      goto done;
    if (signal) {
      struct user_regs_struct regs;
      unsigned long data[5] = {0, 0, 0, 0, 1UL << (signal - 1)};
      if (ptrace(PTRACE_GETREGS, job->pid, 0, &regs) < 0)
        goto done;
      unsigned long at = (regs.rsp - sizeof(data) - 256) & ~7UL;
      if (image_memory(job->pid, at, data, sizeof(data), 1) < 0 ||
          (signal != SIGKILL && signal != SIGSTOP &&
           image_call(job->pid, 0, SYS_rt_sigaction, signal, at, 0, 8) != 0) ||
          image_call(job->pid, 0, SYS_rt_sigprocmask, SIG_UNBLOCK, at + 32, 0, 8) != 0)
        goto done;
    }
    result = 2 + (int)signal;
  }
done:
  broker_free(job);
  return result;
}

/* Preserve the libuv launcher's empty signal mask and default dispositions. */
static int reset_signals(void) {
  sigset_t empty;
  sigemptyset(&empty);
  if (sigprocmask(SIG_SETMASK, &empty, NULL) < 0)
    return -1;
  for (int number = 1; number < NSIG; number++)
    signal(number, SIG_DFL);
  return 0;
}

/* Hold the sandbox's launch context for its owner after proving a script's read
 * position. */
static int probe_context(const char *root, const char *script) {
  char cwd[PATH_MAX];
  char ready;
  int status;
  if (reset_signals() < 0 || !getcwd(cwd, sizeof(cwd)) || strcmp(cwd, root))
    return 70;
  pid_t child = fork();
  if (child == 0) {
    execl(script, script, (char *)NULL);
    _exit(127);
  }
  if (child < 0 || waitpid(child, &status, 0) < 0 || !WIFEXITED(status) ||
      WEXITSTATUS(status) != 42)
    return 71;
  pid_t pid = getpid();
  return transfer(3, &pid, sizeof(pid), 1) == 0 && transfer(3, &ready, 1, 0) == 0 && ready == 'R'
             ? 0
             : 70;
}

static int open_tracee_output(struct decision_job *job, unsigned fd) {
  int duplicate = duplicate_tracee_fd(job, fd);
  if (duplicate >= 0)
    return duplicate;
  char path[64];
  if (snprintf(path, sizeof(path), "/proc/%ld/fd/%u", (long)job->pid, fd) >= (int)sizeof(path)) {
    errno = ENAMETOOLONG;
    return -1;
  }
  /* This fallback opens a new description; never change flags on a pidfd
   * duplicate. */
  int output = open(path, O_WRONLY | O_CLOEXEC | O_NONBLOCK);
  if (output < 0)
    return -1;
  struct stat state;
  int flags = fcntl(output, F_GETFL);
  if (fstat(output, &state) == 0 && S_ISFIFO(state.st_mode) && flags >= 0 &&
      fcntl(output, F_SETFL, flags & ~(O_NONBLOCK | 0x8000)) >= 0)
    return output;
  close(output);
  return -1;
}

/* Only the tracer thread may mutate a held image. Each job owns its reply
 * channel. */
static int request_tracer(struct decision_job *job, unsigned code) {
  char reply;
  return transfer(job->channel[1], &code, sizeof(code), 1) < 0 ||
                 transfer(job->channel[1], &reply, 1, 0) < 0 || reply != 'Y'
             ? -1
             : 0;
}

/* A nonnegative return keeps the connection until the continued tracee exits.
 */

static int actor_decision(struct decision_job *job) {
  if (descriptor_numbers(job->pid, 3, NULL, 0) != 0)
    return -1;
  int connection = job->connection = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (connection < 0)
    return -1;
  struct sockaddr_un address = {.sun_family = AF_UNIX};
  if (strlen(job->socket_path) >= sizeof(address.sun_path))
    return -1;
  strcpy(address.sun_path, job->socket_path);
  if (connect(connection, (struct sockaddr *)&address, sizeof(address)) < 0)
    return -1;
  char line[PI_HELD_LINE_BYTES];
  int length = snprintf(line, sizeof(line),
                        "{\"token\":\"%s\",\"execution\":\"%s\",\"pid\":%ld,\"tracer\":%"
                        "ld,\"descriptors\":[]}\n",
                        job->token, job->execution_id, (long)job->pid, (long)getpid());
  if (length <= 0 || length >= (int)sizeof(line) ||
      transfer(connection, line, (size_t)length, 1) < 0 ||
      read_line(connection, line, sizeof(line)) < 0)
    return -1;
  if (!strcmp(line, "C"))
    return -1;
  if (!strcmp(line, "F"))
    return -2;
  if (!strcmp(line, "O"))
    return connection;
  unsigned code;
  unsigned positions;
  unsigned resources;
  unsigned physical;
  unsigned source;
  size_t total;
  size_t image;
  if (sscanf(line, "P2 %u %u %zu %u %u %zu %u %u", &code, &job->count, &total, &positions,
             &resources, &image, &physical, &source) != 8 ||
      code > 255 || positions || resources || image || physical || source ||
      job->count > PI_HELD_OUTPUT_EVENTS || total > PI_HELD_OUTPUT_BYTES)
    goto decline;
  job->events = calloc(job->count + 1, sizeof(*job->events));
  if (!job->events)
    goto decline;
  size_t received = 0;
  for (unsigned index = 0; index < job->count; index++) {
    struct output_event *event = &job->events[index];
    if (read_line(connection, line, sizeof(line)) < 0 ||
        sscanf(line, "O %u %zu", &event->fd, &event->length) != 2 ||
        (event->fd != 1 && event->fd != 2) || event->length > total - received)
      goto decline;
    if (job->outputs[event->fd] < 0) {
      pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
      job->outputs[event->fd] = open_tracee_output(job, event->fd);
      pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
      if (job->outputs[event->fd] < 0)
        goto decline;
    }
    if (event->length && (!(event->data = malloc(event->length)) ||
                          transfer(connection, event->data, event->length, 0) < 0))
      goto decline;
    received += event->length;
  }
  if (received != total)
    goto decline;
  if (transfer(connection, "S\n", 2, 1) < 0 || read_line(connection, line, sizeof(line)) < 0 ||
      strcmp(line, "G"))
    return -1;
  if (request_tracer(job, 125) < 0)
    return -2;
  if (transfer(connection, "A\n", 2, 1) < 0 || read_line(connection, line, sizeof(line)) < 0 ||
      strcmp(line, "R"))
    return -2;
  if (job->release_barrier && request_tracer(job, 256) < 0)
    return -2;
  for (unsigned index = 0; index < job->count; index++) {
    struct output_event *event = &job->events[index];
    if (transfer(job->outputs[event->fd], event->data, event->length, 1) < 0) {
      if (errno != EPIPE || request_tracer(job, 512 + SIGPIPE) < 0 ||
          transfer(connection, "D\n", 2, 1) < 0)
        return -2;
      return -1;
    }
  }
  if (request_tracer(job, code) < 0 || transfer(connection, "D\n", 2, 1) < 0)
    return -2;
  return -1;
decline:
  (void)transfer(connection, "N\n", 2, 1);
  return -1;
}

static void *decide_process(void *argument) {
  struct decision_job *job = argument;
  sigset_t blocked;
  sigemptyset(&blocked);
  sigaddset(&blocked, SIGPIPE);
  pthread_sigmask(SIG_BLOCK, &blocked, NULL);
  job->result = actor_decision(job);
  shutdown(job->channel[1], SHUT_WR);
  return NULL;
}

static void free_job(struct decision_job *job) {
  close(job->channel[0]);
  close(job->channel[1]);
  close(job->connection);
  close(job->pidfd);
  for (unsigned fd = 1; fd <= 2; fd++)
    close(job->outputs[fd]);
  free_events(job->events, job->count);
  free(job);
}

static int start_decision(struct traced_process *process, const char *socket_path,
                          const char *token, const char *execution_id, int release_barrier) {
  struct user_regs_struct registers;
  if (ptrace(PTRACE_GETREGS, process->pid, 0, &registers) < 0 || registers.cs != 0x33)
    return -1;
  struct decision_job *job = calloc(1, sizeof(*job));
  if (!job)
    return -1;
  *job = (struct decision_job){.pid = process->pid,
                               .socket_path = socket_path,
                               .token = token,
                               .execution_id = execution_id,
                               .channel = {-1, -1},
                               .connection = -1,
                               .pidfd = -1,
                               .outputs = {-1, -1, -1},
                               .result = -1,
                               .release_barrier = release_barrier};
  if (socketpair(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0, job->channel) < 0 ||
      pthread_create(&job->thread, NULL, decide_process, job) != 0) {
    free_job(job);
    return -1;
  }
  process->job = job;
  return 0;
}

/* Strace and the launch observer share staging, validation and adoption. */
struct actor_trace_job {
  struct traced_process process;
  char configuration[PI_HELD_LINE_BYTES];
};

void *pi_actor_start(pid_t pid, const char *source, int *notification) {
  *notification = -1;
  struct actor_trace_job *job = calloc(1, sizeof(*job));
  if (!job)
    return NULL;
  FILE *file = fopen(source, "re");
  size_t size = file ? fread(job->configuration, 1, sizeof(job->configuration) - 1, file) : 0;
  if (file)
    fclose(file);
  char *socket = job->configuration;
  char *token = memchr(socket, '\n', size);
  char *execution = token ? strchr(token + 1, '\n') : NULL;
  char *end = execution ? strchr(execution + 1, '\n') : NULL;
  job->process.pid = pid;
  job->process.fd = -1;
  if (!token || !execution || !end)
    goto fail;
  *token++ = 0;
  *execution++ = 0;
  *end = 0;
  if (start_decision(&job->process, socket, token, execution, 0) < 0)
    goto fail;
  *notification = job->process.job->channel[0];
  return job;
fail:
  free(job);
  return NULL;
}

/* A reply either advances staging, completes observation, or poisons execution. */
static int advance_decision(struct traced_process *item, int descriptors, pid_t *barrier) {
  struct decision_job *job = item->job;
  unsigned code;
  ssize_t received = recv(job->channel[0], &code, sizeof(code), MSG_DONTWAIT);
  if (received == sizeof(code)) {
    char reply;
    if (code == 256 && descriptors && item->armed) {
      item->pending = 0;
      if (*barrier == item->pid)
        *barrier = 0;
      reply = 'Y';
    } else if (code == 512 + SIGPIPE && item->armed) {
      reply = kill(item->pid, SIGPIPE) < 0 ? 'N' : 'Y';
    } else {
      item->armed = 1;
      reply = code > 255 || replace_with_exit(item->pid, code) < 0 ? 'N' : 'Y';
    }
    return send(job->channel[0], &reply, 1, MSG_NOSIGNAL) == 1 ? 0 : -1;
  }
  if (received == 0) {
    pthread_join(job->thread, NULL);
    int result = job->result;
    if (result >= 0) {
      item->fd = result;
      job->connection = -1;
    }
    free_job(job);
    item->job = NULL;
    return result == -2 ? -1 : 1;
  }
  return received < 0 && (errno == EAGAIN || errno == EINTR) ? 0 : -1;
}

int pi_actor_step(void *context) {
  struct actor_trace_job *job = context;
  pid_t barrier = 0;
  int result = advance_decision(&job->process, 0, &barrier);
  return result == 1 && job->process.armed ? 2 : result;
}

static void finish_observation(struct traced_process *observer, int completed) {
  if (observer->job) {
    pthread_cancel(observer->job->thread);
    pthread_join(observer->job->thread, NULL);
    free_job(observer->job);
    observer->job = NULL;
  }
  if (observer->fd >= 0) {
    (void)transfer(observer->fd, completed ? "D\n" : "C\n", 2, 1);
    close(observer->fd);
    observer->fd = -1;
  }
}

void pi_actor_close(void *context, int completed) {
  struct actor_trace_job *job = context;
  finish_observation(&job->process, completed);
  free(job);
}

static int track_process(struct traced_process **processes, pid_t pid) {
  struct traced_process *observer = malloc(sizeof(*observer));
  if (!observer)
    return -1;
  *observer = (struct traced_process){.pid = pid, .fd = -1, .next = *processes};
  *processes = observer;
  return 0;
}

static void release_process(struct traced_process **processes, pid_t pid, int completed) {
  struct traced_process **cursor = processes;
  while (*cursor) {
    struct traced_process *observer = *cursor;
    if (observer->pid != pid) {
      cursor = &observer->next;
      continue;
    }
    *cursor = observer->next;
    finish_observation(observer, completed);
    free(observer);
  }
}

/* No decision remains. Detach ready tracees; blocked vfork parents stay
 * protected until VFORK_DONE.
 */
static int release_tracees(struct traced_process **processes, int *acknowledged) {
  int live = 0;
  int waiting = 0;
  for (struct traced_process *item = *processes; item;) {
    if (!item->historical) {
      if (item->stop == TRACE_STOPPED && !item->awaiting_parent) {
        if (ptrace(PTRACE_DETACH, item->pid, 0, item->delivered) < 0 && errno != ESRCH)
          return -1;
        release_process(processes, item->pid, 0);
        item = *processes;
        live = waiting = 0;
        continue;
      }
      live = 1;
      if (!item->vforking) {
        waiting = 1;
        if (ptrace(PTRACE_INTERRUPT, item->pid, 0, 0) < 0 && errno != ESRCH && errno != EIO)
          return -1;
      }
    }
    item = item->next;
  }
  if (!waiting && !*acknowledged) {
    for (struct traced_process *item = *processes; item; item = item->next)
      finish_observation(item, 0);
    (void)transfer(STDOUT_FILENO, "R", 1, 1);
    close(STDOUT_FILENO);
    *acknowledged = 1;
  }
  return live;
}

/* An ordinary caller cannot lend either saved IDs or capabilities to a reusable
 * image. */
static int process_owned(pid_t pid, struct process_credentials *value) {
  if (process_credentials(pid, value) < 0 || value->permitted || value->effective ||
      value->inheritable || value->ambient)
    return 0;
  for (unsigned i = 0; i < 4; i++)
    if (value->uid[i] != getuid() || value->gid[i] != getgid())
      return 0;
  return 1;
}

static int attach_owned(pid_t root, long flags) {
  struct ucred peer;
  socklen_t length = sizeof(peer);
  struct process_credentials caller;
  struct process_credentials child;
  struct process_credentials observer;
  if (getsockopt(3, SOL_SOCKET, SO_PEERCRED, &peer, &length) < 0 || length != sizeof(peer) ||
      peer.pid != getppid() || peer.uid != getuid() || peer.gid != getgid() ||
      !process_owned(peer.pid, &caller) || !process_owned(root, &child) ||
      child.parent != (unsigned)peer.pid || process_credentials(getpid(), &observer) < 0)
    return -1;
  unsigned long long extra = 1ULL << CAP_SYS_PTRACE;
  if (!child.no_new_privileges && !(observer.effective & extra))
    return -1;
  if (observer.permitted & ~extra || observer.effective & ~extra || observer.inheritable ||
      observer.ambient)
    return -1;
  int parent = syscall(SYS_pidfd_open, peer.pid, 0);
  int target = syscall(SYS_pidfd_open, root, 0);
  int result = -1;
  struct pollfd alive[2] = {{.fd = parent, .events = POLLIN}, {.fd = target, .events = POLLIN}};
  if (parent >= 0 && target >= 0 && poll(alive, 2, 0) == 0 &&
      ptrace(PTRACE_SEIZE, root, 0, flags & ~PTRACE_O_EXITKILL) == 0) {
    struct __user_cap_header_struct header = {_LINUX_CAPABILITY_VERSION_3, 0};
    struct __user_cap_data_struct empty[2] = {{0}, {0}};
    if (syscall(SYS_capset, &header, empty) == 0 && poll(alive, 2, 0) == 0 &&
        ptrace(PTRACE_INTERRUPT, root, 0, 0) == 0)
      result = 0;
  }
  close(parent);
  close(target);
  return result;
}

static int trace(const char *socket_path, const char *token, const char *execution_id, int skip,
                 unsigned skip_code, int descriptors, int skip_root) {
  pid_t barrier = 0;
  sigset_t blocked;
  sigset_t original;
  sigemptyset(&blocked);
  sigaddset(&blocked, SIGCHLD);
  sigaddset(&blocked, SIGPIPE);
  if (pthread_sigmask(SIG_BLOCK, &blocked, &original) != 0)
    return 70;
  int signals = signalfd(-1, &blocked, SFD_CLOEXEC | SFD_NONBLOCK);
  if (signals < 0)
    return 70;
  pid_t root;
  if (transfer(3, &root, sizeof(root), 0) < 0 || root <= 0)
    return 71;
  long flags = options | (descriptors ? PTRACE_O_TRACESYSGOOD : 0);
  if (attach_owned(root, flags) < 0)
    return 72;
  int status = 0;
  int root_status = -1;
  enum { OBSERVING, RETIRE_REQUESTED, DRAINING } phase = OBSERVING;
  int acknowledged = 0;
  int admitted = 0;
  int launched = 0;
  struct traced_process *processes = NULL;
  struct pollfd *polling = NULL;
  size_t capacity = 0;
  if (track_process(&processes, root) < 0) {
    kill(root, SIGKILL);
    goto fatal;
  }
  for (;;) {
    struct pollfd control = {.fd = STDIN_FILENO, .events = POLLIN};
    if (phase == OBSERVING && poll(&control, 1, 0) > 0)
      phase = RETIRE_REQUESTED;
    struct signalfd_siginfo notification;
    while (read(signals, &notification, sizeof(notification)) > 0) {
    }
    for (struct traced_process *item = processes; item; item = item->next) {
      struct decision_job *job = item->job;
      if (!job)
        continue;
      int result = advance_decision(item, descriptors, &barrier);
      if (result < 0)
        goto fatal;
      if (result > 0) {
        item->armed = 0;
        if (descriptors) {
          item->pending = 0;
          if (barrier == item->pid)
            barrier = 0;
        } else {
          if (ptrace(PTRACE_CONT, item->pid, 0, 0) < 0 && errno != ESRCH)
            goto fatal;
          item->stop = TRACE_RUNNING;
        }
      }
    }
  manage_descriptors:
    if (descriptors && phase != DRAINING) {
      if (!barrier)
        for (struct traced_process *item = processes; item; item = item->next)
          if (item->pending && !item->historical) {
            barrier = item->pid;
            break;
          }
      int ready = 1;
      for (struct traced_process *item = processes; item; item = item->next) {
        /* Between its vfork event and VFORK_DONE a parent runs nothing: it
         * cannot stop, and needs not. */
        if (item->historical || item->stop != TRACE_RUNNING || item->vforking)
          continue;
        ready = 0;
        if (barrier && ptrace(PTRACE_INTERRUPT, item->pid, 0, 0) < 0 && errno != ESRCH &&
            errno != EIO)
          goto fatal;
      }
      for (struct traced_process *item = processes; item; item = item->next) {
        if (item->historical || item->stop == TRACE_RUNNING)
          continue;
        if (barrier) {
          if (ready && item->pid == barrier && !item->job &&
              start_decision(item, socket_path, token, execution_id, 1) < 0) {
            item->pending = 0;
            barrier = 0;
            goto manage_descriptors;
          }
        } else if (!item->job && !item->awaiting_parent) {
          if (ptrace(item->stop == TRACE_GROUP_STOP ? PTRACE_LISTEN : PTRACE_CONT, item->pid, 0, item->delivered) <
                  0 &&
              errno != ESRCH)
            goto fatal;
          item->stop = TRACE_RUNNING;
          item->delivered = 0;
        }
      }
    }
    if (phase != OBSERVING || (root_status >= 0 && !barrier)) {
      int busy = 0;
      for (struct traced_process *item = processes; item; item = item->next)
        busy |= item->job != NULL;
      if (!busy) {
        phase = DRAINING;
        int live = release_tracees(&processes, &acknowledged);
        if (live < 0)
          goto fatal;
        if (!live)
          break;
      }
    }
    pid_t pid = waitpid(-1, &status, __WALL | WNOHANG | __WNOTHREAD);
    if (pid < 0) {
      if (errno == EINTR)
        continue;
      if (errno == ECHILD)
        break;
      goto fatal;
    }
    if (pid == 0) {
      size_t count = 2;
      for (struct traced_process *item = processes; item; item = item->next)
        if (item->job)
          count++;
      if (count > capacity) {
        struct pollfd *grown = realloc(polling, count * sizeof(*polling));
        if (!grown)
          goto fatal;
        polling = grown;
        capacity = count;
      }
      polling[0] = (struct pollfd){.fd = signals, .events = POLLIN};
      polling[1] = (struct pollfd){.fd = phase != OBSERVING ? -1 : STDIN_FILENO, .events = POLLIN};
      count = 2;
      for (struct traced_process *item = processes; item; item = item->next)
        if (item->job)
          polling[count++] = (struct pollfd){.fd = item->job->channel[0], .events = POLLIN};
      if (poll(polling, count, -1) < 0 && errno != EINTR)
        goto fatal;
      continue;
    }
    if (WIFEXITED(status) || WIFSIGNALED(status)) {
      if (pid == barrier)
        barrier = 0;
      if (pid == root)
        root_status = status;
      for (struct traced_process *item = processes; item; item = item->next)
        if (item->pid == pid && item->armed)
          goto fatal;
      release_process(&processes, pid, 1);
      continue;
    }
    if (!WIFSTOPPED(status))
      continue;
    unsigned event = (unsigned)status >> 16;
    int delivered = WSTOPSIG(status);
    struct traced_process *current = processes;
    while (current && (current->pid != pid || current->historical))
      current = current->next;
    /* waitpid may deliver an automatically attached child's stop before its
     * parent's fork event. */
    if (!current) {
      if (track_process(&processes, pid) < 0)
        goto fatal;
      current = processes;
      current->awaiting_parent = event == PTRACE_EVENT_STOP;
    }
    current->stop = TRACE_STOPPED;
    struct process_credentials credentials;
    if (!admitted) {
      int valid = phase == OBSERVING && pid == root && event == PTRACE_EVENT_STOP &&
                  process_owned(pid, &credentials) && credentials.parent == (unsigned)getppid();
      if (valid && ptrace(PTRACE_SETOPTIONS, root, 0, flags) < 0)
        goto fatal;
      if (!valid)
        phase = DRAINING;
      (void)transfer(3, valid ? "R" : "N", 1, 1);
      close(3);
      admitted = 1;
    }
    if (event == PTRACE_EVENT_EXEC && !process_owned(pid, &credentials))
      phase = DRAINING;
    /* libuv's fork still contains the host's CLOEXEC pins. Start ancestry at
     * the actual launch, where only descriptors inherited by the program
     * remain. */
    int target_exec = event == PTRACE_EVENT_EXEC && (launched || !skip_root);
    if (event == PTRACE_EVENT_EXEC)
      launched = 1;
    if (event == PTRACE_EVENT_VFORK || event == PTRACE_EVENT_VFORK_DONE)
      current->vforking = event == PTRACE_EVENT_VFORK;
    if (descriptors && delivered == (SIGTRAP | 0x80))
      delivered = 0;
    if (event == PTRACE_EVENT_FORK || event == PTRACE_EVENT_VFORK || event == PTRACE_EVENT_CLONE) {
      if (!launched && descriptors)
        phase = DRAINING;
      unsigned long child;
      if (ptrace(PTRACE_GETEVENTMSG, pid, 0, &child) < 0)
        goto fatal;
      struct traced_process *known = processes;
      while (known && known->pid != (pid_t)child)
        known = known->next;
      if (!known && track_process(&processes, (pid_t)child) < 0)
        goto fatal;
      if (!known)
        known = processes;
      known->awaiting_parent = 0;
    }
    if (phase != DRAINING && event == PTRACE_EVENT_STOP && delivered != SIGTRAP) {
      if (descriptors) {
        current->stop = TRACE_GROUP_STOP;
        continue;
      }
      if (ptrace(PTRACE_LISTEN, pid, 0, 0) < 0 && errno != ESRCH)
        goto fatal;
      current->stop = TRACE_RUNNING;
      continue;
    }
    if (event == PTRACE_EVENT_EXEC) {
      unsigned long previous;
      if (ptrace(PTRACE_GETEVENTMSG, pid, 0, &previous) < 0)
        goto fatal;
      /* A non-leader exec replaces its TID without a separate death
       * notification. */
      if ((pid_t)previous != pid) {
        release_process(&processes, (pid_t)previous, 0);
      }
    }
    if (target_exec) {
      if (phase != DRAINING && skip && replace_with_exit(pid, skip_code) < 0)
        goto fatal;
      if (socket_path && phase == OBSERVING) {
        for (struct traced_process *item = processes; item; item = item->next) {
          if (item->pid != pid)
            continue;
          /* Earlier images still own completion of this process, including
           * later execs.
           */
          if (item->fd >= 0) {
            if (track_process(&processes, pid) < 0)
              goto fatal;
            item->historical = 1;
            item = processes;
            item->stop = TRACE_STOPPED;
          }
          if (descriptors) {
            item->pending = 1;
            goto held;
          }
          if (start_decision(item, socket_path, token, execution_id, 0) == 0)
            goto held;
          break;
        }
      }
    }
    if (event != 0)
      delivered = 0;
    if (descriptors || phase == DRAINING) {
      current->delivered = delivered;
      continue;
    }
    if (ptrace(PTRACE_CONT, pid, 0, delivered) < 0 && errno != ESRCH)
      goto fatal;
    current->stop = TRACE_RUNNING;
  held:;
  }
  while (processes)
    release_process(&processes, processes->pid, 0);
  free(polling);
  close(signals);
  pthread_sigmask(SIG_SETMASK, &original, NULL);
  if (root_status < 0)
    return phase != OBSERVING ? 0 : 125;
  if (WIFEXITED(root_status))
    return WEXITSTATUS(root_status);
  signal(WTERMSIG(root_status), SIG_DFL);
  raise(WTERMSIG(root_status));
  return 128 + WTERMSIG(root_status);
fatal:
  for (struct traced_process *item = processes; item; item = item->next)
    kill(item->pid, SIGKILL);
  while (waitpid(-1, &status, __WALL) > 0 || errno == EINTR) {
  }
  while (processes)
    release_process(&processes, processes->pid, 0);
  free(polling);
  close(signals);
  return 125;
}

/* Reproduce a complete inherited FD table at the existing sandboxed native
 * outlet. The supervisor alone keeps the pins, and drains descendants before
 * reporting offsets. */

static int execute_descriptors(const char *manifest, const char *report, char *executable,
                               char **command, const char *route, unsigned closed_input,
                               const int *output_flags, const rlim_t *stack_limit) {
  (void)report;
  if (manifest || route[2])
    return 65;
  if (closed_input && close(0) < 0 && errno != EBADF)
    return 70;
  for (int fd = 1; fd <= 2; fd++) {
    if (output_flags[fd - 1] < 0)
      continue;
    int flags = fcntl(fd, F_GETFL);
    if (flags < 0 || fcntl(fd, F_SETFL,
                           (flags & ~(O_APPEND | O_NONBLOCK)) |
                               (output_flags[fd - 1] & (O_APPEND | O_NONBLOCK))) < 0)
      return 70;
  }
  if (stack_limit) {
    struct rlimit limit;
    if (getrlimit(RLIMIT_STACK, &limit) < 0)
      return 70;
    limit.rlim_cur = *stack_limit;
    if (setrlimit(RLIMIT_STACK, &limit) < 0)
      return 70;
  }
  execv(executable, command);
  return errno == ENOENT ? 127 : 126;
}

int main(int argc, char **argv) {
  int observing =
      argc > 1 && (!strcmp(argv[1], "--attach") || !strcmp(argv[1], "--attach-skip-code") ||
                   !strcmp(argv[1], "--probe-observation"));
  if ((PI_SPEC_AUTHORITY_ONLY ? !observing : getauxval(AT_SECURE)) ||
      (observing && prctl(PR_SET_DUMPABLE, 0, 0, 0, 0) < 0))
    return 64;
  if (argc >= 3 && !strncmp(argv[1], "--output-channel=", 17)) {
    const char *text = argv[1] + 17;
    char *end;
    errno = 0;
    unsigned long channel = strtoul(text, &end, 10);
    if (!*text || errno || *end || channel < 3 || channel > INT_MAX)
      return 64;
    uint64_t endpoints[2];
    struct stat state;
    char ready;
    for (unsigned index = 0; index < 2; index++) {
      if (fstat(index + 1, &state) || !S_ISSOCK(state.st_mode))
        return 70;
      endpoints[index] = htole64(state.st_ino);
    }
    /* The parent records these inherited objects before any program can replace
     * them. */
    if (transfer((int)channel, endpoints, sizeof(endpoints), 1) ||
        transfer((int)channel, &ready, 1, 0) || ready != 'R')
      return 70;
    close((int)channel);
    execvp(argv[2], argv + 2);
    return 127;
  }
  if (argc >= 5 && !strcmp(argv[1], "--execution-domain")) {
    char *end;
    errno = 0;
    unsigned long long id = strtoull(argv[2], &end, 10);
    if (!id || errno || *end || strspn(argv[2], "0123456789") != strlen(argv[2]))
      return 64;
    struct pi_domain_packet packet = {.operation = PI_DOMAIN_JOIN, .domain = id};
    if (pi_domain_exchange(argv[3], &packet))
      return 70;
    execvp(argv[4], argv + 4);
    return 127;
  }
  if (argc == 2 && !strcmp(argv[1], "--probe-observation")) {
    struct process_credentials credentials;
    unsigned long long extra = 1ULL << CAP_SYS_PTRACE;
    return process_credentials(getpid(), &credentials) == 0 && !(credentials.permitted & ~extra) &&
                   !(credentials.effective & ~extra) && !credentials.inheritable &&
                   !credentials.ambient &&
                   (credentials.no_new_privileges || (credentials.effective & extra))
               ? 0
               : 77;
  }
  if (argc >= 2 && (!strcmp(argv[1], "--attach") || !strcmp(argv[1], "--attach-skip-code"))) {
    int probe = !strcmp(argv[1], "--attach-skip-code");
    if (probe && (argc != 3 || !*argv[2] || strspn(argv[2], "0123456789") != strlen(argv[2]) ||
                  strtoul(argv[2], NULL, 10) > 255))
      return 64;
    char *socket = getenv("PI_SPEC_HELD_EXEC_SOCKET");
    char *token = getenv("PI_SPEC_HELD_EXEC_TOKEN");
    char *id = getenv("PI_SPEC_HELD_EXEC_ID");
    char *descriptors = getenv("PI_SPEC_HELD_EXEC_DESCRIPTORS");
    char *skip = getenv("PI_SPEC_HELD_EXEC_ROOT");
    if (!probe && (!socket || !token || !id))
      return 64;
    return trace(socket, token, id, probe, probe ? (unsigned)atoi(argv[2]) : 0,
                 descriptors && !strcmp(descriptors, "1"), !!skip);
  }
  if (argc >= 2 && (!strcmp(argv[1], "--exec") || !strcmp(argv[1], "--exec-closed-input") ||
                    !strcmp(argv[1], "--exec-fds"))) {
    int descriptors = !strcmp(argv[1], "--exec-fds");
    int name = descriptors ? 5 : 3;
    int output_flags[2] = {-1, -1};
    /* ",<stdout>,<stderr>": status flags the target's outputs carry natively,
     * its parent's own (make's O_APPEND). */
    char *suffix = argc > 2 ? strchr(argv[2], ',') : NULL;
    if (suffix &&
        (*suffix++ = 0, sscanf(suffix, "%d,%d", &output_flags[0], &output_flags[1]) != 2 ||
                            output_flags[0] < 0 || output_flags[1] < 0))
      return 64;
    char *stack = argc > 2 ? strchr(argv[2], ':') : NULL;
    rlim_t stack_limit = 0;
    if (stack) {
      *stack++ = 0;
      char *end = stack + strlen(stack);
      errno = 0;
      stack_limit = !strcmp(stack, "unlimited") ? RLIM_INFINITY : strtoull(stack, &end, 10);
      if ((!isdigit((unsigned char)*stack) && strcmp(stack, "unlimited")) || *end || errno)
        return 64;
    }
    if (argc < name + 2 || strspn(argv[2], "012") != 2 ||
        (strlen(argv[2]) != 2 &&
         (!descriptors || strlen(argv[2]) != 4 || strspn(argv[2] + 2, "ps") != 2 ||
          (argv[2][0] == argv[2][1] && argv[2][2] != argv[2][3]))))
      return 64;
    /* Save stdout's source before changing either endpoint, including swapped
     * routes. Route 0 discards into /dev/null; Node fills vacant stdio, so the
     * device opens above it.
     */
    int discard = memchr(argv[2], '0', 2) ? open("/dev/null", O_WRONLY | O_CLOEXEC) : -1;
    int output = fcntl(argv[2][0] == '0' ? discard : argv[2][0] - '0', F_DUPFD_CLOEXEC, 3);
    if (output < 0)
      return 70;
    int routed =
        dup2(argv[2][1] == '0' ? discard : argv[2][1] - '0', 2) >= 0 && dup2(output, 1) >= 0;
    close(output);
    close(discard);
    if (!routed)
      return 70;
    if (reset_signals() < 0)
      return 70;
    char *executable = argv[name + 1];
    char *manifest = descriptors ? argv[3] : NULL;
    char *report = descriptors ? argv[4] : NULL;
    argv[name + 1] = argv[name];
    return execute_descriptors(manifest, report, executable, argv + name + 1, argv[2],
                               !strcmp(argv[1], "--exec-closed-input"), output_flags,
                               stack ? &stack_limit : NULL);
  }
  if (argc == 4 && !strcmp(argv[1], "--probe-context"))
    return probe_context(argv[2], argv[3]);
  if (argc == 2 && !strcmp(argv[1], "--probe-clean-fds")) {
    int extra = has_unmodeled_descriptors(3);
    return extra < 0 ? 70 : extra ? 65 : 0;
  }
  return 64;
}
