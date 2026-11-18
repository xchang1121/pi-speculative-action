#define _GNU_SOURCE
#include "linux-execution-domain.h"
#include <dirent.h>
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
#include <linux/kcmp.h>
#include <linux/magic.h>
#include <linux/mount.h>
#include <linux/openat2.h>
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
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <sys/user.h>
#include <sys/vfs.h>
#include <sys/wait.h>
#include <unistd.h>

static const long options = PTRACE_O_TRACEFORK | PTRACE_O_TRACEVFORK | PTRACE_O_TRACEEXIT |
                            PTRACE_O_TRACECLONE | PTRACE_O_TRACEEXEC;

/* Weak Node-API imports let the standalone helper and filesystem addon share this source. */
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

static FILE *process_file(pid_t pid, const char *name) {
  char path[64];
  snprintf(path, sizeof(path), "/proc/%d/%s", pid, name);
  return fopen(path, "re");
}

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

/* Read credentials only for the held child of this observer. */

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
 * protected until their child execs or exits.
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
      waiting |= !item->vforking;
      if (ptrace(PTRACE_INTERRUPT, item->pid, 0, 0) < 0 && errno != ESRCH && errno != EIO)
        return -1;
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
      ptrace(PTRACE_SEIZE, root, 0, flags) == 0) {
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
  long flags = options;
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
        /* While its vfork child retains the address space a parent runs nothing: it
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
    if (event == PTRACE_EVENT_EXEC || event == PTRACE_EVENT_EXIT)
      for (struct traced_process *item = processes; item; item = item->next)
        if (item->vforking == pid)
          item->vforking = 0;
    if (event == PTRACE_EVENT_EXIT) {
      if (ptrace(PTRACE_CONT, pid, 0, 0) < 0 && errno != ESRCH)
        goto fatal;
      current->stop = TRACE_RUNNING;
      continue;
    }
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
      if (event == PTRACE_EVENT_VFORK)
        current->vforking = (pid_t)child;
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
