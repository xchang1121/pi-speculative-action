#define main collector_main
#include "../src/linux-execution-domain.c"
#undef main
#include <assert.h>
#include <asm/prctl.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <linux/futex.h>
#include <sys/syscall.h>

static void quotas(void) {
  unsigned char buffers[2][PI_DOMAIN_TRACE_BYTES + 8] = {};
  struct pi_domain_event event = {};
  for (unsigned i = 0; i < 2; i++) {
    groups[i] = (struct domain){.id = i + 1, .events = buffers[i]};
    event.owner = i + 1;
    assert(!metadata_record(NULL, &event, 48));
  }
  event.size = 1;
  event.bytes[0] = 42;
  for (unsigned i = 0; i < 2; i++) {
    event.owner = i + 1;
    assert(!metadata_record(NULL, &event, 49));
    assert(groups[i].event_count == 2 && !groups[i].event_lost);
    assert(groups[i].event_bytes == PI_DOMAIN_TRACE_BYTES && buffers[i][96] == 42);
    assert(!metadata_record(NULL, &event, 49));
    assert(groups[i].event_count == 2 && groups[i].event_lost);
    assert(groups[i].event_bytes == PI_DOMAIN_TRACE_BYTES && !buffers[i][PI_DOMAIN_TRACE_BYTES]);
  }
}

static void unknown_calls(void) {
  for (int i = 0; i < 32; i++) {
    syscall(SYS_inotify_add_watch, -1, ".", 0);
    syscall(SYS_fanotify_mark, -1, 0, 0, -1, NULL);
    syscall(SYS_mq_unlink, NULL);
    syscall(SYS_mq_notify, -1, NULL);
    syscall(SYS_rt_sigqueueinfo, -1, -1, NULL);
    syscall(SYS_rt_tgsigqueueinfo, -1, -1, -1, NULL);
    syscall(SYS_landlock_create_ruleset, NULL, 0, 1);
    syscall(70000);
  }
}

int main(int argc, char **argv) {
  assert(argc == 2);
  if (!strcmp(argv[1], "quota")) {
    quotas();
    return 0;
  }
  if (!strcmp(argv[1], "fd")) {
    assert(!ftruncate(3, 4096) && !fchmod(3, 0600));
    assert(!syscall(SYS_utimensat, 3, NULL, NULL, 0));
    void *shared = mmap(NULL, 4096, PROT_READ | PROT_WRITE, MAP_SHARED, 3, 0);
    assert(shared != MAP_FAILED && !munmap(shared, 4096));
    return 0;
  }
  if (!strcmp(argv[1], "unknown")) {
    pid_t child = fork();
    assert(child >= 0);
    unknown_calls();
    if (!child)
      _exit(0);
    int status;
    assert(waitpid(child, &status, 0) == child && status == 0);
    return 0;
  }
  int shared = !strncmp(argv[1], "shared", 6);
  void *page = mmap(NULL, 4096, PROT_READ, shared ? MAP_SHARED : MAP_PRIVATE | MAP_ANONYMOUS,
                    shared ? 3 : -1, 0);
  assert(page != MAP_FAILED);
  if (!strcmp(argv[1], "shared-futex")) {
    assert(syscall(SYS_futex, page, FUTEX_WAKE, 1, NULL, NULL, 0) == 0);
  } else if (!strcmp(argv[1], "remove")) {
    madvise(page, 4096, MADV_REMOVE);
  } else {
    assert(!madvise(page, 4096, MADV_NORMAL));
    if (strcmp(argv[1], "shared-pkey"))
      assert(!mprotect(page, 4096, PROT_READ | PROT_WRITE));
    if (strcmp(argv[1], "shared"))
      assert(!syscall(SYS_pkey_mprotect, page, 4096, PROT_READ | PROT_WRITE, -1));
    if (!shared) {
      assert(mremap(page, 4096, 4096, 0) == page);
      unsigned long fs;
      assert(!syscall(SYS_arch_prctl, ARCH_GET_FS, &fs));
      syscall(SYS_arch_prctl, ARCH_GET_CPUID, 0);
      syscall(SYS_arch_prctl, 0x3001 /* ARCH_CET_STATUS */, &fs);
      int epoll = epoll_create1(EPOLL_CLOEXEC), event = eventfd(1, EFD_CLOEXEC);
      struct epoll_event ready = {.events = EPOLLIN};
      assert(epoll >= 0 && event >= 0 && !epoll_ctl(epoll, EPOLL_CTL_ADD, event, &ready));
      assert(epoll_wait(epoll, &ready, 1, 0) == 1);
      assert(!close(event) && !close(epoll));
      *(volatile int *)page = 1;
      assert(syscall(SYS_futex, page, FUTEX_WAKE, 1, NULL, NULL, 0) == 0);
    }
  }
  assert(!munmap(page, 4096));
  return 0;
}
