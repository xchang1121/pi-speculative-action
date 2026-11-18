#define main collector_main
#include "../src/linux-execution-domain.c"
#undef main
#include <assert.h>
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

static void probe(void *page) {
  for (int i = 0; i < 32; i++) {
    assert(!mprotect(page, 4096, PROT_READ | PROT_WRITE));
    syscall(SYS_pkey_mprotect, page, 4096, PROT_READ | PROT_WRITE, 0);
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
  void *page = mmap(NULL, 4096, PROT_READ, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
  assert(page != MAP_FAILED && !mprotect(page, 4096, PROT_READ));
  syscall(SYS_pkey_mprotect, page, 4096, PROT_READ, 0);
  if (!strcmp(argv[1], "write")) {
    pid_t child = fork();
    assert(child >= 0);
    probe(page);
    if (!child)
      _exit(0);
    int status;
    assert(waitpid(child, &status, 0) == child && status == 0);
  }
  assert(!munmap(page, 4096));
  return 0;
}
