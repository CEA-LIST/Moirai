/* NW.22 instrument: LD_PRELOAD library that dumps the calling thread's
 * backtrace on SIGUSR2, into $NW22_BT_DIR/bt.<pid>.txt (appended).
 * Inert unless NW22_BT_DIR is set; installs nothing else. No ptrace needed
 * (kernel.yama.ptrace_scope=2 on this machine forbids attaching).
 * The harness sends SIGUSR2 to each thread of a stalled node with tgkill. */
#define _GNU_SOURCE
#include <errno.h>
#include <execinfo.h>
#include <fcntl.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/prctl.h>
#include <sys/syscall.h>
#include <unistd.h>

static char out_path[600];
static int armed = 0;

static size_t put_str(char *dst, size_t at, const char *s) {
    while (*s && at < 500) dst[at++] = *s++;
    return at;
}

static size_t put_num(char *dst, size_t at, long v) {
    char tmp[24];
    int n = 0;
    if (v == 0) tmp[n++] = '0';
    while (v > 0 && n < 23) { tmp[n++] = (char)('0' + v % 10); v /= 10; }
    while (n > 0 && at < 500) dst[at++] = tmp[--n];
    return at;
}

static void handler(int sig, siginfo_t *si, void *uc) {
    (void)sig; (void)si; (void)uc;
    int saved = errno;
    void *frames[128];
    int n = backtrace(frames, 128);
    int fd = open(out_path, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd >= 0) {
        char head[512];
        char name[17] = {0};
        prctl(PR_GET_NAME, name, 0, 0, 0);
        size_t at = 0;
        at = put_str(head, at, "=== thread tid=");
        at = put_num(head, at, (long)syscall(SYS_gettid));
        at = put_str(head, at, " name=");
        at = put_str(head, at, name);
        at = put_str(head, at, "\n");
        (void)!write(fd, head, at);
        backtrace_symbols_fd(frames, n, fd);
        close(fd);
    }
    errno = saved;
}

__attribute__((constructor)) static void nw22_init(void) {
    const char *dir = getenv("NW22_BT_DIR");
    if (!dir || !*dir || armed) return;
    size_t at = 0;
    at = put_str(out_path, at, dir);
    at = put_str(out_path, at, "/bt.");
    at = put_num(out_path, at, (long)getpid());
    at = put_str(out_path, at, ".txt");
    out_path[at] = 0;
    void *warm[2];
    backtrace(warm, 2); /* load libgcc_s now, never inside the handler */
    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_sigaction = handler;
    sa.sa_flags = SA_SIGINFO | SA_RESTART;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGUSR2, &sa, NULL);
    armed = 1;
}
