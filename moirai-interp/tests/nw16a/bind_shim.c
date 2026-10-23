/*
 * NW.16a round 2, P5a and P5b (vault spec/items/NW.16a.md, "Round 2, Cam's
 * ruling A"): a library the P5 tests build and preload into one model_node
 * process (LD_PRELOAD), so that they read and steer its TCP binds from
 * inside without touching its code. It wraps two C library calls, bind and
 * rename, and changes nothing else. It prints nothing on the node's output.
 *
 * NW16A_BIND_LOG=<file>
 *     Every successful bind of a TCP socket (AF_INET or AF_INET6) appends
 *     "bind <port asked> <port got> <socket inode>\n"; every bind refused by
 *     the gap below appends "refused <port>\n".
 *
 * NW16A_GAP=1
 *     A bind to a port that an earlier bind to port 0 in this process
 *     obtained fails with EADDRINUSE, as the kernel answers it when another
 *     process took the port after the node let it go. This is P5b's gap made
 *     certain: the kernel chose the port, the node reported it and released
 *     it, and someone else holds it when the Zenoh session binds it again.
 *     A process that keeps the socket it bound never meets it.
 *
 * NW16A_HOLD_AT=<file>
 *     A rename onto <file> (PORTS_FILE: NW.02's contract writes it beside and
 *     renames it) completes, creates <file>.held, then the thread that
 *     renamed waits until <file>.go exists, 120 s at most: the node stands
 *     still at the moment its PORTS_FILE appears while the test reads the
 *     kernel's tables.
 *
 * Built by the tests with: cc -shared -fPIC -O2 -o <scratch>/x.so bind_shim.c -ldl -pthread
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

typedef int (*bind_fn)(int, const struct sockaddr *, socklen_t);
typedef int (*rename_fn)(const char *, const char *);

static pthread_mutex_t chosen_lock = PTHREAD_MUTEX_INITIALIZER;
static unsigned short chosen[1024];
static size_t chosen_count;

/* The port of an AF_INET or AF_INET6 address; 0 for any other family. */
static int inet_port(const struct sockaddr *addr, socklen_t len, unsigned short *port)
{
    if (addr == NULL)
        return 0;
    if (addr->sa_family == AF_INET && len >= (socklen_t)sizeof(struct sockaddr_in)) {
        *port = ntohs(((const struct sockaddr_in *)addr)->sin_port);
        return 1;
    }
    if (addr->sa_family == AF_INET6 && len >= (socklen_t)sizeof(struct sockaddr_in6)) {
        *port = ntohs(((const struct sockaddr_in6 *)addr)->sin6_port);
        return 1;
    }
    return 0;
}

static int is_stream(int fd)
{
    int type = 0;
    socklen_t len = sizeof type;
    return getsockopt(fd, SOL_SOCKET, SO_TYPE, &type, &len) == 0 && type == SOCK_STREAM;
}

static void log_line(const char *line)
{
    const char *path = getenv("NW16A_BIND_LOG");
    if (path == NULL || *path == '\0')
        return;
    int fd = open(path, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
    if (fd < 0)
        return;
    ssize_t written = write(fd, line, strlen(line));
    (void)written;
    close(fd);
}

static int gap_on(void)
{
    const char *gap = getenv("NW16A_GAP");
    return gap != NULL && strcmp(gap, "1") == 0;
}

static int was_chosen(unsigned short port)
{
    int found = 0;
    pthread_mutex_lock(&chosen_lock);
    for (size_t i = 0; i < chosen_count; i++)
        if (chosen[i] == port)
            found = 1;
    pthread_mutex_unlock(&chosen_lock);
    return found;
}

static void note_chosen(unsigned short port)
{
    pthread_mutex_lock(&chosen_lock);
    if (chosen_count < sizeof chosen / sizeof chosen[0])
        chosen[chosen_count++] = port;
    pthread_mutex_unlock(&chosen_lock);
}

int bind(int fd, const struct sockaddr *addr, socklen_t len)
{
    static bind_fn real_bind;
    if (real_bind == NULL)
        real_bind = (bind_fn)dlsym(RTLD_NEXT, "bind");
    if (real_bind == NULL) {
        errno = ENOSYS;
        return -1;
    }

    unsigned short asked = 0;
    int tcp = inet_port(addr, len, &asked) && is_stream(fd);
    if (tcp && asked != 0 && gap_on() && was_chosen(asked)) {
        char line[64];
        snprintf(line, sizeof line, "refused %u\n", (unsigned)asked);
        log_line(line);
        errno = EADDRINUSE;
        return -1;
    }

    int result = real_bind(fd, addr, len);
    if (result == 0 && tcp) {
        int saved = errno;
        struct sockaddr_storage local;
        socklen_t local_len = sizeof local;
        unsigned short got = 0;
        if (getsockname(fd, (struct sockaddr *)&local, &local_len) == 0)
            inet_port((const struct sockaddr *)&local, local_len, &got);
        unsigned long long inode = 0;
        struct stat st;
        if (fstat(fd, &st) == 0)
            inode = (unsigned long long)st.st_ino;
        if (asked == 0 && got != 0)
            note_chosen(got);
        char line[96];
        snprintf(line, sizeof line, "bind %u %u %llu\n", (unsigned)asked, (unsigned)got, inode);
        log_line(line);
        errno = saved;
    }
    return result;
}

int rename(const char *old_path, const char *new_path)
{
    static rename_fn real_rename;
    if (real_rename == NULL)
        real_rename = (rename_fn)dlsym(RTLD_NEXT, "rename");
    if (real_rename == NULL) {
        errno = ENOSYS;
        return -1;
    }

    int result = real_rename(old_path, new_path);
    const char *hold = getenv("NW16A_HOLD_AT");
    if (result == 0 && hold != NULL && *hold != '\0' && new_path != NULL
        && strcmp(new_path, hold) == 0) {
        int saved = errno;
        char held[4096];
        char go[4096];
        snprintf(held, sizeof held, "%s.held", hold);
        snprintf(go, sizeof go, "%s.go", hold);
        int fd = open(held, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
        if (fd >= 0)
            close(fd);
        struct timespec tick = { 0, 10 * 1000 * 1000 };
        for (int i = 0; i < 12000 && access(go, F_OK) != 0; i++)
            nanosleep(&tick, NULL);
        errno = saved;
    }
    return result;
}
