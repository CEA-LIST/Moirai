/*
 * NW.16b, O1 (vault spec/items/NW.16b.md): a library the O1 test builds and
 * preloads into one model_node process (LD_PRELOAD), so that one direct link
 * between two peers can be blocked on the host without root. It wraps one
 * C library call, connect, and changes nothing else. It prints nothing on
 * the node's output.
 *
 * NW16B_REFUSE_CONNECT=<port>[,<port>...]
 *     A connect of a TCP socket (AF_INET or AF_INET6) to one of these ports,
 *     at any address, fails at once with ECONNREFUSED, as a connect to a
 *     closed port does. Two peers each refused the other's listen port can
 *     never link to each other, whichever dials, and stay free to link to
 *     their router.
 *
 * NW16B_CONNECT_LOG=<file>
 *     Every refused connect appends "refused <port>\n"; every other TCP
 *     connect appends "connect <port>\n".
 *
 * Built by the tests with: cc -shared -fPIC -O2 -Wall -o <scratch>/x.so connect_shim.c -ldl
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

typedef int (*connect_fn)(int, const struct sockaddr *, socklen_t);

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

/* Whether `port` is in the comma-separated list NW16B_REFUSE_CONNECT. */
static int refused(unsigned short port)
{
    const char *list = getenv("NW16B_REFUSE_CONNECT");
    if (list == NULL)
        return 0;
    const char *at = list;
    while (*at != '\0') {
        char *end = NULL;
        unsigned long value = strtoul(at, &end, 10);
        if (end == at)
            return 0;
        if (value == port)
            return 1;
        at = (*end == ',') ? end + 1 : end;
        if (*end != ',' && *end != '\0')
            return 0;
    }
    return 0;
}

static void log_line(const char *line)
{
    const char *path = getenv("NW16B_CONNECT_LOG");
    if (path == NULL || *path == '\0')
        return;
    int fd = open(path, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
    if (fd < 0)
        return;
    ssize_t written = write(fd, line, strlen(line));
    (void)written;
    close(fd);
}

int connect(int fd, const struct sockaddr *addr, socklen_t len)
{
    static connect_fn real_connect;
    if (real_connect == NULL)
        real_connect = (connect_fn)dlsym(RTLD_NEXT, "connect");
    if (real_connect == NULL) {
        errno = ENOSYS;
        return -1;
    }

    unsigned short port = 0;
    if (inet_port(addr, len, &port) && is_stream(fd)) {
        char line[64];
        if (refused(port)) {
            snprintf(line, sizeof line, "refused %u\n", (unsigned)port);
            log_line(line);
            errno = ECONNREFUSED;
            return -1;
        }
        int saved = errno;
        snprintf(line, sizeof line, "connect %u\n", (unsigned)port);
        log_line(line);
        errno = saved;
    }
    return real_connect(fd, addr, len);
}
