/*
 * EC.03c2 (vault spec/items/EC.03c2.md): preloaded (LD_PRELOAD) into every
 * process the oracle starts, so that nothing binds beyond loopback. A bind
 * to the wildcard address (0.0.0.0 or ::) is made on loopback instead
 * (127.0.0.1 or ::1), the same port; any other address is left as asked.
 * Every bind of an inet socket appends one line to $EC03C2_BIND_LOG:
 *
 *     <pid> <address bound> <port asked> <rewritten|kept> <result>
 *
 * and the oracle fails a run whose log is empty or names an address that is
 * not loopback. Both builds of model_node, 748a185's and the candidate's,
 * run under the same shim, which changes nothing else.
 *
 * Built by run.sh: cc -shared -fPIC -O2 -o <scratch>/loopback_bind.so loopback_bind.c -ldl
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <dlfcn.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

typedef int (*bind_fn)(int, const struct sockaddr *, socklen_t);

static void log_bind(const char *addr, unsigned port, const char *what, int result)
{
    const char *path = getenv("EC03C2_BIND_LOG");
    if (path == NULL || *path == '\0')
        return;
    char line[160];
    int n = snprintf(line, sizeof line, "%d %s %u %s %d\n", (int)getpid(), addr, port, what, result);
    int fd = open(path, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
    if (fd < 0)
        return;
    if (n > 0)
        (void)!write(fd, line, (size_t)n);
    close(fd);
}

int bind(int fd, const struct sockaddr *addr, socklen_t len)
{
    static bind_fn real_bind;
    if (real_bind == NULL)
        real_bind = (bind_fn)dlsym(RTLD_NEXT, "bind");
    char text[INET6_ADDRSTRLEN] = "?";
    if (addr != NULL && addr->sa_family == AF_INET && len >= (socklen_t)sizeof(struct sockaddr_in)) {
        struct sockaddr_in in;
        memcpy(&in, addr, sizeof in);
        const char *what = "kept";
        if (in.sin_addr.s_addr == htonl(INADDR_ANY)) {
            in.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
            what = "rewritten";
        }
        int result = real_bind(fd, (const struct sockaddr *)&in, sizeof in);
        inet_ntop(AF_INET, &in.sin_addr, text, sizeof text);
        log_bind(text, ntohs(in.sin_port), what, result);
        return result;
    }
    if (addr != NULL && addr->sa_family == AF_INET6 && len >= (socklen_t)sizeof(struct sockaddr_in6)) {
        struct sockaddr_in6 in6;
        memcpy(&in6, addr, sizeof in6);
        const char *what = "kept";
        if (IN6_IS_ADDR_UNSPECIFIED(&in6.sin6_addr)) {
            in6.sin6_addr = in6addr_loopback;
            what = "rewritten";
        }
        int result = real_bind(fd, (const struct sockaddr *)&in6, sizeof in6);
        inet_ntop(AF_INET6, &in6.sin6_addr, text, sizeof text);
        log_bind(text, ntohs(in6.sin6_port), what, result);
        return result;
    }
    return real_bind(fd, addr, len);
}
