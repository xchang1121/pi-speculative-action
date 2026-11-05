#ifndef PI_EXECUTION_DOMAIN_H
#define PI_EXECUTION_DOMAIN_H

/* The collector owns observations; the application receives only read views. */
#define PI_DOMAIN_ABI 16
#define PI_DOMAIN_VALUES 4096
#define PI_DOMAIN_LIMIT 256
#define PI_DOMAIN_RECORDS 65536
#define PI_DOMAIN_ROOTS 4
#define PI_DOMAIN_ANCESTORS 64
enum pi_domain_operation { PI_DOMAIN_HELLO, PI_DOMAIN_OPEN, PI_DOMAIN_JOIN, PI_DOMAIN_CLOSE = 4, PI_DOMAIN_WATCH, PI_DOMAIN_VERSION, PI_DOMAIN_CHANGES,
	PI_DOMAIN_TRACK, PI_DOMAIN_FILE, PI_DOMAIN_QUEUES, PI_DOMAIN_TRACE, PI_DOMAIN_EVENTS, PI_DOMAIN_ADMIT };
struct pi_domain_packet {
	unsigned int operation, count;
	unsigned long long domain;
	unsigned long long values[PI_DOMAIN_VALUES];
};
struct pi_domain_inode { unsigned long long device, inode; };
struct pi_domain_record { unsigned long long type, first, second; };
struct pi_object_version { unsigned long long version, active, uncertain, binding, born, filesystem; };
struct pi_version_clock { unsigned long long version, lost, uncertain, active; };
/* Description identity ends at security_file_free, including queued rights. No
 * observer pin or reconstructed FD table extends that lifetime. */
struct pi_domain_file { unsigned long long descriptor, id, owner, device, inode, attributes, channel, event, task, flags; };
struct pi_domain_admission {
	unsigned long long descriptor, device, inode, filesystem, mode;
	long long writers, mappings, direct_io, inode_lock;
};
struct pi_domain_state {
	unsigned long long namespace_device, namespace_inode, mount_namespace, uncertain;
	unsigned long long descriptor_lost, descriptor_active, descriptor_escaped;
	unsigned long long metadata_device, metadata_inode, metadata_lost, metadata_records;
	unsigned int root_count, ancestor_count;
	struct pi_domain_inode roots[PI_DOMAIN_ROOTS], ancestors[PI_DOMAIN_ANCESTORS];
};
struct pi_domain_event {
	unsigned long long owner, task, time, id, kind;
	unsigned int size; int error;
	unsigned char bytes[4096];
};
#define PI_DOMAIN_TRACE_BYTES (512UL * 1024 * 1024)

#ifndef PI_BPF_OBJECT
#include <errno.h>
#include <stddef.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/un.h>
#include <unistd.h>
#include <string.h>
#include <time.h>

static inline ssize_t pi_domain_message(int channel, struct msghdr *message, int receiving) {
	struct timespec start, now; if (clock_gettime(CLOCK_MONOTONIC, &start)) return -1;
	size_t control = message->msg_controllen;
	for (;;) {
		message->msg_controllen = control;
		ssize_t bytes = receiving ? recvmsg(channel, message, MSG_TRUNC | MSG_CMSG_CLOEXEC) : sendmsg(channel, message, MSG_NOSIGNAL);
		if (bytes >= 0 || errno != EINTR) return bytes;
		if (clock_gettime(CLOCK_MONOTONIC, &now)) return -1;
		if (now.tv_sec - start.tv_sec > 2 || (now.tv_sec - start.tv_sec == 2 && now.tv_nsec >= start.tv_nsec)) { errno = ETIMEDOUT; return -1; }
	}
}

/* The owner is a foreground root process; application code never receives its
 * BPF handles or cgroup descriptors. Every operation has a bounded round trip. */
static inline int pi_domain_exchange(const char *socket_path, struct pi_domain_packet *packet) {
	struct sockaddr_un address = {.sun_family = AF_UNIX};
	if (!socket_path || strlen(socket_path) >= sizeof(address.sun_path) || packet->count > PI_DOMAIN_VALUES) { errno = EINVAL; return -1; }
	strcpy(address.sun_path, socket_path);
	int channel = socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0), error = 0;
	if (channel < 0) return -1;
	struct timeval timeout = {.tv_sec = 2};
	struct ucred peer; socklen_t size = sizeof(peer);
	if (setsockopt(channel, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) ||
		setsockopt(channel, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) ||
		connect(channel, (struct sockaddr *)&address, sizeof(address)) ||
		getsockopt(channel, SOL_SOCKET, SO_PEERCRED, &peer, &size) || size != sizeof(peer) || peer.uid) { error = errno ? errno : EACCES; goto done; }
	unsigned operation = packet->operation;
	size_t bytes = offsetof(struct pi_domain_packet, values) + packet->count * sizeof(*packet->values);
	struct iovec vector = {packet, bytes};
	struct msghdr message = {.msg_iov = &vector, .msg_iovlen = 1};
	ssize_t sent = pi_domain_message(channel, &message, 0);
	if (sent != (ssize_t)bytes) { error = sent < 0 ? errno : EIO; goto done; }
	char control[CMSG_SPACE(sizeof(int))];
	vector.iov_len = sizeof(*packet); message.msg_control = control; message.msg_controllen = sizeof(control);
	ssize_t received = pi_domain_message(channel, &message, 1);
	if (received < 0) { error = errno; goto done; }
	int descriptor = -1; unsigned descriptors = 0;
	for (struct cmsghdr *item = CMSG_FIRSTHDR(&message); item; item = CMSG_NXTHDR(&message, item))
		if (item->cmsg_level == SOL_SOCKET && item->cmsg_type == SCM_RIGHTS && item->cmsg_len >= CMSG_LEN(0))
			for (size_t offset = 0; offset + sizeof(int) <= item->cmsg_len - CMSG_LEN(0); offset += sizeof(int)) {
				int current; memcpy(&current, CMSG_DATA(item) + offset, sizeof(current));
				if (descriptors++) close(current); else descriptor = current;
			}
	if (received < 16 || packet->count > PI_DOMAIN_VALUES || received != (ssize_t)(16 + packet->count * sizeof(*packet->values)) || message.msg_flags & MSG_CTRUNC || descriptors > 1) error = EPROTO;
	else if (packet->operation == (unsigned)-1) error = packet->domain && packet->domain < 4096 ? (int)packet->domain : EPROTO;
	else if (packet->operation != operation) error = EPROTO;
	else if (operation == PI_DOMAIN_TRACE) { if (descriptor < 0 || packet->count != 1) error = EPROTO; else packet->values[0] = descriptor; }
	else if (descriptor >= 0) error = EPROTO;
	if (error && descriptor >= 0) close(descriptor);
done:
	close(channel); if (error) { errno = error; return -1; } return 0;
}
#endif

#endif
