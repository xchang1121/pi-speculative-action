#define _GNU_SOURCE
#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>

static sqlite3_vfs readonly_view, sealed_view;
static pthread_mutex_t registration = PTHREAD_MUTEX_INITIALIZER;

/* Keep the borrowed directory in the pathname. The original unix VFS owns all
 * live database I/O and locks; its SHM state must live in a separate process. */
static int view_path(sqlite3_vfs *vfs, const char *name, int capacity, char *out) {
	(void)vfs;
	int fd = -1, offset = 0;
	struct statvfs info;
	if (!name || sscanf(name, "/proc/self/fd/%d%n", &fd, &offset) != 1 || fd < 0 || !offset ||
		strcmp(name + offset, "/metadata.sqlite") || fstatvfs(fd, &info) || !(info.f_flag & ST_RDONLY) ||
		strlen(name) >= (size_t)capacity) return SQLITE_CANTOPEN;
	memcpy(out, name, strlen(name) + 1);
	return SQLITE_OK;
}

typedef struct SealedFile { sqlite3_file base; int fd; } SealedFile;
static int sealed_descriptor(const char *name) {
	if (!name || strncmp(name, "/proc/self/fd/", 14) || name[14] < '0' || name[14] > '9') return -1;
	char *end;
	errno = 0;
	long value = strtol(name + 14, &end, 10);
	return errno || *end || value < 0 || value > INT_MAX ? -1 : (int)value;
}
static int sealed_object(int fd) {
	struct stat info;
	int seals = fcntl(fd, F_GET_SEALS);
	int required = F_SEAL_WRITE | F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_SEAL;
	return seals >= 0 && (seals & required) == required && !fstat(fd, &info) && S_ISREG(info.st_mode) && !info.st_nlink;
}
static int sealed_path(sqlite3_vfs *vfs, const char *name, int capacity, char *out) {
	(void)vfs;
	int fd = sealed_descriptor(name);
	if (fd < 0 || !sealed_object(fd) || strlen(name) >= (size_t)capacity) return SQLITE_CANTOPEN;
	memcpy(out, name, strlen(name) + 1);
	return SQLITE_OK;
}
static int sealed_close(sqlite3_file *file) {
	SealedFile *owned = (SealedFile *)file;
	int result = close(owned->fd);
	owned->fd = -1; file->pMethods = NULL;
	return result ? SQLITE_IOERR_CLOSE : SQLITE_OK;
}
static int sealed_read(sqlite3_file *file, void *buffer, int amount, sqlite3_int64 offset) {
	int done = 0;
	if (amount < 0 || offset < 0 || offset > INT64_MAX - amount) return SQLITE_IOERR_READ;
	while (done < amount) {
		ssize_t count = pread(((SealedFile *)file)->fd, (char *)buffer + done, (size_t)(amount - done), offset + done);
		if (count < 0) { if (errno == EINTR) continue; return SQLITE_IOERR_READ; }
		if (!count) { memset((char *)buffer + done, 0, (size_t)(amount - done)); return SQLITE_IOERR_SHORT_READ; }
		done += (int)count;
	}
	return SQLITE_OK;
}
static int sealed_write(sqlite3_file *file, const void *buffer, int amount, sqlite3_int64 offset) {
	(void)file; (void)buffer; (void)amount; (void)offset;
	return SQLITE_READONLY;
}
static int sealed_truncate(sqlite3_file *file, sqlite3_int64 size) {
	(void)file; (void)size;
	return SQLITE_READONLY;
}
static int sealed_noop(sqlite3_file *file, int value) {
	(void)file; (void)value;
	return SQLITE_OK;
}
static int sealed_size(sqlite3_file *file, sqlite3_int64 *size) {
	struct stat info;
	if (fstat(((SealedFile *)file)->fd, &info)) return SQLITE_IOERR_FSTAT;
	*size = info.st_size;
	return SQLITE_OK;
}
static int sealed_reserved(sqlite3_file *file, int *reserved) {
	(void)file; *reserved = 0;
	return SQLITE_OK;
}
static int sealed_control(sqlite3_file *file, int operation, void *argument) {
	(void)file; (void)operation; (void)argument;
	return SQLITE_NOTFOUND;
}
static int sealed_sector(sqlite3_file *file) { (void)file; return 4096; }
static int sealed_characteristics(sqlite3_file *file) { (void)file; return SQLITE_IOCAP_IMMUTABLE; }
static const sqlite3_io_methods sealed_methods = {
	.iVersion = 1, .xClose = sealed_close, .xRead = sealed_read, .xWrite = sealed_write,
	.xTruncate = sealed_truncate, .xSync = sealed_noop, .xFileSize = sealed_size,
	.xLock = sealed_noop, .xUnlock = sealed_noop, .xCheckReservedLock = sealed_reserved,
	.xFileControl = sealed_control, .xSectorSize = sealed_sector, .xDeviceCharacteristics = sealed_characteristics
};
static int sealed_open(sqlite3_vfs *vfs, sqlite3_filename name, sqlite3_file *file, int flags, int *out_flags) {
	(void)vfs;
	file->pMethods = NULL;
	int borrowed = sealed_descriptor(name);
	if (borrowed < 0 || !(flags & SQLITE_OPEN_MAIN_DB) || !(flags & SQLITE_OPEN_READONLY) ||
		(flags & (SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_DELETEONCLOSE)) ||
		!sqlite3_uri_boolean(name, "immutable", 0)) return SQLITE_CANTOPEN;
	int fd = fcntl(borrowed, F_DUPFD_CLOEXEC, 3);
	if (fd < 0) return SQLITE_CANTOPEN;
	if (!sealed_object(fd)) { close(fd); return SQLITE_CANTOPEN; }
	((SealedFile *)file)->fd = fd;
	file->pMethods = &sealed_methods;
	if (out_flags) *out_flags = flags;
	return SQLITE_OK;
}
static int sealed_access(sqlite3_vfs *vfs, const char *name, int flags, int *result) {
	(void)vfs;
	int fd = sealed_descriptor(name);
	*result = fd >= 0 && flags != SQLITE_ACCESS_READWRITE && sealed_object(fd);
	return SQLITE_OK;
}
static int sealed_delete(sqlite3_vfs *vfs, const char *name, int directory_sync) {
	(void)vfs; (void)name; (void)directory_sync;
	return SQLITE_READONLY;
}

/* The standard entry point also works on Node 22.19, whose API takes only a path. */
int sqlite3_extension_init(sqlite3 *database, char **error, const sqlite3_api_routines *api) {
	(void)database; (void)error;
	pthread_mutex_lock(&registration);
	int result = SQLITE_OK;
	if (sqlite3_api && sqlite3_api != api) { result = SQLITE_MISMATCH; goto done; }
	SQLITE_EXTENSION_INIT2(api);
	if (sqlite3_libversion_number() < 3022000 || !api->vfs_find || !api->vfs_register || !api->uri_boolean) {
		result = SQLITE_MISMATCH; goto done;
	}
	sqlite3_vfs *original = sqlite3_vfs_find("unix");
	if (!original) { result = SQLITE_NOTFOUND; goto done; }
	if (!sqlite3_vfs_find("pi-read-view")) {
		readonly_view = *original;
		readonly_view.zName = "pi-read-view"; readonly_view.pNext = NULL;
		readonly_view.xFullPathname = view_path;
		result = sqlite3_vfs_register(&readonly_view, 0);
		if (result != SQLITE_OK) goto done;
	}
	if (!sqlite3_vfs_find("pi-sealed-view")) {
		sealed_view = *original;
		sealed_view.zName = "pi-sealed-view"; sealed_view.pNext = NULL;
		sealed_view.szOsFile = sizeof(SealedFile);
		sealed_view.xFullPathname = sealed_path; sealed_view.xOpen = sealed_open;
		sealed_view.xAccess = sealed_access; sealed_view.xDelete = sealed_delete;
		result = sqlite3_vfs_register(&sealed_view, 0);
	}
done:
	pthread_mutex_unlock(&registration);
	return result == SQLITE_OK ? SQLITE_OK_LOAD_PERMANENTLY : result;
}
