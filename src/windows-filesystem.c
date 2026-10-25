#define _WIN32_WINNT 0x0602
#define NAPI_VERSION 8
#include <windows.h>
#include <node_api.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <wchar.h>

/* NTFS object leases: a kernel identity lifetime, independent of names or content versions. */
struct object_lease { HANDLE handle; };
static LONG live_leases;
static const napi_type_tag lease_tag = {UINT64_C(0x2ecb8147d98f430a), UINT64_C(0x9f31ae60c7b254d8)};

static napi_value native_error(napi_env env, const char *operation, DWORD error) {
    char code[32], message[160];
    snprintf(code, sizeof(code), "WIN32_%lu", (unsigned long)error);
    snprintf(message, sizeof(message), "%s failed with Windows error %lu", operation, (unsigned long)error);
    napi_throw_error(env, code, message);
    return NULL;
}

static int argument(napi_env env, napi_callback_info info, napi_value *value) {
    size_t count = 1;
    if (napi_get_cb_info(env, info, &count, value, NULL, NULL) == napi_ok && count == 1) return 1;
    napi_throw_type_error(env, "ERR_LEASE_ARGUMENT", "one argument is required");
    return 0;
}

static struct object_lease *owned(napi_env env, napi_value value) {
    bool matches = false; struct object_lease *lease = NULL;
    if (napi_check_object_type_tag(env, value, &lease_tag, &matches) != napi_ok || !matches ||
        napi_unwrap(env, value, (void **)&lease) != napi_ok || !lease) {
        napi_throw_type_error(env, "ERR_LEASE_OWNER", "an issued object lease is required");
        return NULL;
    }
    return lease;
}

static void finalize(napi_env env, void *data, void *hint) {
    (void)env; (void)hint;
    struct object_lease *lease = data;
    if (lease->handle != INVALID_HANDLE_VALUE) {
        CloseHandle(lease->handle);
        InterlockedDecrement(&live_leases);
    }
    free(lease);
}

static int same_object(BY_HANDLE_FILE_INFORMATION a, BY_HANDLE_FILE_INFORMATION b) {
    return a.dwVolumeSerialNumber == b.dwVolumeSerialNumber && a.nFileIndexHigh == b.nFileIndexHigh && a.nFileIndexLow == b.nFileIndexLow;
}

static napi_value pin(napi_env env, napi_callback_info info) {
    napi_value value, result; size_t length = 0;
    if (!argument(env, info, &value)) return NULL;
    if (napi_get_value_string_utf16(env, value, NULL, 0, &length) != napi_ok || !length || length >= 32767) {
        napi_throw_type_error(env, "ERR_LEASE_PATH", "a bounded nonempty path is required");
        return NULL;
    }
    WCHAR *name = calloc(length + 1, sizeof(*name));
    if (!name) return native_error(env, "allocate path", ERROR_NOT_ENOUGH_MEMORY);
    if (napi_get_value_string_utf16(env, value, (char16_t *)name, length + 1, &length) != napi_ok || wcslen(name) != length) {
        free(name); napi_throw_type_error(env, "ERR_LEASE_PATH", "path contains an embedded NUL"); return NULL;
    }
    HANDLE named = CreateFileW(name, 0, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL,
        OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT, NULL);
    DWORD error = GetLastError(); free(name);
    if (named == INVALID_HANDLE_VALUE) return native_error(env, "CreateFileW", error);
    BY_HANDLE_FILE_INFORMATION before, after;
    if (!GetFileInformationByHandle(named, &before)) {
        error = GetLastError(); CloseHandle(named); return native_error(env, "source identity", error);
    }
    if (before.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) {
        CloseHandle(named); return native_error(env, "reparse-point lease is not qualified", ERROR_NOT_SUPPORTED);
    }
    WCHAR filesystem[32]; DWORD capabilities;
    if (!GetVolumeInformationByHandleW(named, NULL, 0, NULL, NULL, &capabilities, filesystem, 32)) {
        error = GetLastError(); CloseHandle(named); return native_error(env, "volume capabilities", error);
    }
    if (wcscmp(filesystem, L"NTFS") || !(capabilities & FILE_SUPPORTS_OPEN_BY_FILE_ID)) {
        CloseHandle(named); return native_error(env, "object leases require NTFS file IDs", ERROR_NOT_SUPPORTED);
    }
    FILE_ID_DESCRIPTOR descriptor = {.dwSize = sizeof(descriptor), .Type = FileIdType};
    descriptor.FileId.QuadPart = ((uint64_t)before.nFileIndexHigh << 32) | before.nFileIndexLow;
    HANDLE identified = OpenFileById(named, &descriptor, 0, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        NULL, FILE_FLAG_BACKUP_SEMANTICS);
    error = GetLastError();
    if (identified == INVALID_HANDLE_VALUE) {
        CloseHandle(named); return native_error(env, "OpenFileById", error);
    }
    if (!GetFileInformationByHandle(identified, &after)) {
        error = GetLastError(); CloseHandle(identified); CloseHandle(named); return native_error(env, "ID identity", error);
    }
    if (!same_object(before, after)) {
        CloseHandle(identified); CloseHandle(named); return native_error(env, "reopened identity", ERROR_FILE_INVALID);
    }
    if (!CloseHandle(named)) {
        error = GetLastError(); CloseHandle(identified); return native_error(env, "release path handle", error);
    }
    struct object_lease *lease = malloc(sizeof(*lease));
    if (!lease) { CloseHandle(identified); return native_error(env, "allocate lease", ERROR_NOT_ENOUGH_MEMORY); }
    lease->handle = identified;
    InterlockedIncrement(&live_leases);
    if (napi_create_object(env, &result) != napi_ok || napi_type_tag_object(env, result, &lease_tag) != napi_ok ||
        napi_wrap(env, result, lease, finalize, NULL, NULL) != napi_ok) {
        finalize(env, lease, NULL); napi_throw_error(env, "ERR_LEASE_WRAP", "could not wrap native lease"); return NULL;
    }
    return result;
}

static napi_value identity(napi_env env, napi_callback_info info) {
    napi_value value, result;
    if (!argument(env, info, &value)) return NULL;
    struct object_lease *lease = owned(env, value);
    if (!lease) return NULL;
    BY_HANDLE_FILE_INFORMATION state;
    if (!GetFileInformationByHandle(lease->handle, &state)) return native_error(env, "lease identity", GetLastError());
    uint32_t fields[] = {state.dwVolumeSerialNumber, state.nFileIndexHigh, state.nFileIndexLow, state.nNumberOfLinks,
        state.nFileSizeHigh, state.nFileSizeLow, state.dwFileAttributes};
    if (napi_create_buffer_copy(env, sizeof(fields), fields, NULL, &result) != napi_ok) return NULL;
    return result;
}

static napi_value close_lease(napi_env env, napi_callback_info info) {
    napi_value value, result;
    if (!argument(env, info, &value)) return NULL;
    struct object_lease *lease = owned(env, value);
    if (!lease) return NULL;
    if (lease->handle != INVALID_HANDLE_VALUE) {
        if (!CloseHandle(lease->handle)) return native_error(env, "close lease", GetLastError());
        lease->handle = INVALID_HANDLE_VALUE;
        InterlockedDecrement(&live_leases);
    }
    return napi_get_undefined(env, &result) == napi_ok ? result : NULL;
}

static napi_value live_count(napi_env env, napi_callback_info info) {
    (void)info; napi_value result;
    return napi_create_int32(env, InterlockedCompareExchange(&live_leases, 0, 0), &result) == napi_ok ? result : NULL;
}

static napi_value initialize(napi_env env, napi_value exports) {
    napi_property_descriptor methods[] = {
        {.utf8name = "pin", .method = pin}, {.utf8name = "identity", .method = identity},
        {.utf8name = "close", .method = close_lease}, {.utf8name = "liveCount", .method = live_count},
    };
    return napi_define_properties(env, exports, sizeof(methods) / sizeof(methods[0]), methods) == napi_ok ? exports : NULL;
}
NAPI_MODULE(NODE_GYP_MODULE_NAME, initialize)
