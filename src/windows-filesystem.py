"""Own Windows file-ID handles for one parent pipe connection."""

import ctypes
from ctypes import wintypes
import json
import struct
import sys


class FileInformation(ctypes.Structure):
    _fields_ = [
        ("attributes", wintypes.DWORD),
        ("creation", wintypes.FILETIME),
        ("access", wintypes.FILETIME),
        ("modified", wintypes.FILETIME),
        ("volume", wintypes.DWORD),
        ("size_high", wintypes.DWORD),
        ("size_low", wintypes.DWORD),
        ("links", wintypes.DWORD),
        ("index_high", wintypes.DWORD),
        ("index_low", wintypes.DWORD),
    ]


class FileID(ctypes.Union):
    _fields_ = [("index", ctypes.c_longlong), ("object_id", ctypes.c_byte * 16)]


class FileIDDescriptor(ctypes.Structure):
    _anonymous_ = ("identifier",)
    _fields_ = [("size", wintypes.DWORD), ("kind", ctypes.c_int), ("identifier", FileID)]


kernel = ctypes.WinDLL("kernel32", use_last_error=True)


def api(name, arguments, result):
    function = getattr(kernel, name)
    function.argtypes = arguments
    function.restype = result
    return function


create_file = api(
    "CreateFileW",
    [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, ctypes.c_void_p,
     wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE],
    wintypes.HANDLE,
)
open_by_id = api(
    "OpenFileById",
    [wintypes.HANDLE, ctypes.POINTER(FileIDDescriptor), wintypes.DWORD,
     wintypes.DWORD, ctypes.c_void_p, wintypes.DWORD],
    wintypes.HANDLE,
)
information = api(
    "GetFileInformationByHandle",
    [wintypes.HANDLE, ctypes.POINTER(FileInformation)],
    wintypes.BOOL,
)
close_handle = api("CloseHandle", [wintypes.HANDLE], wintypes.BOOL)
invalid_handle = ctypes.c_void_p(-1).value


def checked(value):
    if not value:
        raise ctypes.WinError(ctypes.get_last_error())
    return value


def inspect(handle):
    state = FileInformation()
    checked(information(handle, ctypes.byref(state)))
    return state


def identity(state):
    return state.volume, state.index_high, state.index_low


def pin(name):
    if not isinstance(name, str) or not name or "\0" in name or len(name) >= 32767:
        raise ValueError("invalid Windows object path")
    named = create_file(name, 0, 7, None, 3, 0x02200000, None)
    if named == invalid_handle:
        raise ctypes.WinError(ctypes.get_last_error())
    identified = invalid_handle
    try:
        before = inspect(named)
        if before.attributes & 0x400:
            raise ValueError("reparse points cannot own a file-ID lease")
        descriptor = FileIDDescriptor()
        descriptor.size = ctypes.sizeof(descriptor)
        descriptor.index = (before.index_high << 32) | before.index_low
        identified = open_by_id(named, ctypes.byref(descriptor), 0, 7, None, 0x02000000)
        if identified == invalid_handle:
            raise ctypes.WinError(ctypes.get_last_error())
        if identity(before) != identity(inspect(identified)):
            raise ValueError("filesystem object binding changed")
        result = identified
        identified = invalid_handle
        return result
    finally:
        if identified != invalid_handle:
            close_handle(identified)
        close_handle(named)


# One escaped Windows path (six bytes per UTF-16 unit) plus the JSON envelope.
maximum_request = 6 * 32767 + 128
handles = {}
try:
    while True:
        line = sys.stdin.buffer.readline(maximum_request + 1)
        if not line:
            break
        if len(line) > maximum_request or not line.endswith(b"\n"):
            break
        request_id = None
        try:
            request_id, operation, argument = json.loads(line)
            if type(request_id) is not int or request_id <= 0:
                raise ValueError("invalid request identity")
            if operation == "pin":
                name, maximum_leases = argument
                if len(handles) >= maximum_leases:
                    raise ValueError("Windows object lease capacity exceeded")
                if request_id in handles:
                    raise ValueError("duplicate lease identity")
                handles[request_id] = pin(name)
                value = request_id
            elif operation == "identity":
                state = inspect(handles[argument])
                value = struct.pack(
                    "<7I", *identity(state), state.links,
                    state.size_high, state.size_low, state.attributes,
                ).hex()
            elif operation == "close":
                handle = handles.get(argument)
                if handle is not None:
                    checked(close_handle(handle))
                    del handles[argument]
                value = None
            elif operation == "count":
                value = len(handles)
            else:
                raise ValueError("unknown lease operation")
            response = {"value": value}
        except Exception as error:
            response = {"error": {
                "code": "WIN32_" + str(getattr(error, "winerror", 0)),
                "message": str(error),
            }}
        print(json.dumps([request_id, response]), flush=True)
finally:
    for handle in handles.values():
        close_handle(handle)
