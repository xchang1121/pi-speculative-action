"""CPython source compilation for controlled processes and the isolated host worker."""
import os
import sys


def _limits():
    digits = getattr(sys, "get_int_max_str_digits", None)
    return (sys.getrecursionlimit(), digits() if digits else None)


def _engine():
    import hashlib
    import importlib.util
    import json

    def identity(name):
        st = os.stat(name)
        return (st.st_dev, st.st_ino, st.st_size, st.st_ctime_ns)

    images = {identity(os.path.realpath(sys.executable))}
    with open("/proc/self/maps", "rb") as maps:
        for line in maps:
            fields = line.rstrip(b"\n").split(None, 5)
            if len(fields) != 6 or b"x" not in fields[1]:
                continue
            path = fields[5]
            if not os.path.basename(path).startswith(b"libpython"):
                continue
            mapped = identity(path)
            major, minor = (int(part, 16) for part in fields[3].split(b":"))
            if mapped[:2] != (os.makedev(major, minor), int(fields[4])):
                raise ValueError("Python library changed since launch")
            images.add(mapped)
    build = [sys.version, importlib.util.MAGIC_NUMBER.hex(), sorted(images), _limits()]
    return hashlib.sha256(json.dumps(build).encode("ascii")).hexdigest()


def _worker():
    import base64
    import io
    import json
    import marshal
    import resource
    import time
    import tokenize
    import warnings

    limit = 384 * 1024 * 1024
    _, hard = resource.getrlimit(resource.RLIMIT_AS)
    resource.setrlimit(resource.RLIMIT_AS, (min(limit, hard) if hard >= 0 else limit, hard))
    warnings.simplefilter("error")
    print(json.dumps(_engine()), flush=True)
    for line in sys.stdin.buffer:
        try:
            request = json.loads(line)
            data = base64.b64decode(request["data"], validate=True)
            encoding, _ = tokenize.detect_encoding(io.BytesIO(data).readline)
            if encoding not in ("utf-8", "utf-8-sig"):
                raise ValueError("use the caller's source codec")
            start = time.monotonic_ns()
            code = compile(data, request["filename"], "exec", dont_inherit=True, optimize=request["optimize"])
            elapsed = (time.monotonic_ns() - start) / 1e6
            ended = time.time_ns() / 1e6
            result = marshal.dumps(code)
            reply = [base64.b64encode(result).decode("ascii"), elapsed, ended] if len(result) <= request["limit"] else None
        except Exception:
            # The caller's original compiler owns warnings, syntax errors and their tracebacks.
            reply = None
        print(json.dumps(reply), flush=True)


def _transport(address, token):
    import _thread
    import atexit
    import base64
    import hashlib
    import json
    import marshal
    import socket
    import types

    exe = os.path.realpath(sys.executable)
    engine = _engine()
    limits = _limits()
    client = None
    reader = None
    disabled = False
    lock = _thread.RLock()
    active = False

    def close():
        nonlocal client, reader
        stream, sock = reader, client
        reader = client = None
        if stream is not None:
            try:
                stream.close()
            except OSError:
                pass
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass

    atexit.register(close)
    if hasattr(os, "register_at_fork"):
        def reset_child():
            nonlocal active
            try:
                close()
                active = False
            finally:
                lock.release()

        os.register_at_fork(before=lock.acquire, after_in_parent=lock.release, after_in_child=reset_child)

    def connect():
        nonlocal client, reader
        if client is None:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                sock.settimeout(5)
                sock.connect(address)
                stream = sock.makefile("rb")
            except BaseException:
                sock.close()
                raise
            client = sock
            reader = stream
        return client, reader

    def begin(payload):
        sock, stream = connect()
        sock.sendall(payload)
        line = stream.readline(24 * 1024 * 1024)
        if not line:
            raise EOFError("compiler channel closed")
        return sock, stream, json.loads(line)

    def exchange(data, path, optimize):
        nonlocal disabled
        if disabled or not (0 <= optimize <= 2) or "\0" in path or _limits() != limits:
            return
        request = dict(op="python_compile", context=token, engine=engine, executable=exe, filename=path,
                       optimize=optimize, digest=hashlib.sha256(data).hexdigest())
        payload = json.dumps(request).encode("ascii") + b"\n"
        fresh = client is None
        try:
            sock, stream, reply = begin(payload)
        except (OSError, ValueError, EOFError):
            close()
            if fresh:
                raise
            sock, stream, reply = begin(payload)
        try:
            state = reply.get("state")
            if state == "local":
                return
            if state == "unavailable":
                disabled = True
                close()
                return
            if state == "source":
                frame = {"op": "source", "data": base64.b64encode(data).decode("ascii")}
                sock.sendall(json.dumps(frame).encode("ascii") + b"\n")
                line = stream.readline(24 * 1024 * 1024)
                if not line:
                    raise EOFError("compiler channel closed")
                reply = json.loads(line)
                state = reply.get("state")
                if state == "unavailable":
                    return
            if state != "compiled":
                close()
                return
            code = marshal.loads(base64.b64decode(reply["data"], validate=True))
            if type(code) is not types.CodeType or code.co_filename != path:
                close()
                return
            sock.sendall(b'{"op":"consumed"}\n')
            line = stream.readline(256)
            if not line or json.loads(line).get("state") != "ready":
                close()
                return
            return code
        except BaseException:
            close()
            raise

    def compile_source(data, path, optimize):
        nonlocal active
        if disabled:
            return
        with lock:
            if active:
                return
            active = True
            try:
                return exchange(data, path, optimize)
            finally:
                active = False

    return compile_source


def _install():
    import builtins
    from importlib.machinery import SourceFileLoader

    address = os.environ.get("PI_PYTHON_COMPILE_SOCKET")
    token = os.environ.get("PI_PYTHON_COMPILE_CONTEXT")
    if not address or not token or sys.implementation.name != "cpython":
        return
    if (sys._xoptions or sys.flags.debug or sys.flags.dev_mode or os.environ.get("PYTHONNODEBUGRANGES")
            or any(k.startswith("LD_") for k in os.environ)):
        return
    original = SourceFileLoader.source_to_code
    compiler = builtins.compile
    if (original.__module__ != "_frozen_importlib_external" or type(compiler) is not type(len)
            or compiler.__module__ != "builtins" or compiler.__name__ != "compile"):
        return
    transport = None

    def source_to_code(loader, data, path, *, _optimize=-1):
        nonlocal transport
        if (type(loader) is SourceFileLoader and type(data) is bytes and isinstance(path, str)
                and os.path.isabs(path) and len(data) <= 2 * 1024 * 1024 and builtins.compile is compiler):
            if transport is None:
                # Imports needed by the transport may themselves compile source.
                transport = False
                try:
                    transport = _transport(address, token)
                except (OSError, ImportError, ValueError):
                    pass
            if transport:
                # Honour a local audit hook before sending source or accepting an artifact.
                sys.audit("compile", data, path)
                try:
                    code = transport(data, path, sys.flags.optimize if _optimize == -1 else _optimize)
                except (OSError, ValueError, EOFError, TypeError, KeyError):
                    code = None
                if code is not None:
                    return code
        return original(loader, data, path, _optimize=_optimize)

    SourceFileLoader.source_to_code = source_to_code


if __name__ == "__main__":
    _worker()
else:
    # The hook is appended to PYTHONPATH. Keep the application's original customization and search order.
    from importlib.machinery import PathFinder
    _here = os.path.dirname(__file__)
    sys.path[:] = [p for p in sys.path if p != _here]
    _site = PathFinder.find_spec("sitecustomize", sys.path)
    if _site is not None and _site.loader is not None:
        import importlib.util
        _original_site = importlib.util.module_from_spec(_site)
        sys.modules[__name__] = _original_site
        _site.loader.exec_module(_original_site)
    _install()
