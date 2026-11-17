import base64
import json
import os
import socket
import sys
import time

address = os.environ["PI_SUBCOMPUTE_SOCKET"]
if os.name == "nt":
    channel = open(address, "r+b", buffering=0)
else:
    connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    connection.connect(address)
    channel = connection.makefile("rwb", buffering=0)

def send(value):
    channel.write(json.dumps(value).encode() + b"\n")

send({"protocol": 1, "op": "compute", "context": os.environ["PI_SUBCOMPUTE_CONTEXT"],
      "namespace": "cross-language.reverse", "version": "1", "key": sys.argv[1] if len(sys.argv) > 1 else "shared",
      "inputResources": [{"path": "input.bin", "scope": "content"}], "includeInputContent": True,
      "envKeys": ["TRANSFORM_MODE"],
      "environment": {"TRANSFORM_MODE": os.environ.get("TRANSFORM_MODE")}})
reply = json.loads(channel.readline())
hit = reply["state"] == "hit"
if reply["state"] == "miss":
    started = time.perf_counter()
    output = base64.b64decode(reply["inputResources"][0]["content"]["value"])[::-1]
    send({"op": "publish", "value": {"encoding": "base64", "value": base64.b64encode(output).decode()},
          "durationMs": (time.perf_counter() - started) * 1000})
    reply = json.loads(channel.readline())
channel.close()
if reply["state"] == "error":
    raise RuntimeError(reply["error"])
print(json.dumps({"hit": hit, "value": reply["value"]}))
