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

send({"op": "compute", "context": os.environ["PI_SUBCOMPUTE_CONTEXT"],
      "namespace": "cross-language.reverse", "key": sys.argv[1] if len(sys.argv) > 1 else "shared",
      "inputResources": [{"path": "input.bin", "scope": "content"}], "includeInputContent": True,
      "envKeys": ["TRANSFORM_MODE"],
      "environment": {key: os.environ[key] for key in ["TRANSFORM_MODE"] if key in os.environ}})
reply = json.loads(channel.readline())
hit = reply["state"] == "hit"
if reply["state"] == "miss":
    started = time.perf_counter()
    output = base64.b64decode(reply["inputResources"][0]["content"]["value"])[::-1]
    token = reply["token"]
    if len(sys.argv) > 2 and sys.argv[2] == "--include":
        include = base64.b64decode(reply["inputResources"][0]["content"]["value"]).decode()
        send({"op": "observe", "token": token, "reads": [{"path": include, "scope": "content"}]})
        observed = json.loads(channel.readline())
        if observed != {"state": "observed", "token": token}:
            raise RuntimeError(observed)
        with open(include, "rb") as dependency:
            output = dependency.read()
    send({"op": "publish", "token": token, "value": {"encoding": "base64", "value": base64.b64encode(output).decode()},
          "durationMs": (time.perf_counter() - started) * 1000})
    reply = json.loads(channel.readline())
channel.close()
if reply["state"] == "error":
    raise RuntimeError(reply["error"])
print(json.dumps({"hit": hit, "value": reply["value"]}))
