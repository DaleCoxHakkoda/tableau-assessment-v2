"""Normalize database host names so one server is counted once.

The same server shows up spelled many ways across workbooks: different case, with or
without the company domain, with a port, or as one of several spellings of "this
machine". normalize_host() reduces them to one form:

  1. trim and lower-case                         P196            -> p196
  2. drop a trailing port: ",1433", " , 1433", ":1433"
  3. drop company domains (HOST_DOMAINS in .env, comma-separated), as either
     ".<domain>" or "@<domain>"                  p196.regence.com -> p196
  4. collapse every spelling of the local machine to "localhost":
     local, (local), localhost, 127.0.0.1, ::1, ".", (localdb), with or without
     an instance (e.g. (local)\\sqlexpress)
A SQL Server named instance on a real server is kept (p196\\inst), since two
instances on one machine can hold different databases.

Example with HOST_DOMAINS=regence.com:  "P196.regence.com,1433" -> "p196"
"""

import os
import re

LOCAL_NAMES = {"local", "(local)", "localhost", "127.0.0.1", "::1", ".", "(localdb)"}
_PORT = re.compile(r"\s*[,:]\s*\d+\s*$")


def domains_from_env():
    raw = os.environ.get("HOST_DOMAINS", "")
    return [d.strip().lower().lstrip(".@") for d in raw.split(",") if d.strip()]


def normalize_host(raw, domains=None):
    if raw is None:
        return None
    s = str(raw).strip().lower()
    if not s:
        return None
    if s in LOCAL_NAMES:
        return "localhost"
    s = _PORT.sub("", s)
    host, sep, inst = s.partition("\\")
    host = host.strip()
    if host in LOCAL_NAMES:
        return "localhost"
    host = host.rstrip(".")
    for d in domains if domains is not None else domains_from_env():
        for suffix in ("." + d, "@" + d):
            if host.endswith(suffix):
                host = host[: -len(suffix)]
    host = host.strip()
    if host in LOCAL_NAMES:
        return "localhost"
    if not host:
        return None
    return host + (sep + inst.strip() if inst.strip() else "")
