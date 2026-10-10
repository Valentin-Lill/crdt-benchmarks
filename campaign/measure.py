#!/usr/bin/env python3
"""Capture kernel peak RSS and CPU accounting for the benchmark child only."""
import json, os, pathlib, subprocess, sys, time
dest=sys.argv[1];start=time.monotonic()
p=subprocess.Popen(sys.argv[2:])
_,status,usage=os.wait4(p.pid,0)
p.returncode=os.waitstatus_to_exitcode(status)
pathlib.Path(dest).write_text(json.dumps({'max_rss_kib':usage.ru_maxrss,'elapsed_s':time.monotonic()-start,'user_s':usage.ru_utime,'system_s':usage.ru_stime}))
sys.exit(p.returncode)
