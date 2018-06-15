#!/usr/bin/env python

import msgpack
import sys
import pprint

try:
    pp = pprint.PrettyPrinter(indent=2)
    line = sys.stdin.read()
    try:
        pp.pprint(msgpack.unpackb(line))
    except (
        TypeError, ValueError, msgpack.exceptions.UnpackValueError,
        msgpack.exceptions.ExtraData):
        print line
except KeyboardInterrupt:
    pass
