#!/bin/sh

PUERTO=1099
test $1 && PUERTO=$1
ss -tnl | grep -q $PUERTO > /dev/null 2>&1 && { echo "error: puerto ocupado" >&2; exit 1; }
CLASSPATH=.:common.jar exec rmiregistry $* > /dev/null 2>&1
