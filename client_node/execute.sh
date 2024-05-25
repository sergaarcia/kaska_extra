#!/bin/sh


cd bin
PROG=apps/$1
test $# -eq 0 || test ! -f $PROG.class && { echo "Debe especificar el nombre del programa ejecutar y sus argumentos" >&2; exit 1; }
shift

set -x
java -cp .:../common.jar $PROG $*
