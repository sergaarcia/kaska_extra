#!/bin/sh
  
set -x

cd src
javac -Xlint -d ../bin kaskaprot/*.java

test $? -eq 0 || exit 1

cd ../bin

jar cf ../common.jar kaskaprot/*.class

