#!/bin/sh
  
set -x

cd src
javac -Xlint -cp .:../common.jar -d ../bin kaska/*.java apps/*.java
