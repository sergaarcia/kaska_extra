#!/bin/sh
  
set -x

cd src
javac -Xlint -cp .:../common.jar -d ../bin server/*.java
