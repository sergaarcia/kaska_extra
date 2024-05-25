#!/bin/sh

set -x

cd bin
java -cp .:../common.jar client.EchoClient $*
