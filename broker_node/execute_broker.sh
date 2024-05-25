#!/bin/sh

set -x

cd bin
java  -cp .:../common.jar broker.BrokerSrv $*
