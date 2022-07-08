#!/bin/bash

cd /root/chia-blockchain
. ./activate
chia stop -d all
if [ "$1" == "start" ]; then
	chia start farmer
fi
deactivate