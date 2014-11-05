#!/usr/bin/sh
pssh -h spark/conf/slaves -l root killall tshark
