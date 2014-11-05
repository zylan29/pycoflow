#!/usr/bin/python

import subprocess


def run_tshark(slaves_file):
    proc = []
    with open(slaves_file) as f:
        for h in f.readlines():
            if h.startswith("#"):
                continue
            host = h.strip()
            cmd = 'ssh root@%s nohup tshark -i eth0 -s 100 -w ts.pcap "tcp" &' % host
            p = subprocess.Popen(cmd, shell=True, stderr=None)
            proc.append(p)


def stop_tshark(slaves_file):
    cmd = "pssh -h %s -l root killall tshark" % slaves_file
    subprocess.Popen(cmd, shell=True, stderr=None)