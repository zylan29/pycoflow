#!/usr/bin/sh
pcap_dir=$1
for node in `cat /home/zyang/spark/conf/slaves`
do
    scp root@$node:/root/ts.pcap $pcap_dir/$node.pcap
done 
