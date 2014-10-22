#!/usr/bin/sh
log_dir=$1
for node in `cat /home/zyang/spark/conf/slaves`
do
    ssh $node grep TE -R /home/zyang/spark/work/ | grep BlockFetcherIterator > $log_dir/$node.txt
done
