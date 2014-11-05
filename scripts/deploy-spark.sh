#!/usr/bin/bash


cd /home/zyang/spark/
sh sbin/stop-all.sh

rm -rf /home/zyang/spark/logs/*
rm -rf /home/zyang/spark/work/*

cd /home/zyang/
for i in `cat spark/conf/slaves`
do
    echo "=====$i===="
    rsync -r spark/ $i:/home/zyang/spark/
    ssh $i rm -rf /home/zyang/spark/logs/*
    ssh $i rm -rf /home/zyang/spark/work/*
done

wait
cd /home/zyang/spark/
sh sbin/start-all.sh
