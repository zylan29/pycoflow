#!/usr/bin/python

import os, sys
import subprocess
import time

import tshark

slaves_file = './conf/slaves'

tshark.run_tshark(slaves_file)

st = time.time()
logfile = open('./tasklogs/spark-submit-%s.log'%st, 'w')
cmd = "./bin/spark-submit --master spark://master:7077  --total-executor-cores 24 --executor-memory 7g --class org.apache.spark.examples.bagel.WikipediaPageRank /home/zyang/WikiPageRank/target/scala-2.10/wikipagerank_2.10-1.0.jar hdfs://master:9100/wikipage-26g 1 3 True"
p = subprocess.Popen(cmd, shell=True, universal_newlines=True, stdout=logfile, stderr=logfile)
ret_code = p.wait()
logfile.flush()
et = time.time()
print "task completed"
print "ret code = ", ret_code
print "time = ", et - st

tshark.stop_tshark(slaves_file)
