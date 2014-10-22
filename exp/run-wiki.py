#!/usr/bin/python

import subprocess
import time
import os

import tshark


spark_dir = '/home/zyang/spark/'
slaves_file = '%s/conf/slaves' % spark_dir

tasklogs_dir = '%s/tasklogs/' % spark_dir

tshark.run_tshark(slaves_file)

st = time.time()

if not os.path.exists(tasklogs_dir):
    os.mkdir(tasklogs_dir)

logfile = open('%s/spark-submit-%s.log' % (tasklogs_dir, st), 'w')
cmd = "%s/bin/spark-submit --master spark://master:7077  --total-executor-cores 24 --executor-memory 7g " \
      "--class org.apache.spark.examples.bagel.WikipediaPageRank " \
      "/home/zyang/WikiPageRank/target/scala-2.10/wikipagerank_2.10-1.0.jar " \
      "hdfs://192.168.1.12:9000/freebase-26G 1 100 True" % spark_dir
p = subprocess.Popen(cmd, shell=True, universal_newlines=True, stdout=logfile, stderr=logfile)
ret_code = p.wait()
logfile.flush()
et = time.time()
print "task completed"
print "ret code = ", ret_code
print "time elapsed = ", et - st

tshark.stop_tshark(slaves_file)
