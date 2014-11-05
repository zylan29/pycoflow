#!/usr/bin/python

import time

import runspark
import tshark


spark_dir = '/home/zyang/spark/'
tasklogs_dir = '%s/tasklogs/' % spark_dir
slaves_file = '%s/conf/slaves' % spark_dir


tshark.run_tshark(slaves_file)

st = time.time()
print "Run Exp @%s" % st

process_list = []

process_list.append(runspark.run_wc_21(spark_dir))
process_list.append(runspark.run_wc_40(spark_dir))
process_list.append(runspark.run_wiki_13(spark_dir))
process_list.append(runspark.run_wiki_26(spark_dir))

for i in xrange(len(process_list)):
    ret_code = process_list[i].wait()
    print "ret_code %d = " % i, ret_code

et = time.time()
print "task completed"
print "time elapsed = ", et - st

tshark.stop_tshark(slaves_file)
