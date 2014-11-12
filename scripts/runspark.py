import os
import random
import subprocess
import time


def run_wc_21(spark_dir, cores=72):
    tasklogs_dir = check_tasklogs_dir(spark_dir)
    st = time.time()
    print "Run WordCount 21G @%s" % st
    rand_id = random.randint(1, 1000)
    logfile = open('%s/spark-submit-%s-%d.log' % (tasklogs_dir, st, rand_id), 'w')
    cmd = "%s/bin/spark-submit --master spark://master:7077  --total-executor-cores %d --executor-memory 7g " \
          "--class ca.haow.www.SimpleApp /home/zyang/SimpleApp/target/scala-2.10/simple-project_2.10-1.0.jar " \
          "hdfs://192.168.1.12:9000/alldata-21G False" % (spark_dir, cores)
    p = subprocess.Popen(cmd, shell=True, universal_newlines=True, stdout=logfile, stderr=logfile)
    return p


def run_wc_40(spark_dir, cores=72):
    tasklogs_dir = check_tasklogs_dir(spark_dir)
    st = time.time()
    print "Run WordCount 40G @%s" % st
    rand_id = random.randint(1, 1000)
    logfile = open('%s/spark-submit-%s-%d.log' % (tasklogs_dir, st, rand_id), 'w')
    cmd = "%s/bin/spark-submit --master spark://master:7077  --total-executor-cores %d --executor-memory 7g " \
          "--class ca.haow.www.SimpleApp /home/zyang/SimpleApp/target/scala-2.10/simple-project_2.10-1.0.jar " \
          "hdfs://192.168.1.12:9000/alldata-40G False" % (spark_dir, cores)
    p = subprocess.Popen(cmd, shell=True, universal_newlines=True, stdout=logfile, stderr=logfile)
    return p


def run_wiki_13(spark_dir, cores=72):
    tasklogs_dir = check_tasklogs_dir(spark_dir)
    st = time.time()
    print "Run WikiPageRank 13G @%s" % st
    rand_id = random.randint(1, 1000)
    logfile = open('%s/spark-submit-%s-%d.log' % (tasklogs_dir, st, rand_id), 'w')
    cmd = "%s/bin/spark-submit --master spark://master:7077  --total-executor-cores %d --executor-memory 7g " \
          "--class org.apache.spark.examples.bagel.WikipediaPageRank " \
          "/home/zyang/WikiPageRank/target/scala-2.10/wikipagerank_2.10-1.0.jar " \
          "hdfs://192.168.1.12:9000/freebase-13G 1 %d True" % (spark_dir, cores, cores)
    p = subprocess.Popen(cmd, shell=True, universal_newlines=True, stdout=logfile, stderr=logfile)
    return p


def run_wiki_26(spark_dir, cores=72):
    tasklogs_dir = check_tasklogs_dir(spark_dir)
    st = time.time()
    print "Run WikiPageRank 26G @%s" % st
    rand_id = random.randint(1, 1000)
    logfile = open('%s/spark-submit-%s-%d.log' % (tasklogs_dir, st, rand_id), 'w')
    cmd = "%s/bin/spark-submit --master spark://master:7077  --total-executor-cores %d --executor-memory 7g " \
          "--class org.apache.spark.examples.bagel.WikipediaPageRank " \
          "/home/zyang/WikiPageRank/target/scala-2.10/wikipagerank_2.10-1.0.jar " \
          "hdfs://192.168.1.12:9000/freebase-26G 1 %d True" % (spark_dir, cores, cores)
    p = subprocess.Popen(cmd, shell=True, universal_newlines=True, stdout=logfile, stderr=logfile)
    return p


def check_tasklogs_dir(spark_dir):
    tasklogs_dir = '%s/tasklogs/' % spark_dir
    if not os.path.exists(tasklogs_dir):
        os.mkdir(tasklogs_dir)
    return tasklogs_dir