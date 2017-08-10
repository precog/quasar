#!/bin/bash

# start spark master
/opt/spark/bin/spark-class org.apache.spark.deploy.master.Master -h $(hostname) &

sleep 2

# start spark worker
/opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://$(hostname):7077
