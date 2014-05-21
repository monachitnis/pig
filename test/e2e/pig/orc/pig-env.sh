#!/bin/bash

export PIG_HOME=`pwd`
export PIG_BIN=`pwd`/bin/pig
export HADOOP_HOME=$HADOOP_PREFIX
export PIG_CLASSPATH=/homes/chitnis/pigorc/pig-0.13.0-SNAPSHOT.jar:/homes/chitnis/pigorc/pig-0.13.0-SNAPSHOT-withouthadoop.jar:/home/y/libexec/hive13/lib/hive-exec.jar:/home/y/libexec/hive13/lib/hive-metastore.jar:/home/y/libexec/hive13/lib/hive-common.jar:/home/y/libexec/hive13/lib/hive-serde.jar:/home/y/libexec/hive13/lib/libfb303.jar:/home/y/libexec/hive/conf:/home/y/libexec/hive13/lib/hcatalog-core.jar:/home/y/libexec/hive13/lib/hcatalog-pig-adapter.jar:/home/y/libexec/hive13/auxlib/jdo-api-3.0.1.jar

export HIVE_HOME=/home/y/libexec/hive13
export HIVE_CONF_DIR=/home/y/libexec/hive/conf
export PATH=$HIVE_HOME/bin/:$PATH
export CLASSPATH=/home/y/libexec/hive13/lib:/home/y/libexec/hive13/auxlib/:/home/y/libexec/hive/conf/

#./pig -Dmapreduce.job.queuename=grideng -Dpig.additional.jars=/home/y/libexec/hive13/lib/hive-exec.jar:/home/y/libexec/hive13/lib/hive-metastore.jar:/home/y/libexec/hive13/lib/libfb303.jar:/home/y/libexec/hive13/lib/hcatalog-core.jar:/home/y/libexec/hive13/lib/hcatalog-pig-adapter.jar:/home/y/libexec/hive13/auxlib/jdo-api-3.0.1.jar -f test.pig

