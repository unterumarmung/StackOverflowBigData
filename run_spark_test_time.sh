# /bin/bash
for EXECUTOR_COUNT in {1..10}
    do
        hadoop fs -rm -r dudkin/output/sema/*
        /usr/hdp/current/spark2-client/bin/spark-submit --class vsst.StackOverflow --master yarn --deploy-mode client --executor-memory 20G --num-executors ${EXECUTOR_COUNT} ./SparkProject/target/lab2Spark-1.0-SNAPSHOT.jar dudkin/output/sema  | tee output.out
        rm -rf results/*
        hadoop fs -getmerge dudkin/output/sema/mean_time_answer results/mean_time_answer.txt
        hadoop fs -getmerge dudkin/output/sema/top_tags results/top_tags.txt
    done
