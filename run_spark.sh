hadoop fs -rm -r dudkin/output/sema/*
/usr/hdp/current/spark2-client/bin/spark-submit --class vsst.StackOverflow --master yarn --deploy-mode client --executor-memory 20G --num-executors 50 ./SparkProject/target/lab2Spark-1.0-SNAPSHOT.jar dudkin/output/sema  | tee output.out
rm -rf results/*
hadoop fs -getmerge dudkin/output/sema/mean_time_answer results/mean_time_answer.txt
