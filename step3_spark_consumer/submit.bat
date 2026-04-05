@echo off
REM submit.bat
REM ----------
REM Step 3 - Launch Spark Streaming job on Windows (CMD)
REM
REM Usage (in Command Prompt):
REM   submit.bat

REM FIX: Force UTF-8 output to avoid cp1252 UnicodeEncodeError
set PYTHONIOENCODING=utf-8
set PYTHONUTF8=1

REM FIX: tell Hadoop to use "root" instead of your Windows username
REM      This avoids "Permission denied: user=ACER" HDFS error
set HADOOP_USER_NAME=root

echo ============================================================
echo   CCTV Alert System - Step 3 Spark Submit (Windows CMD)
echo   HADOOP_USER_NAME = %HADOOP_USER_NAME%
echo ============================================================
echo.

REM Pre-create Kafka topic so Spark never hits UnknownTopicOrPartitionException
echo Creating Kafka topic...
docker exec cctv-kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic cctv-hand-events --partitions 2 --replication-factor 1
echo Kafka topic ready.
echo.

REM Pre-create the HDFS output directory with open permissions
REM so Spark never hits a permission error on first write
echo Creating HDFS directory...
docker exec cctv-namenode hdfs dfs -mkdir -p /cctv/raw_events
docker exec cctv-namenode hdfs dfs -chmod -R 777 /cctv
echo HDFS directory ready.
echo.

spark-submit ^
  --master local[*] ^
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.kafka:kafka-clients:3.4.0 ^
  --conf spark.sql.shuffle.partitions=4 ^
  --conf spark.driver.memory=1g ^
  spark_consumer.py
