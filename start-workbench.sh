#!/bin/bash

docker-compose up -d

my_ip=`ip route get 1|awk '{print $NF;exit}'`
echo "Namenode: http://${my_ip}:50070"
echo "Datanode: http://${my_ip}:50075"
echo "Spark-master: http://${my_ip}:8082"
echo "Spark-notebook: http://${my_ip}:9001"
echo "Hue (DataLake Filebrowser): http://${my_ip}:8088/home"
echo "PgAdmin (Data warehouse): http://${my_ip}:8082/home"
echo "Airflow UI: http://${my_ip}:8080/home"