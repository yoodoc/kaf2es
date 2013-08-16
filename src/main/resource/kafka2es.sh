#!/bin/bash

zookeeper_address=14.63.199.135 
group_name=kafka2es
topic_name=yoodoc1

run_cmd="java -jar Kafka2EsCmd-jar-with-dependencies.jar --zookeeper=$zookeeper_address --group=$group_name --topic=$topic_name"

echo $run_cmd

