#!/usr/bin/env bash

#docker-compose -f docker-compose-single-broker.yml up --no-recreate -d
cd /Users/thygesen/Projects/apache/kafka_2.11-1.0.0

bin/zookeeper-server-start.sh config/zookeeper.properties
