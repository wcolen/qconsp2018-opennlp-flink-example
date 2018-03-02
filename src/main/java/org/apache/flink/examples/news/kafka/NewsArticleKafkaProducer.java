package org.apache.flink.examples.news.kafka;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

public class NewsArticleKafkaProducer {

  /*
  java -cp JAR org.apache.flink.examples.news.kafka.NewsArticleKafkaProducer
  --newsdatafile /Users/thygesen/Projects/data/10k.json.gz
  --topic newsarticles
  --bootstrap.servers localhost:9092
  --client.id news-producer
  --key.serializer org.apache.kafka.common.serialization.StringSerializer
  --value.serializer org.apache.kafka.common.serialization.StringSerializer
  --compression.type snappy
   */
  public static void main(String[] args) throws Exception {

    ParameterTool params = ParameterTool.fromArgs(args);

    try(BufferedReader in = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(params.getRequired("newsdatafile"))), "UTF8"))) {
      try (KafkaProducer<String, String> producer = new KafkaProducer<String, String>(params.getProperties())) {

        String line;
        while ((line = in.readLine()) != null) {
          final ProducerRecord msg = new ProducerRecord<String, String>(params.getRequired("topic"), String.valueOf(line.hashCode()), line);
          producer.send(msg, (meta, exception) -> {
            if (meta == null)
              exception.printStackTrace();
          });
        }
        producer.flush();
        Thread.sleep(10000);
      }
    }

  }
}
/*
start:
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties

create topic:
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic newsarticles

view:
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic newsarticles --key-deserializer org.apache.kafka.common.serialization.StringDeserializer --value-deserializer org.apache.kafka.common.serialization.StringDeserializer --from-beginning

purge:
#bin/kafka-topics.sh --zookeeper localhost:2181 --alter --topic newsarticles --config retention.ms=1000
bin/kafka-configs.sh --zookeeper localhost:2181 --add-config retention.ms=1000 --entity-name newsarticles --entity-type topics --alter --force
then wait for 1 min.. set to 10. min default is: 604800000 = 168 hours
#bin/kafka-topics.sh --zookeeper localhost:2181 --alter --topic newsarticles --config retention.ms=600000
bin/kafka-configs.sh --zookeeper localhost:2181 --add-config retention.ms=600000 --entity-name newsarticles --entity-type topics --alter
 */