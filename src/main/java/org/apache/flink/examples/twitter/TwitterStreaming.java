package org.apache.flink.examples.twitter;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import twitter4j.GeoLocation;

import java.util.Date;

public class TwitterStreaming {

  public static void main(String[] args) throws Exception {

    // Get an instance of the Streaming Execution Environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<Tuple5<String, String, Date, GeoLocation, String>> twitterStream =
        env.addSource(new TwitterSource("src/main/resources/twitter.properties",
            new String[]{"#dcflinkmeetup", "#tensorflow"}));

    twitterStream.filter(new MyFilterFunction());
    twitterStream.print();

    env.execute();

  }

  private static class MyFilterFunction implements FilterFunction<Tuple5<String, String, Date, GeoLocation, String>> {
    @Override
    public boolean filter(Tuple5<String, String, Date, GeoLocation, String> tweet) throws Exception {
      return false;
    }
  }
}
