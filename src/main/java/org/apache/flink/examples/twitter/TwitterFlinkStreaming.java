package org.apache.flink.examples.twitter;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import twitter4j.GeoLocation;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class TwitterFlinkStreaming {

  public static void main(String[] args) throws Exception {

    // Get an instance of the Streaming Execution Environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<Tweet> twitterStream =
        env.addSource(new TwitterSource("src/main/resources/twitter.properties",
            new String[]{"#dcflinkmeetup", "#datascience"}));

    // Split the Stream based on the Selector criterion - '#DCFlinkMeetup' and others
    SplitStream<Tweet> tweetSplitStream = twitterStream.split(new SplitSelector());

    DataStream<Tweet> dcFlinkTweetStream = tweetSplitStream.select("DCFlinkMeetup");

    DataStream<Tweet> otherTweetStream = tweetSplitStream.select("Others");

    dcFlinkTweetStream.writeAsText("/tmp/DCFlinkTweets", FileSystem.WriteMode.OVERWRITE);
    otherTweetStream.writeAsText("/tmp/OtherTweets", FileSystem.WriteMode.OVERWRITE);

    env.execute();
  }

  /**
   * Split the input {@link DataStream} into two based on the select criterion
   */
  public static class SplitSelector implements OutputSelector<Tweet> {

    @Override
    public Iterable<String> select(Tweet tweet) {
      List<String> list = new ArrayList<>();
      System.out.println(tweet.toString());
      if (tweet.getText().toLowerCase().contains("#dcflinkmeetup")) {
        list.add("DCFlinkMeetup");
      } else {
        list.add("Others");
      }
      return list;
    }
  }

  private static class MyFilterFunction implements FilterFunction<Tuple5<String, String, Date, GeoLocation, String>> {
    @Override
    public boolean filter(Tuple5<String, String, Date, GeoLocation, String> tweet) throws Exception {
      return false;
    }
  }
}
