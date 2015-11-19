package org.apache.flink.examples.twitter;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.GeoLocation;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Main Twitter Streaming Demo class
 *
 */
public class TwitterFlinkStreaming {

  private static final Logger LOG = LoggerFactory.getLogger(TwitterFlinkStreaming.class);

  public static void main(String[] args) throws Exception {

    // Get an instance of the Streaming Execution Environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // create a DataStream from TwitterSource
    DataStream<Tweet> twitterStream =
        env.addSource(new TwitterSource("src/main/resources/twitter.properties",
            new String[]{"#dcflinkmeetup", "#datascience"}));

    // Split the Stream based on the Selector criterion - '#DCFlinkMeetup' and others
    SplitStream<Tweet> tweetSplitStream = twitterStream.split(new SplitSelector());

    DataStream<Tweet> dcFlinkTweetStream = tweetSplitStream.select("DCFlinkMeetup");

    DataStream<Tweet> otherTweetStream = tweetSplitStream.select("Others");

    // Persist the Split streams as Text to local filesystem, overwrites any previous files that may exist
    dcFlinkTweetStream.writeAsText("/tmp/DCFlinkTweets", FileSystem.WriteMode.OVERWRITE);
    otherTweetStream.writeAsText("/tmp/OtherTweets", FileSystem.WriteMode.OVERWRITE);

    // Execute the program
    env.execute();
  }

  /**
   * Split the input {@link DataStream} into two DataStreams based on the select criterion
   */
  public static class SplitSelector implements OutputSelector<Tweet> {

    @Override
    public Iterable<String> select(Tweet tweet) {
      LOG.info(tweet.toString());

      List<String> list = new ArrayList<>();
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
