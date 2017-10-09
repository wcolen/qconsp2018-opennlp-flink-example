package org.apache.flink.examples.twitter;

import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Serializable;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Main Twitter Streaming Demo class
 *
 */
public class TwitterFlinkStreaming {

  private static final Logger LOG = LoggerFactory.getLogger(TwitterFlinkStreaming.class);

  public static void main(String[] args) throws Exception {

    // Get an instance of the Streaming Execution Environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

    // twitter credentials & source
    Properties props = new Properties();
    props.load(TwitterFlinkStreaming.class.getResourceAsStream("/twitter.properties"));
    TwitterSource twitterSource = new TwitterSource(props);
    twitterSource.setCustomEndpointInitializer(new MyFilterEndpoint());

    // create a DataStream from TwitterSource
    DataStream<String> twitterStream = env.addSource(twitterSource);

    // Split the Stream based on the Selector criterion - '#DCFlinkMeetup' and others
    SplitStream<String> tweetSplitStream = twitterStream.split(new SplitSelector());

    DataStream<String> dcFlinkTweetStream = tweetSplitStream.select("apacheBigData");

    DataStream<String> otherTweetStream = tweetSplitStream.select("ColumbusDay");

    // Persist the Split streams as Text to local filesystem, overwrites any previous files that may exist
    dcFlinkTweetStream.writeAsText("/tmp/MondayMotivation", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
    otherTweetStream.writeAsText("/tmp/ColumbusDay", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

    // Join two different streams
    //    JoinedStreams<Tweet, Tweet> tweetStream = dcFlinkTweetStream.join(otherTweetStream);

    // TODO: Add Tumbling Window Semantics

    // Execute the program
    env.execute();
  }

  /**
   * Split the input {@link DataStream} into two DataStreams based on the select criterion
   */
  public static class SplitSelector implements OutputSelector<String> {
    @Override
    public Iterable<String> select(String tweet) {
      LOG.info(tweet.toString());

      List<String> list = new ArrayList<>();
      if (tweet.toLowerCase().contains("#dcflinkmeetup")) {
        list.add("DCFlinkMeetup");
      } else {
        list.add("Others");
      }
      return list;
    }
  }

  private static class MyFilterFunction implements FilterFunction<String> {
    @Override
    public boolean filter(String tweet) throws Exception {
      return false;
    }
  }

  private static class MyFilterEndpoint implements TwitterSource.EndpointInitializer, Serializable {

    @Override
    public StreamingEndpoint createEndpoint() {
      StatusesFilterEndpoint ep = new StatusesFilterEndpoint();
      ep.trackTerms(Lists.newArrayList("MondayMotivation", "ColumbusDay"));
      return ep;
    }
  }
}
