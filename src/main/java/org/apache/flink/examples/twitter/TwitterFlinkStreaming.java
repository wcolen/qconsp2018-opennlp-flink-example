package org.apache.flink.examples.twitter;

import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
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
    twitterSource.setCustomEndpointInitializer(new TwitterFlinkStreaming.FilterEndpoint("#MondayMotivation", "#ColumbusDay"));

    // create a DataStream from TwitterSource
    DataStream<String> twitterStream = env.addSource(twitterSource);


    // Split the Stream based on the Selector criterion - '#DCFlinkMeetup' and others
    SplitStream<String> tweetSplitStream = twitterStream.filter(new FilterFunction<String>() {
      @Override
      public boolean filter(String value) throws Exception {
        return !StringUtils.isNumeric(value);
      }
    }).map(new TwitterFlinkStreaming.JsonConverter()).split(new TwitterFlinkStreaming.SplitSelector());

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

  private static class JsonConverter extends RichMapFunction<String,String> {
    private transient ObjectMapper mapper;

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      mapper = new ObjectMapper();
    }

    @Override
    public String map(String value) throws Exception {
      JsonNode tweet = mapper.readValue(value, JsonNode.class);
      return tweet.get("text").asText();
    }
  }

  /**
   * Split the input {@link DataStream} into two DataStreams based on the select criterion
   */
  public static class SplitSelector implements OutputSelector<String> {

    @Override
    public Iterable<String> select(String text) {

      LOG.info(text);

      List<String> list = new ArrayList<>();
      if (text.contains("#dcflinkmeetup")) {
        list.add("DCFlinkMeetup");
      } else {
        list.add("Others");
      }
      return list;
    }
  }

  private static class FilterEndpoint implements TwitterSource.EndpointInitializer, Serializable {

    private final List<String> tags;
    public FilterEndpoint(final String ... tags) {
      this.tags = Lists.newArrayList(tags);
    }

    @Override
    public StreamingEndpoint createEndpoint() {
      StatusesFilterEndpoint ep = new StatusesFilterEndpoint();
      ep.trackTerms(tags);
      return ep;
    }
  }
}