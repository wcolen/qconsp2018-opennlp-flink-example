package org.apache.flink.examples.twitter;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.FilterQuery;
import twitter4j.GeoLocation;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Date;
import java.util.Properties;

/**
 * Twitter Feed Source
 */
public class TwitterSource extends RichSourceFunction<Tuple5<String, String, Date, GeoLocation, String>> {

  private static final Logger LOG = LoggerFactory.getLogger(TwitterSource.class);

  private volatile boolean isRunning = true;
  private String authPath;
  private String[] filterTerms;

  public TwitterSource(String authPath) {
    this.authPath = authPath;
  }

  public TwitterSource(String authPath, String[] filterTerms) {
    this.authPath = authPath;
    this.filterTerms = filterTerms;
  }

  @Override
  public void run(SourceContext<Tuple5<String, String, Date, GeoLocation, String>> sourceContext)
      throws Exception {

    ConfigurationBuilder builder = new ConfigurationBuilder();
    setUpTwitterConfiguration(builder);

    // Get an instance of TwitterStream
    TwitterStream twitterStream = new TwitterStreamFactory(builder.build()).getInstance();
    twitterStream.addListener(new TwitterStreamListener(sourceContext));

    // Filter Query to capture only English language Tweets
    FilterQuery filterQuery = new FilterQuery(0, null, filterTerms, null, new String[]{"en"});
    twitterStream.filter(filterQuery);

    while (isRunning) {
      // keep going
    }
  }

  private void setUpTwitterConfiguration(ConfigurationBuilder builder) {
    // Load the Twitter OAuth credentials
    Properties properties = loadAuthenticationProperties();

    builder.setDebugEnabled(true);
    builder.setIncludeMyRetweetEnabled(true);
    builder.setUserStreamWithFollowingsEnabled(true);
    builder.setPrettyDebugEnabled(true);

    builder.setOAuthConsumerKey(properties.getProperty("consumerKey"));
    builder.setOAuthConsumerSecret(properties.getProperty("consumerSecret"));
    builder.setOAuthAccessToken(properties.getProperty("accessToken"));
    builder.setOAuthAccessTokenSecret(properties.getProperty("accessSecret"));

  }

  /**
   * Reads the given properties file for the authentication data.
   * @return the authentication data.
   */
  private Properties loadAuthenticationProperties() {
    Properties properties = new Properties();
    try (InputStream input = new FileInputStream(authPath)) {
      properties.load(input);
    } catch (Exception e) {
      throw new RuntimeException("Cannot open .properties file: " + authPath, e);
    }
    return properties;
  }

  @Override
  public void cancel() {
    this.isRunning = false;
  }
}
