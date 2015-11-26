package org.apache.flink.examples.twitter;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.FilterQuery;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * Twitter Feed Source that extends Flink's {@link RichSourceFunction}
 */
public class TwitterSource extends RichSourceFunction<Tweet> {

  private static final Logger LOG = LoggerFactory.getLogger(TwitterSource.class);

  private volatile boolean isRunning = true;
  private String authPath;
  private String[] filterTerms;

  public TwitterSource(String authPath, String[] filterTerms) {
    this.authPath = authPath;
    this.filterTerms = filterTerms;
  }

  @Override
  public void run(SourceContext<Tweet> sourceContext) throws Exception {

    ConfigurationBuilder builder = new ConfigurationBuilder();
    setUpTwitterConfiguration(builder);

    // Get an instance of TwitterStream
    TwitterStream twitterStream = new TwitterStreamFactory(builder.build()).getInstance();

    // Add the Twitter Status Listener to TwitterStream
    twitterStream.addListener(new TwitterStreamListener(sourceContext));

    // Filter Query to capture only English language Tweets with one of the specified filter terms
    FilterQuery filterQuery = new FilterQuery(0, null, filterTerms, null, new String[]{"en"});

    // Add FilterQuery to TwitterStream, this should now only capture tweets that satisfy the filters
    twitterStream.filter(filterQuery);

    while (isRunning) {
      // keep going
    }
  }

  private void setUpTwitterConfiguration(ConfigurationBuilder builder) {
    // Load the Twitter OAuth credentials
    Properties properties = loadAuthenticationProperties();

    builder.setIncludeMyRetweetEnabled(true);
    builder.setUserStreamWithFollowingsEnabled(true);
    builder.setPrettyDebugEnabled(true);

    // Set the OAuth credentials
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
    } catch (Exception ex) {
      throw new RuntimeException("Cannot open twitter.properties file: " + authPath, ex);
    }
    return properties;
  }

  @Override
  public void cancel() {
    this.isRunning = false;
  }
}
