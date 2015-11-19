package org.apache.flink.examples.twitter;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

/**
 *
 * This is a Listener needed by Twitter4J API
 * Define methods that would be invoked by the Twitter4J API  - especially onStatus().
 * Flink DataStream API is made aware of the incoming tweets by this Listener which has a handle to
 * {@link org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext} and
 * all incoming tweets are then forwarded to the DataStream API
 * via {@link SourceFunction.SourceContext#collect} method
 *
 */
public class TwitterStreamListener implements StatusListener {

  private SourceFunction.SourceContext<Tweet> ctx;

  public TwitterStreamListener(SourceFunction.SourceContext<Tweet> ctx) {
    this.ctx = ctx;
  }

  @Override
  public void onStatus(Status status) {
    ctx.collect(
        new Tweet(
            status.getUser().getName(),
            status.getText(),
            status.getCreatedAt(),
            status.getGeoLocation(),
            status.getLang())
    );
  }

  @Override
  public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
    // do nothing
  }

  @Override
  public void onTrackLimitationNotice(int i) {
    // do nothing
  }

  @Override
  public void onScrubGeo(long l, long l1) {
    // do nothing
  }

  @Override
  public void onStallWarning(StallWarning stallWarning) {
    // do nothing
  }

  @Override
  public void onException(Exception ex) {
    ex.printStackTrace();
  }
}
