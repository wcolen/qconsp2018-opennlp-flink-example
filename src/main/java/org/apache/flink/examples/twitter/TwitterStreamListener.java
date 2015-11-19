package org.apache.flink.examples.twitter;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import twitter4j.GeoLocation;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

import java.util.Date;

public class TwitterStreamListener implements StatusListener {

  private SourceFunction.SourceContext<Tweet> ctx;

  public TwitterStreamListener(SourceFunction.SourceContext<Tweet> ctx) {
    this.ctx = ctx;
  }

  @Override
  public void onStatus(Status status) {
    Tweet tweet = new Tweet();
    tweet.setUserName(status.getUser().getName());
    tweet.setText(status.getText());
    tweet.setDateCreated(status.getCreatedAt());
    tweet.setGeoLocation(status.getGeoLocation());
    tweet.setLanguage(status.getLang());
    ctx.collect(tweet);
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
