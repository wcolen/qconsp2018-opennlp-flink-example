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

  private SourceFunction.SourceContext<Tuple5<String, String, Date, GeoLocation, String>> ctx;

  public TwitterStreamListener(SourceFunction.SourceContext<Tuple5<String, String, Date, GeoLocation, String>> ctx) {
    this.ctx = ctx;
  }

  @Override
  public void onStatus(Status status) {
    Tuple5<String, String, Date, GeoLocation, String> tweet = new Tuple5<>();
    tweet.f0 = status.getUser().getName();
    tweet.f1 = status.getText();
    tweet.f2 = status.getCreatedAt();
    tweet.f3 = status.getGeoLocation();
    tweet.f4 = status.getLang();
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
