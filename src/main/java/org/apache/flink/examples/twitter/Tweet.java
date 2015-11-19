package org.apache.flink.examples.twitter;

import twitter4j.GeoLocation;

import java.util.Date;

public class Tweet {

  private String userName;
  private String text;
  private Date dateCreated;
  private GeoLocation geoLocation;
  private String language;

  public Tweet() {
  }

  public Tweet(String userName, String text, Date dateCreated, GeoLocation geoLocation, String language) {
    this.userName = userName;
    this.text = text;
    this.dateCreated = dateCreated;
    this.geoLocation = geoLocation;
    this.language = language;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public String getText() {
    return text;
  }

  public void setText(String text) {
    this.text = text;
  }

  public Date getDateCreated() {
    return dateCreated;
  }

  public void setDateCreated(Date dateCreated) {
    this.dateCreated = dateCreated;
  }

  public GeoLocation getGeoLocation() {
    return geoLocation;
  }

  public void setGeoLocation(GeoLocation geoLocation) {
    this.geoLocation = geoLocation;
  }

  public String getLanguage() {
    return language;
  }

  public void setLanguage(String language) {
    this.language = language;
  }

  @Override
  public String toString() {
    return "Tweet{" +
        "userName='" + userName + '\'' +
        ", text='" + text + '\'' +
        ", dateCreated=" + dateCreated +
        ", geoLocation=" + geoLocation +
        ", language='" + language + '\'' +
        '}';
  }
}
