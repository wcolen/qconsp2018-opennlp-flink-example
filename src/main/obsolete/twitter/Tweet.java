package org.apache.flink.examples.twitter;

import twitter4j.GeoLocation;

import java.util.Date;

/**
 *
 * POJO to represent a tweet which is a Tuple5<Name, Text, Date, GeoLocation, Language>
 *
 */
public class Tweet {

  private String userName;
  private String text;
  private Date dateCreated;
  private GeoLocation geoLocation;
  private String language;

  public Tweet(String userName, String text, Date dateCreated,
               GeoLocation geoLocation, String language) {
    this.userName = userName;
    this.text = text;
    this.dateCreated = dateCreated;
    this.geoLocation = geoLocation;
    this.language = language;
  }

  public String getUserName() {
    return this.userName;
  }

  public String getText() {
    return this.text;
  }

  public Date getDateCreated() {
    return this.dateCreated;
  }

  public GeoLocation getGeoLocation() {
    return this.geoLocation;
  }

  public String getLanguage() {
    return this.language;
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
