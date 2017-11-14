package org.bigdata.opennlp;

import java.io.Serializable;
import java.util.Map;

import org.apache.flink.shaded.com.google.common.collect.Maps;

import opennlp.tools.util.Span;

public class Annotation implements Serializable {

  private final String id;
  private final String sofa;
  private String language;
  private final Map<String,Object> properties = Maps.newHashMap();

  private Span headline;
  private Span[] sentences;
  private Span[][] tokens;
  private String[][] personMentions;
  private String[][] organizationMentions;
  private String[][] locationMentions;

  private String[][] pos;
  private Span[][] chunks;

  public Annotation(final String id, final String sofa) {
    this.id = id;
    this.sofa = sofa;
  }

  public String getId() {
    return id;
  }

  public String getSofa() {
    return sofa;
  }

  public Span getHeadline() {
    return headline;
  }

  public void setHeadline(Span headline) {
    this.headline = headline;
  }

  public String getLanguage() {
    return language;
  }

  public void setLanguage(String language) {
    this.language = language;
  }

  public String[][] getPersonMention() {
    return personMentions;
  }

  public String[][] getOrganizationMention() {
    return organizationMentions;
  }

  public String[][] getLocationMention() {
    return locationMentions;
  }

  public void setPersonMention(String[][] entityMention) {
    this.personMentions = entityMention;
  }

  public void setOrganizationMention(String[][] entityMention) {
    this.organizationMentions = entityMention;
  }

  public void setLocatioMention(String[][] entityMention) {
    this.locationMentions = entityMention;
  }

  public String[][] getPos() {
    return pos;
  }

  public void setPos(String[][] pos) {
    this.pos = pos;
  }

  public Object putProperty(String key, Object value) {
    return properties.put(key, value);
  }

  public Object getProperty(String key) {
    return properties.get(key);
  }

  public Span[] getSentences() {
    if (sentences != null) {
      return sentences;
    }
    else {
      return new Span[0];
    }
  }

  public void setSentences(Span[] sentences) {
    this.sentences = sentences;

    tokens = new Span[sentences.length][];
    pos = new String[sentences.length][];
    personMentions = new String[sentences.length][];
    organizationMentions = new String[sentences.length][];
    locationMentions = new String[sentences.length][];
    chunks = new Span[sentences.length][];
  }

  public Span[][] getTokens() {
    return tokens;
  }

  public void setTokens(Span[][] tokens) {
    this.tokens = tokens;
  }

  @Override
  public String toString() {
    return getLanguage() + " : " + getSofa().substring(getHeadline().getStart(), getHeadline().getEnd());
  }

  public Span[][] getChunks() {
    return chunks;
  }
}
