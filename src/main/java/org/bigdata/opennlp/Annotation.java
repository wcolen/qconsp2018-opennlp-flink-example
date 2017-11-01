package org.bigdata.opennlp;

import java.io.Serializable;

import opennlp.tools.util.Span;

public class Annotation<T> implements Serializable {

  private final String id;
  private final String sofa;
  private String language;
  private final T piggyback;

  private Span headline;
  private Span[] sentences;
  private Span[][] tokens;
  private Span[][] entityMentions;
  private String[][] pos;

  public Annotation(final String id, final String sofa, final T piggyback) {
    this.id = id;
    this.sofa = sofa;
    this.piggyback = piggyback;
  }

  public String getId() {
    return id;
  }

  public String getSofa() {
    return sofa;
  }

  public T getPiggyback() {
    return piggyback;
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

  public Span[][] getEntityMention() {
    return entityMentions;
  }

  public void setEntityMention(Span[][] entityMention) {
    this.entityMentions = entityMention;
  }

  public String[][] getPos() {
    return pos;
  }

  public void setPos(String[][] pos) {
    this.pos = pos;
  }

  public Span[] getSentences() {
    return sentences;
  }

  public void setSentences(Span[] sentences) {
    this.sentences = sentences;

    tokens = new Span[sentences.length][];
    pos = new String[sentences.length][];
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
}
