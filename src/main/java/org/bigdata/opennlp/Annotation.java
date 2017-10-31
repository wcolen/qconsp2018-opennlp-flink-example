package org.bigdata.opennlp;

import opennlp.tools.util.Span;

import java.io.Serializable;

public class Annotation<T> implements Serializable {

  private final String id;
  private final String sofa;
  private String language;
  private final T piggyback;

  private Span headline;
  private Span[] tokens;
  private Span[] sentences;
  private Span[] persons;
  private Span[] pos;

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

  public Span[] getPersons() {
    return persons;
  }

  public void setPersons(Span[] persons) {
    this.persons = persons;
  }

  public Span[] getPos() {
    return pos;
  }

  public void setPos(Span[] pos) {
    this.pos = pos;
  }

  public Span[] getSentences() {
    return sentences;
  }

  public void setSentences(Span[] sentences) {
    this.sentences = sentences;
  }

  public Span[] getTokens() {
    return tokens;
  }

  public void setTokens(Span[] tokens) {
    this.tokens = tokens;
  }
}
