package org.bigdata.opennlp.functions;

import opennlp.tools.util.Span;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.bigdata.opennlp.Annotation;
import org.bigdata.opennlp.sentiment.SentimentDetector;
import org.bigdata.opennlp.sentiment.SentimentDetectorTF;

import java.util.Arrays;
import java.util.List;

public class SentimentDetectorFunction extends RichMapFunction<Annotation,Annotation> {

  private final String sentimentModel;
  private transient SentimentDetector sentimentDetector;

  public SentimentDetectorFunction(String model) {
    this.sentimentModel = model;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    sentimentDetector = new SentimentDetectorTF(sentimentModel);
  }


  @Override
  public Annotation map(Annotation annotation) {

    List<String> sentences = Arrays.asList(Span.spansToStrings(annotation.getSentences(), annotation.getSofa()));

    int[] sentiment = sentimentDetector.sentimentDetect(sentences);
    if (sentiment[0] == SentimentDetector.NEGATIVE)
      annotation.setSentiment("NEGATIVE");
    else
      annotation.setSentiment("POSITIVE");

    if (sentiment[1] == SentimentDetector.NEGATIVE)
      annotation.setSentimentSum("NEGATIVE");
    else
      annotation.setSentimentSum("POSITIVE");

    return annotation;
  }

  @Override
  public void close() throws Exception {
    super.close();
    sentimentDetector.close();
  }

}
