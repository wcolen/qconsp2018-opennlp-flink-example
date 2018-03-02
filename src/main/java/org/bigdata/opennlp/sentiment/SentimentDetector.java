package org.bigdata.opennlp.sentiment;

import java.util.List;

public interface SentimentDetector extends AutoCloseable {
  int sentimentDetect(String text);
  int sentimentDetect(String[] tokens);
  int[] sentimentDetect(List<String> sentences);

  static final int NEGATIVE = 0;
  static final int POSITIVE = 1;
}
