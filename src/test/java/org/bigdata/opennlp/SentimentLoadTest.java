package org.bigdata.opennlp;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.bigdata.opennlp.sentiment.SentimentDetector;
import org.bigdata.opennlp.sentiment.SentimentDetectorTF;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.counting;

public class SentimentLoadTest {

  public static void main(String[] args) {

    List<String> l = Lists.newArrayList("a", "b", "c", "b", "c", "c");

    Map<String,Long> m = l.stream().collect(Collectors.groupingBy(Function.identity(), counting()));

    m.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).map(Map.Entry::getKey).collect(Collectors.toList());

    SentimentDetector detector = new SentimentDetectorTF("/Users/thygesen/Projects/flink/DC-FlinkMeetup/src/main/resources/model");

    System.out.println();
  }
}
