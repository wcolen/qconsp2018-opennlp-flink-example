package org.apache.flink.examples;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * Streaming Window Word Count Example
 */
public class WindowWordCount {

  public static void main(String[] args) throws Exception {
    // Get an instance of the Streaming Execution Environment
    final StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment();

    // Create a DataStream from the text input
    DataStream<String> lines =
        env.readTextFile("src/main/resources/wordcount/input.txt");

    DataStream<Tuple2<String, Integer>> counts =
        lines.flatMap(new LineSplitter())
            .keyBy(0)
            // counts for words every 1 seconds
            .timeWindow(Time.of(1, TimeUnit.SECONDS))
            .sum(1);

    counts.print();

    // Process the DataStream
    env.execute("Streaming Word Count");
  }

  public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
      for (String word : s.split(" ")) {
        collector.collect(new Tuple2<>(word, 1));
      }
    }
  }
}
