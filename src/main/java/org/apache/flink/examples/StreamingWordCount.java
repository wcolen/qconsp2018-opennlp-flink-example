package org.apache.flink.examples;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Flink Streaming WordCount Example
 */
public class StreamingWordCount {
  public static void main(String[] args) throws Exception {
    // Get an instance of the Streaming Execution Environment
    final StreamExecutionEnvironment streamingExecutionEnvironment =
        StreamExecutionEnvironment.getExecutionEnvironment();

    // Create a DataStream from the streaming input
    DataStream<String> lines =
        streamingExecutionEnvironment.socketTextStream("localhost", 9999);

    DataStream<Tuple2<String, Integer>> counts =
        lines.flatMap(new LineSplitter())
            .keyBy(0)
            .sum(1);

    counts.print();

    // Process the DataStream
    streamingExecutionEnvironment.execute("Streaming Word Count");
  }

  // FlatMap implementation converts each line to multiple <Word, 1> pairs
  public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>>  {
    @Override
    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
      for (String word : s.split(" ")) {
        collector.collect(new Tuple2<>(word, 1));
      }
    }
  }
}
