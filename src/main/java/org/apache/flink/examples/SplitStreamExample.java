package org.apache.flink.examples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SplitStreamExample {

  private static final int INPUT_MAX = 100;
  private static final int MAX_NUMBER_OF_ELEMENTS = 10000;

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // Create a DataStream from input
    final DataStream<Tuple2<Long, Long>> tuples = getData(env);

    // Split the Stream based on the Selector criterion - 'odd' and 'even'
    SplitStream<Tuple2<Long, Long>> out = tuples.split(new SplitSelector());

    // Select the split streams based on 'odd' vs 'even'
    DataStream<Tuple2<Long, Long>> odd = out.select("odd");
    DataStream<Tuple2<Long, Long>> even = out.select("even");

    // Write 'odd' split as CSV
    odd.writeAsCsv("/tmp/sumOdd", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
    // Write 'even' split as text
    even.writeAsText("/tmp/sumEven", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

    // Process the Stream
    env.execute();
  }

  /**
   * Split the input {@link DataStream} into based on the select criterion
   */
  public static class SplitSelector implements OutputSelector<Tuple2<Long, Long>> {

    @Override
    public Iterable<String> select(Tuple2<Long, Long> tuple2) {
      List<String> list = new ArrayList<>();
      list.add((tuple2.f0 + tuple2.f1) % 2 == 0 ? "even" : "odd");
      return list;
     }
  }

  /**
   * Generate Random Tuples of <Long, Long>
   *
   * @param env - {@link StreamExecutionEnvironment}
   * @return {@link DataStream<Tuple2>}
   */
  public static DataStream<Tuple2<Long, Long>> getData(StreamExecutionEnvironment env) {
    List<Tuple2<Long, Long>> list = new ArrayList<>();
    Random rnd = new Random();
    for (int i = 0; i < MAX_NUMBER_OF_ELEMENTS; i++) {
      long r = rnd.nextInt(INPUT_MAX);
      list.add(new Tuple2<>(r, r + rnd.nextInt(10)));
    }
    // Create a DataStream from the collection
    return env.fromCollection(list);
  }
}
