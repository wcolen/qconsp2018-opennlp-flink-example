package org.apache.flink.examples;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class SplitStreamTest {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // Create a DataStream from the streaming input
    final DataStream<String> lines = env.socketTextStream("localhost", 9999);

    SplitStream<String> out = lines.split(new SplitSelector());

    DataStream<String> odd = out.select("odd");
    DataStream<String> even = out.select("even");

    odd.writeAsText("/tmp/odd", FileSystem.WriteMode.NO_OVERWRITE);
    even.writeAsText("/tmp/even", FileSystem.WriteMode.OVERWRITE);

    env.execute();
  }

  public static class SplitSelector implements OutputSelector<String> {

    @Override
    public Iterable<String> select(String s) {
      List<String> list = new ArrayList<>();
      list.add(Integer.valueOf(s) % 2 == 0 ? "even" : "odd");
      return list;
     }
  }
}
