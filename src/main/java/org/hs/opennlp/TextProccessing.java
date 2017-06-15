package org.hs.opennlp;

import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileMonitoringFunction;

/**
 * Created by smarthi on 6/13/17.
 */
public class TextProccessing {

  public static void main(String[] args) throws Exception {

    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    String inputStreamDir = parameterTool.get("inputStreamDir");
    String interval = parameterTool.get("interval");

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//    DataStream<String> inputTextStream =
//        env.readFile(new TextInputFormat(new Path(inputStreamDir)), );
  }

}
