package org.hs.opennlp;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by smarthi on 6/2/17.
 */
public class OpenNLPMain {

  private static final Logger LOG = LoggerFactory.getLogger(OpenNLPMain.class);

  public static void main(String[] args) throws Exception {

    // Get an instance of the Streaming Execution Environment
    final StreamExecutionEnvironment streamExecutionEnvironment =
        StreamExecutionEnvironment.getExecutionEnvironment();

    ParameterTool parameterTool = ParameterTool.fromArgs(args);

    streamExecutionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

//    DataStream<>
  }

}
