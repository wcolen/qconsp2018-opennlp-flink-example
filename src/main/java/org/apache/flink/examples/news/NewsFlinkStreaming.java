package org.apache.flink.examples.news;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class NewsFlinkStreaming {

  /**
   *
   * @param args
   *  --file <path to article.json.gz>
   * @throws Exception
   */
  public static void main(final String[] args) throws Exception {

    final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

    ParameterTool parameterTool = ParameterTool.fromArgs(args);

    DataStream<NewsArticle> articleStream =
            env.readFile(new NewsArticleInputFormat(), parameterTool.getRequired("file"));

    DataStream<Tuple2<String,Integer>> counts = articleStream
            .flatMap((FlatMapFunction<NewsArticle, Tuple2<String, Integer>>) (article, collector) -> collector.collect(new Tuple2<>(article.getLanguage(), 1)))
            .returns(new TupleTypeInfo(TypeInformation.of(String.class), TypeInformation.of(Integer.class)))
            .keyBy(0)
            .sum(1);

    counts.print();

    env.execute();

  }
}
