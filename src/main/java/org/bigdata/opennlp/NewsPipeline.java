package org.bigdata.opennlp;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.examples.news.AnnotationInputFormat;
import org.apache.flink.examples.news.NewsArticle;
import org.apache.flink.examples.news.NewsArticleAnnotationFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class NewsPipeline {

  /**
   * --parallelism <n>, default=1
   * --file <newsfile.gz>
   * @param args
   */
  public static void main(String[] args) throws Exception {

    ParameterTool parameterTool = ParameterTool.fromArgs(args);

    final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment()
                    .setParallelism(parameterTool.getInt("parallelism", 1));

    DataStream<Annotation<NewsArticle>> articleStream =
            env.readFile(new AnnotationInputFormat(NewsArticleAnnotationFactory.getFactory()), parameterTool.getRequired("file"))
                    .map(new LanguageDetectorFunction<NewsArticle>());

    DataStream<Tuple2<String,Integer>> counts =
            articleStream.flatMap((FlatMapFunction<Annotation<NewsArticle>, Tuple2<String, Integer>>)
                    (annotation, collector) -> collector.collect(new Tuple2<>(annotation.getLanguage(), 1)))
            .returns(new TupleTypeInfo(TypeInformation.of(String.class), TypeInformation.of(Integer.class)))
            .keyBy(0)
            .sum(1);

    counts.print();

    env.execute();

  }

}
