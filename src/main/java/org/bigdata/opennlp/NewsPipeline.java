package org.bigdata.opennlp;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.examples.news.AnnotationInputFormat;
import org.apache.flink.examples.news.NewsArticle;
import org.apache.flink.examples.news.NewsArticleAnnotationFactory;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;

import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.postag.POSModel;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.tokenize.TokenizerModel;

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
            .setParallelism(parameterTool.getInt("parallelism", 5)).setMaxParallelism(10);

    DataStream<Annotation<NewsArticle>> rawStream =
            env.readFile(new AnnotationInputFormat(NewsArticleAnnotationFactory.getFactory()), parameterTool.getRequired("file"))
                    .map(new LanguageDetectorFunction<>());

    SplitStream<Annotation<NewsArticle>> articleStream = rawStream.split(new LanguageSelector());

    SentenceModel engSentenceModel =
        new SentenceModel(NewsPipeline.class.getResource("/opennlp-models/en-sent.bin"));

    TokenizerModel engTokenizerModel =
        new TokenizerModel(NewsPipeline.class.getResource("/opennlp-models/en-token.bin"));

    SingleOutputStreamOperator<Annotation<NewsArticle>> eng = articleStream.select("eng")
        .map(new SentenceDetectorFunction<>(engSentenceModel))
        .map(new TokenizerFunction<>(engTokenizerModel));

    POSModel engPosModel = new POSModel(NewsPipeline.class.getResource("/opennlp-models/en-pos-perceptron.bin"));

    TokenNameFinderModel engNerPersonModel =
        new TokenNameFinderModel(NewsPipeline.class.getResource("/opennlp-models/en-ner-person.bin"));

    SingleOutputStreamOperator<Annotation<NewsArticle>> analyzedEng = eng.setParallelism(2)
        .map(new POSTaggerFunction<>(engPosModel)).setParallelism(2)
        .map(new NameFinderFunction<>(engNerPersonModel));

    Map<String,String> config = new HashMap<>();
    config.put("cluster.name", "docker-cluster");
    config.put("bulk.flush.max.actions", "1000");

    List<InetSocketAddress> transportAddresses = Lists.newArrayList(
            new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300),
            new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9301));

    analyzedEng.addSink(new ElasticsearchSink<Annotation<NewsArticle>>(config, transportAddresses, new ESSinkFunction()));


/*
    SingleOutputStreamOperator eng2 = articleStream.flatMap((FlatMapFunction<Annotation<NewsArticle>, Tuple2<String, Integer>>)
        (annotation, collector) -> collector.collect(new Tuple2<>(annotation.getLanguage(), 1)))
        .returns(new TupleTypeInfo(TypeInformation.of(String.class), TypeInformation.of(Integer.class)))
        .keyBy(0)
        .sum(1);
*/
    analyzedEng.print();

    //eng.print();

    env.execute();

  }

  private static class LanguageSelector<T> implements OutputSelector<Annotation<T>> {
    @Override
    public Iterable<String> select(Annotation<T> annotation) {
      return Collections.singletonList(annotation.getLanguage());
    }
  }
}
