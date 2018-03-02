package org.bigdata.opennlp.kafka;

import opennlp.tools.chunker.ChunkerModel;
import opennlp.tools.doccat.DoccatModel;
import opennlp.tools.gender.GenderDetectorModel;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.postag.POSModel;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.Span;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.examples.news.kafka.AnnotationKeyedDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.bigdata.opennlp.*;
import org.bigdata.opennlp.functions.*;
import org.bigdata.opennlp.serializer.AnnotationSerializer;
import org.bigdata.opennlp.serializer.SpanSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/*
--topic newsarticles --bootstrap.servers localhost:9092 --zookeeper.connect localhost:2181 --group.id news-consumer --compression.type snappy
 */
public class NewsKafkaConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(NewsKafkaConsumer.class);

  private static SentenceModel engSentenceModel;
  private static TokenizerModel engTokenizerModel;
  private static POSModel engPosModel;
  private static ChunkerModel engChunkModel;
  private static TokenNameFinderModel engNerPersonModel;
  private static DoccatModel engDoccatModel;
  private static GenderDetectorModel engGenderDetectModel;

  private static void initializeModels() throws IOException {
    engSentenceModel = new SentenceModel(NewsKafkaConsumer.class.getResource("/opennlp-models/en-sent.bin"));
    engTokenizerModel = new TokenizerModel(NewsKafkaConsumer.class.getResource("/opennlp-models/en-token.bin"));
    engPosModel= new POSModel(NewsKafkaConsumer.class.getResource("/opennlp-models/en-pos-perceptron.bin"));
    engChunkModel = new ChunkerModel(NewsKafkaConsumer.class.getResource("/opennlp-models/en-chunker.bin"));
    engNerPersonModel = new TokenNameFinderModel(NewsKafkaConsumer.class.getResource("/opennlp-models/en-ner.bin"));
    engDoccatModel = new DoccatModel(NewsKafkaConsumer.class.getResource("/opennlp-models/en-doccat.bin"));
    engGenderDetectModel = new GenderDetectorModel(NewsPipeline.class.getResource("/opennlp-models/en-gender.bin"));

  }

  private static SinkFunction<Annotation> createElasticSearchSink() throws IOException {
    // elastic search configuration
    Map<String,String> config = new HashMap<>();
    config.put("cluster.name", "docker-cluster");
    config.put("bulk.flush.max.actions", "1000");
    config.put("bulk.flush.interval.ms", "10000");

    List<InetSocketAddress> transportAddresses = Stream.of(
            new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300),
            new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9301))
            .collect(Collectors.toList());
    return new ElasticsearchSink<>(config, transportAddresses, new ESSinkFunction());
  }

  public static void main(String[] args) throws Exception {

    initializeModels();

    String[] nlpLanguages = new String[] {"eng", "por"};

    ParameterTool parameterTool = ParameterTool.fromArgs(args);

    final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment()
                    .setParallelism(parameterTool.getInt("parallelism", 1))
                    .setMaxParallelism(10);

    env.getConfig().enableObjectReuse();
    env.getConfig().registerTypeWithKryoSerializer(Annotation.class, AnnotationSerializer.class);
    env.getConfig().registerTypeWithKryoSerializer(Span.class, SpanSerializer.class);

    // Kafka json consumer
    final FlinkKafkaConsumer011<Annotation> consumer =
            new FlinkKafkaConsumer011<>(parameterTool.getRequired("topic"),
                    new AnnotationKeyedDeserializationSchema(),
                    parameterTool.getProperties());


    DataStream<Annotation> newsStream = env.addSource(consumer).filter(a->a.getSofa().length()>400);

    // Perform language detection
    SplitStream<Annotation> annotationStream = newsStream
            .map(new LanguageDetectorFunction())
            .split(new LanguageSelector(nlpLanguages));

    // english nlp pipeline
    annotationStream.select("eng")
            .map(new SentenceDetectorFunction(engSentenceModel))
            .map(new TokenizerFunction(engTokenizerModel))
            .map(new POSTaggerFunction(engPosModel))
            .map(new ChunkerFunction(engChunkModel))
            .map(new NameFinderFunction(engNerPersonModel))
            .map(new GenderDetectorFunction(engGenderDetectModel))
            .map(new DocumentClassifierFunction(engDoccatModel))
            .addSink(createElasticSearchSink());


    env.execute();


  }
}
