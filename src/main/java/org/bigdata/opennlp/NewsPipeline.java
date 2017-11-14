package org.bigdata.opennlp;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.examples.news.AnnotationInputFormat;
import org.apache.flink.examples.news.NewsArticleAnnotationFactory;
import org.apache.flink.shaded.com.google.common.collect.Sets;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import opennlp.tools.chunker.ChunkerModel;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.postag.POSModel;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.tokenize.TokenizerModel;

public class NewsPipeline {

  private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");

  private static final Logger LOG = LoggerFactory.getLogger(NewsPipeline.class);

  private static SentenceModel engSentenceModel;
  private static TokenizerModel engTokenizerModel;
  private static POSModel engPosModel;
  private static ChunkerModel engChunkModel;
  private static TokenNameFinderModel engNerPersonModel;

  private static SentenceModel porSentenceModel;
  private static TokenizerModel porTokenizerModel;
  private static POSModel porPosModel;
  private static ChunkerModel porChunkModel;
  private static TokenNameFinderModel porNerPersonModel;

  private static void initializeModels() throws IOException {
    engSentenceModel = new SentenceModel(NewsPipeline.class.getResource("/opennlp-models/en-sent.bin"));
    engTokenizerModel = new TokenizerModel(NewsPipeline.class.getResource("/opennlp-models/en-token.bin"));
    engPosModel= new POSModel(NewsPipeline.class.getResource("/opennlp-models/en-pos-perceptron.bin"));
    engChunkModel = new ChunkerModel(NewsPipeline.class.getResource("/opennlp-models/en-chunker.bin"));
    engNerPersonModel = new TokenNameFinderModel(NewsPipeline.class.getResource("/opennlp-models/en-ner-person.bin"));

    // TODO: we need a portugese model here
    porSentenceModel = new SentenceModel(NewsPipeline.class.getResource("/opennlp-models/en-sent.bin"));
    porTokenizerModel = new TokenizerModel(NewsPipeline.class.getResource("/opennlp-models/por-token.bin"));
    porPosModel = new POSModel(NewsPipeline.class.getResource("/opennlp-models/por-pos-maxent.bin"));
    porChunkModel = new ChunkerModel(NewsPipeline.class.getResource("/opennlp-models/por-chunker.bin"));
    porNerPersonModel = new TokenNameFinderModel(NewsPipeline.class.getResource("/opennlp-models/por-ner.bin"));
  }

  /**
   * --parallelism <n>, default=1
   * --file <newsfile.gz>
   * @param args
   */
  public static void main(String[] args) throws Exception {

    LOG.info("Started: " + dtf.format(LocalDateTime.now()));

    initializeModels();

    LOG.info("Models loaded: " + dtf.format(LocalDateTime.now()));

    String[] nlpLanguages = new String[] {"eng", "por"};

    ParameterTool parameterTool = ParameterTool.fromArgs(args);

    final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment()
            .setParallelism(parameterTool.getInt("parallelism", 1)).setMaxParallelism(10);

    // elastic search configuration
    Map<String,String> config = new HashMap<>();
    config.put("cluster.name", "docker-cluster");
    config.put("bulk.flush.max.actions", "1000");
    config.put("bulk.flush.interval.ms", "10000");

    List<InetSocketAddress> transportAddresses = Stream.of(
        new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300),
        new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9301))
        .collect(Collectors.toList());

    DataStream<Annotation> rawStream = env.readFile(
        new AnnotationInputFormat(NewsArticleAnnotationFactory.getFactory()), parameterTool.getRequired("file"));

    // Perform language detection
    SplitStream<Annotation> articleStream = rawStream
        .map(new LanguageDetectorFunction())
        .split(new LanguageSelector(nlpLanguages));

    // English NLP pipeline
    articleStream.select("eng")
        .map(new SentenceDetectorFunction(engSentenceModel))
        .map(new TokenizerFunction(engTokenizerModel))
        .map(new POSTaggerFunction(engPosModel))
        .map(new ChunkerFunction(engChunkModel))
        .map(new NameFinderFunction(engNerPersonModel))
        .addSink(new ElasticsearchSink<>(config, transportAddresses, new ESSinkFunction()));

    // Portuguese NLP pipeline
    articleStream.select("por")
        .map(new SentenceDetectorFunction(porSentenceModel))
        .map(new TokenizerFunction(porTokenizerModel))
        .map(new POSTaggerFunction(porPosModel))
        .map(new ChunkerFunction(porChunkModel))
        .map(new NameFinderFunction(porNerPersonModel))
        .addSink(new ElasticsearchSink<>(config, transportAddresses, new ESSinkFunction()));

    // Index the articles in all of the other languages into ES
    articleStream.select(LanguageSelector.OTHER_LANGUAGES)
            .addSink(new ElasticsearchSink<>(config, transportAddresses, new ESSinkFunction()));

    env.execute();

    LOG.info("Done: " + dtf.format(LocalDateTime.now()));
  }

  private static class LanguageSelector implements OutputSelector<Annotation> {
    public static String OTHER_LANGUAGES = "OTHER_LANGUAGES";

    private final Set<String> supportedLanguaged;

    public LanguageSelector(String ... languages) {
      supportedLanguaged = Sets.newHashSet(languages);
    }

    @Override
    public Iterable<String> select(Annotation annotation) {
      if (supportedLanguaged.contains(annotation.getLanguage()))
        return Collections.singletonList(annotation.getLanguage());
      else
        return Collections.singletonList(OTHER_LANGUAGES);
    }
  }
}
