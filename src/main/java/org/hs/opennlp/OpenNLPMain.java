package org.hs.opennlp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.kryo.JavaSerializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import opennlp.tools.chunker.ChunkerME;
import opennlp.tools.chunker.ChunkerModel;
import opennlp.tools.langdetect.LanguageDetectorME;
import opennlp.tools.langdetect.LanguageDetectorModel;
import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.NameSample;
import opennlp.tools.namefind.TokenNameFinder;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.Span;

public class OpenNLPMain {

  private static final Logger LOG = LoggerFactory.getLogger(OpenNLPMain.class);

  String modelsDir = "opennlp-models";

  private static SentenceDetectorME engSentenceDetector = null;
  private static TokenizerME engTokenizer = null;
  private static POSTaggerME engPosTagger = null;
  private static ChunkerME engChunker = null;
  private static LanguageDetectorME languageDetectorME = null;
  private static TokenNameFinder engNameFinder = null;

  private static SentenceDetectorME porSentenceDetector = null;
  private static TokenizerME porTokenizer = null;
  private static POSTaggerME porPosTagger = null;
  private static ChunkerME porChunker = null;
  private static TokenNameFinder porNameFinder = null;

  public static void main(String[] args) throws Exception {

    OpenNLPMain nlp = new OpenNLPMain();
    nlp.initializeModels();

    // Get an instance of the Streaming Execution Environment
    final StreamExecutionEnvironment streamExecutionEnvironment =
        StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

    streamExecutionEnvironment.getConfig()
        .registerTypeWithKryoSerializer(POSSample.class, JavaSerializer.class);

    streamExecutionEnvironment.getConfig()
        .registerTypeWithKryoSerializer(NameSample.class, JavaSerializer.class);

    ParameterTool parameterTool = ParameterTool.fromArgs(args);

    streamExecutionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

    DataStream<String> inputStream =
        streamExecutionEnvironment.readTextFile(
        OpenNLPMain.class.getResource("/input/eng_news_2015_100K-sentences.txt").getFile());

    // read german text
    DataStream<String> portugeseText =
        streamExecutionEnvironment.readTextFile(
            OpenNLPMain.class.getResource("/input/por-br_newscrawl_2011_100K-sentences.txt").getFile());

    // Merge all streams
    DataStream<String> mergedStream = inputStream.union(portugeseText);

    // Parse the text
    DataStream<Tuple2<String, String>> newsArticles = mergedStream.map(new LeipzigParser());

    SplitStream<Tuple2<String, String>> langStream = newsArticles.split(new LanguageSelector());

    DataStream<Tuple2<String, String>> engNewsArticles = langStream.select("eng");
    DataStream<Tuple2<String, String[]>> engNewsTokenized = engNewsArticles.map(new EngTokenizerMapFunction());

    DataStream<POSSample> engNewsPOS = engNewsTokenized.map(new EngPOSTaggerMapFunction());
    DataStream<NameSample> engNewsNamedEntities = engNewsTokenized.map(new EngNameFinderMapFunction());

    DataStream<Tuple2<String, String>> porNewsArticles = langStream.select("por");
    DataStream<Tuple2<String, String[]>> porNewsTokenized = porNewsArticles.map(new PorTokenizerMapFunction());

    DataStream<POSSample> porNewsPOS = porNewsTokenized.map(new PorPOSTaggerMapFunction());
    DataStream<NameSample> porNewsNamedEntities = porNewsTokenized.map(new PorNameFinderMapFunction());


    

    engNewsPOS.writeAsText("output.txt", FileSystem.WriteMode.OVERWRITE);

    streamExecutionEnvironment.execute();
  }

  private void initializeModels() throws IOException {
    engSentenceDetector = new SentenceDetectorME(new SentenceModel(
        OpenNLPMain.class.getResource("/opennlp-models/en-sent.bin")));

    engTokenizer = new TokenizerME(new TokenizerModel(
        OpenNLPMain.class.getResource("/opennlp-models/en-token.bin")));

    engPosTagger = new POSTaggerME(new POSModel(
        OpenNLPMain.class.getResource("/opennlp-models/en-pos-maxent.bin")));

    engChunker = new ChunkerME(new ChunkerModel(
        OpenNLPMain.class.getResource("/opennlp-models/en-chunker.bin")));

    engNameFinder = new NameFinderME(new TokenNameFinderModel(
        OpenNLPMain.class.getResource("/opennlp-models/en-ner-person.bin")));

    languageDetectorME = new LanguageDetectorME(new LanguageDetectorModel(
        OpenNLPMain.class.getResource("/opennlp-models/lang-maxent.bin")));

    porSentenceDetector = new SentenceDetectorME(new SentenceModel(
        OpenNLPMain.class.getResource("/opennlp-models/en-sent.bin")));

    porTokenizer = new TokenizerME(new TokenizerModel(
        OpenNLPMain.class.getResource("/opennlp-models/por-token.bin")));

    porPosTagger = new POSTaggerME(new POSModel(
        OpenNLPMain.class.getResource("/opennlp-models/por-pos-maxent.bin")));

    porChunker = new ChunkerME(new ChunkerModel(
        OpenNLPMain.class.getResource("/opennlp-models/por-chunker.bin")));

    porNameFinder = new NameFinderME(new TokenNameFinderModel(
        OpenNLPMain.class.getResource("/opennlp-models/por-ner.bin")));
  }

  private static class LeipzigParser implements MapFunction<String, Tuple2<String, String>> {

    private int id = 0;

    @Override
    public Tuple2<String, String> map(String s) throws Exception {
      return new Tuple2<>(Integer.toString(id++), s.substring(s.indexOf('\t') + 1));
    }
  }

  private static class LanguageSelector implements OutputSelector<Tuple2<String, String>> {

    @Override
    public Iterable<String> select(Tuple2<String, String> s) {
      List<String> list = new ArrayList<>();
      list.add(languageDetectorME.predictLanguage(s.f1).getLang());
      return list;
    }
  }

  private static class PorTokenizerMapFunction implements MapFunction<Tuple2<String, String>, Tuple2<String, String[]>> {
    @Override
    public Tuple2<String, String[]> map(Tuple2<String, String> s) {
      return new Tuple2<>(s.f0, porTokenizer.tokenize(s.f0));
    }
  }

  private static class EngTokenizerMapFunction implements MapFunction<Tuple2<String, String>, Tuple2<String, String[]>> {
    @Override
    public Tuple2<String, String[]> map(Tuple2<String, String> s) {
      return new Tuple2<>(s.f0, engTokenizer.tokenize(s.f0));
    }
  }

  private static class EngPOSTaggerMapFunction implements MapFunction<Tuple2<String, String[]>, POSSample> {
    @Override
    public POSSample map(Tuple2<String, String[]> s) {
      String[] tags = engPosTagger.tag(s.f1);
      return new POSSample(s.f0, s.f1, tags);
    }
  }

  private static class PorPOSTaggerMapFunction implements MapFunction<Tuple2<String, String[]>, POSSample> {
    @Override
    public POSSample map(Tuple2<String, String[]> s) {
      String[] tags = porPosTagger.tag(s.f1);
      return new POSSample(s.f0, s.f1, tags);
    }
  }

  private static class EngNameFinderMapFunction implements MapFunction<Tuple2<String, String[]>, NameSample> {
    @Override
    public NameSample map(Tuple2<String, String[]> s) {
      Span[] names = engNameFinder.find(s.f1);
      return new NameSample(s.f0, s.f1, names, null, true);
    }
  }

  private static class PorNameFinderMapFunction implements MapFunction<Tuple2<String, String[]>, NameSample> {
    @Override
    public NameSample map(Tuple2<String, String[]> s) {
      Span[] names = engNameFinder.find(s.f1);
      return new NameSample(s.f0, s.f1, names, null, true);
    }
  }
}
