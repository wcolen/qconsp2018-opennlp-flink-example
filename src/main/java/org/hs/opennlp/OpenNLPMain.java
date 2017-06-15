package org.hs.opennlp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
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
import opennlp.tools.postag.POSSample;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.Span;

public class OpenNLPMain {

  private static final Logger LOG = LoggerFactory.getLogger(OpenNLPMain.class);

  String modelsDir = "opennlp-models";

  private static SentenceDetectorME sentenceDetector = null;
  private static TokenizerME tokenizer = null;
  private static POSTaggerME posTagger = null;
  private static ChunkerME chunker = null;
  private static LanguageDetectorME languageDetectorME = null;
  private static TokenNameFinder nameFinder = null;

  public static void main(String[] args) throws Exception {

    OpenNLPMain nlp = new OpenNLPMain();
    nlp.initializeModels();

    // Get an instance of the Streaming Execution Environment
    final StreamExecutionEnvironment streamExecutionEnvironment =
        StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

    ParameterTool parameterTool = ParameterTool.fromArgs(args);

    streamExecutionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

    DataStream<String> inputStream =
        streamExecutionEnvironment.readTextFile(
        OpenNLPMain.class.getResource("/input/eng_news_2015_100K-sentences.txt").getFile());

    // read german text
    DataStream<String> germanText =
        streamExecutionEnvironment.readTextFile(
            OpenNLPMain.class.getResource("/input/deu_news_2015_30K-sentences.txt").getFile());


    // read italian text
    DataStream<String> italianText =
        streamExecutionEnvironment.readTextFile(
            OpenNLPMain.class.getResource("/input/ita_news_2010_30K-sentences.txt").getFile());

    // Merge all streams
    DataStream<String> mergedStream = inputStream.union(germanText, italianText);

    // Parse the text
    DataStream<String> newsArticles = mergedStream.map(new LeipzigParser());

    SplitStream<String> langStream = newsArticles.split(new LanguageSelector());

    DataStream<String> engNewsArticles = langStream.select("eng");
    DataStream<String[]> engNewsTokenized = engNewsArticles.map(new TokenizerMapFunction());

    DataStream<POSSample> engNewsPOS = engNewsTokenized.map(new POSTaggerMapFunction());
    DataStream<NameSample> engNewsNamedEntities = engNewsTokenized.map(new NameFinderMapFunction());



    DataStream<String> deuNewsArticles = langStream.select("deu");

    DataStream<String> itaNewsArticles = langStream.select("ita");

    engNewsPOS.writeAsText("output.txt", FileSystem.WriteMode.OVERWRITE);

    streamExecutionEnvironment.execute();
  }

  private void initializeModels() throws IOException {
    sentenceDetector = new SentenceDetectorME(new SentenceModel(
        OpenNLPMain.class.getResource("/opennlp-models/en-sent.bin")));

    tokenizer = new TokenizerME(new TokenizerModel(
        OpenNLPMain.class.getResource("/opennlp-models/en-token.bin")));

    posTagger = new POSTaggerME(new POSModel(
        OpenNLPMain.class.getResource("/opennlp-models/en-pos-maxent.bin")));

    chunker = new ChunkerME(new ChunkerModel(
        OpenNLPMain.class.getResource("/opennlp-models/en-chunker.bin")));

    nameFinder = new NameFinderME(new TokenNameFinderModel(
        OpenNLPMain.class.getResource("/opennlp-models/en-ner-person.bin")));

    languageDetectorME = new LanguageDetectorME(new LanguageDetectorModel(
        OpenNLPMain.class.getResource("/opennlp-models/lang-maxent.bin")));
  }

  private static class LeipzigParser implements MapFunction<String, String> {
    @Override
    public String map(String s) throws Exception {
      return s.substring(s.indexOf('\t') + 1);
    }
  }

  private static class LanguageSelector implements OutputSelector<String> {

    @Override
    public Iterable<String> select(String s) {
      List<String> list = new ArrayList<>();
      list.add(languageDetectorME.predictLanguage(s).getLang());
      return list;
    }
  }

  private static class TokenizerMapFunction implements MapFunction<String, String[]> {

    @Override
    public String[] map(String s) {
      return tokenizer.tokenize(s);
    }
  }

  private static class POSTaggerMapFunction implements MapFunction<String[], POSSample> {

    @Override
    public POSSample map(String[] s) {
      String[] tags = posTagger.tag(s);
      return new POSSample(s, tags);
    }
  }

  private static class NameFinderMapFunction implements MapFunction<String[], NameSample> {

    @Override
    public NameSample map(String[] s) {
      Span[] names = nameFinder.find(s);
      return new NameSample(s, names, true);
    }
  }
}
