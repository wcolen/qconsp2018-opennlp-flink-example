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
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;

public class OpenNLPMain {

  private static final Logger LOG = LoggerFactory.getLogger(OpenNLPMain.class);

  String modelsDir = "opennlp-models";

  private static SentenceDetectorME sentenceDetector = null;
  private static TokenizerME tokenizer = null;
  private static POSTaggerME posTagger = null;
  private static ChunkerME chunker = null;
  private static LanguageDetectorME languageDetectorME = null;

  public static void main(String[] args) throws Exception {

    OpenNLPMain nlp = new OpenNLPMain();
    nlp.initializeModels();

    // Get an instance of the Streaming Execution Environment
    final StreamExecutionEnvironment streamExecutionEnvironment =
        StreamExecutionEnvironment.getExecutionEnvironment();

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

    DataStream<String> deuNewsArticles = langStream.select("deu");

    DataStream<String> itaNewsArticles = langStream.select("ita");



//     DataStream<String[]> sentences = engLangStream.map(new SentenceMapFunction());


   // sentences.writeAsText("/Users/smarthi/bbuzz/src/main/resources/sentences.txt", FileSystem.WriteMode.OVERWRITE);


    engNewsArticles.writeAsText("output.txt", FileSystem.WriteMode.OVERWRITE);

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

  private static class SentenceMapFunction implements MapFunction<String, String[]> {
    @Override
    public String[] map(String s) throws Exception {
      return sentenceDetector.sentDetect(s);
    }
  }
}
