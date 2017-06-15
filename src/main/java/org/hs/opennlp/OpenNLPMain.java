package org.hs.opennlp;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import opennlp.tools.chunker.ChunkerME;
import opennlp.tools.chunker.ChunkerModel;
import opennlp.tools.langdetect.Language;
import opennlp.tools.langdetect.LanguageDetectorME;
import opennlp.tools.langdetect.LanguageDetectorModel;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;

/**
 * Created by smarthi on 6/2/17.
 */
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
        streamExecutionEnvironment.readTextFile(OpenNLPMain.class.getResource("/input.txt").getFile());

    DataStream<String> textStream = inputStream.filter(new MyFilter());

    SplitStream<String> langStream = textStream.split(new LanguageSelector());

    DataStream<String> engLangStream = langStream.select("eng");
//    DataStream<String> germanStream = langStream.select("deu");

    DataStream<String[]> sentences = engLangStream.map(new SentenceMapFunction());

    sentences.writeAsText("/Users/smarthi/bbuzz/src/main/resources/sentences.txt", FileSystem.WriteMode.OVERWRITE);


    textStream.writeAsText("/Users/smarthi/bbuzz/src/main/resources/output.txt", FileSystem.WriteMode.OVERWRITE);

    streamExecutionEnvironment.execute();

  }

  private void initializeModels() throws IOException {
    try (InputStream sentenceDetectorModel = OpenNLPMain.class.getResourceAsStream("/opennlp-models/en-sent.bin")) {
      sentenceDetector = new SentenceDetectorME(new SentenceModel(sentenceDetectorModel));
    }

    try (InputStream tokenizerModel = OpenNLPMain.class.getResourceAsStream("/opennlp-models/en-token.bin")) {
      tokenizer = new TokenizerME(new TokenizerModel(tokenizerModel));
    }

    try (InputStream posModel = OpenNLPMain.class.getResourceAsStream("/opennlp-models/en-pos-maxent.bin")) {
      posTagger = new POSTaggerME(new POSModel(posModel));
    }

    try (InputStream chunkerModel = OpenNLPMain.class.getResourceAsStream("/opennlp-models/en-chunker.bin")) {
      chunker = new ChunkerME(new ChunkerModel(chunkerModel));
    }

    try (InputStream languageModel = OpenNLPMain.class.getResourceAsStream("/opennlp-models/lang-maxent.bin")) {
      languageDetectorME = new LanguageDetectorME(new LanguageDetectorModel(languageModel));
    }
  }

  private static class MyFilter implements FilterFunction<String> {
    @Override
    public boolean filter(String s) throws Exception {
      return s.trim().length() > 0 && !(s.startsWith("=") || !Character.isAlphabetic(s.charAt(0)));
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
