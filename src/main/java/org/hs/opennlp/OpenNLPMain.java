package org.hs.opennlp;

import opennlp.tools.chunker.ChunkerME;
import opennlp.tools.chunker.ChunkerModel;
import opennlp.tools.langdetect.LanguageDetectorME;
import opennlp.tools.langdetect.LanguageDetectorModel;
import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinder;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTagger;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.Span;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.runtime.kryo.JavaSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OpenNLPMain {

  private static final Logger LOG = LoggerFactory.getLogger(OpenNLPMain.class);

  String modelsDir = "opennlp-models";

  // refac to RichMapFunctions
  private static SentenceDetectorME engSentenceDetector = null;
  private static ChunkerME engChunker = null;
  private static LanguageDetectorME languageDetectorME = null;
  private static SentenceDetectorME porSentenceDetector = null;
  private static ChunkerME porChunker = null;

  public static void main(String[] args) throws Exception {

    OpenNLPMain nlp = new OpenNLPMain();
    nlp.initializeModels();

    // Get an instance of the Streaming Execution Environment
    final StreamExecutionEnvironment streamExecutionEnvironment =
        StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(5);

    streamExecutionEnvironment.getConfig()
        .registerTypeWithKryoSerializer(POSSample.class, JavaSerializer.class);

    streamExecutionEnvironment.getConfig()
        .registerTypeWithKryoSerializer(NameSample.class, JavaSerializer.class);

    //ParameterTool parameterTool = ParameterTool.fromArgs(args); not used

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

    // set1.coGroup(set2).where(<key-definition>).equalTo(<key-definition>).with(new MyCoGroupFunction());

    DataStream<Tuple3<String, POSSample, NameSample>> porNewsEnriched = porNewsPOS.coGroup(porNewsNamedEntities)
            .where(posSample -> posSample.getId())
            .equalTo(nameSample -> nameSample.getId())
            .window(GlobalWindows.create())
            .apply(new CoGroupFunction<POSSample, NameSample, Tuple3<String, POSSample, NameSample>>() {

              @Override
              public void coGroup(Iterable<POSSample> iterable, Iterable<NameSample> iterable1, Collector<Tuple3<String, POSSample, NameSample>> collector) throws Exception {

                POSSample posSample = iterable.iterator().next();
                NameSample nameSample = iterable1.iterator().next();

                collector.collect(new Tuple3<>(posSample.getId(), posSample, nameSample));
              }
            });


    porNewsEnriched.writeAsText("output.txt", FileSystem.WriteMode.OVERWRITE);

    streamExecutionEnvironment.execute();
  }

  // refac use RichMapFunctions.open(...)
  private void initializeModels() throws IOException {
    engSentenceDetector = new SentenceDetectorME(new SentenceModel(
        OpenNLPMain.class.getResource("/opennlp-models/en-sent.bin")));

    engChunker = new ChunkerME(new ChunkerModel(
        OpenNLPMain.class.getResource("/opennlp-models/en-chunker.bin")));

    languageDetectorME = new LanguageDetectorME(new LanguageDetectorModel(
        OpenNLPMain.class.getResource("/opennlp-models/langdetect-183.bin")));

    porChunker = new ChunkerME(new ChunkerModel(
        OpenNLPMain.class.getResource("/opennlp-models/por-chunker.bin")));

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

  private static class PorTokenizerMapFunction extends RichMapFunction<Tuple2<String, String>, Tuple2<String, String[]>> {

    private transient Tokenizer porTokenizer;

    @Override
    public Tuple2<String, String[]> map(Tuple2<String, String> s) {
      return new Tuple2<>(s.f0, porTokenizer.tokenize(s.f1));
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      porTokenizer = new TokenizerME(new TokenizerModel(
              OpenNLPMain.class.getResource("/opennlp-models/por-token.bin")));
    }
  }

  private static class EngTokenizerMapFunction extends RichMapFunction<Tuple2<String, String>, Tuple2<String, String[]>> {
    private transient Tokenizer engTokenizer;

    @Override
    public Tuple2<String, String[]> map(Tuple2<String, String> s) {
      return new Tuple2<>(s.f0, engTokenizer.tokenize(s.f1));
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      engTokenizer = new TokenizerME(new TokenizerModel(
              OpenNLPMain.class.getResource("/opennlp-models/en-token.bin")));
    }
  }

  private static class EngPOSTaggerMapFunction extends RichMapFunction<Tuple2<String, String[]>, POSSample> {

    private transient POSTagger engPosTagger;

    @Override
    public POSSample map(Tuple2<String, String[]> s) {
      String[] tags = engPosTagger.tag(s.f1);
      return new POSSample(s.f0, s.f1, tags);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      engPosTagger = new POSTaggerME(new POSModel(
              OpenNLPMain.class.getResource("/opennlp-models/en-pos-maxent.bin")));
    }
  }

  private static class PorPOSTaggerMapFunction extends RichMapFunction<Tuple2<String, String[]>, POSSample> {

    private transient POSTagger porPosTagger;

    @Override
    public POSSample map(Tuple2<String, String[]> s) {
      String[] tags = porPosTagger.tag(s.f1);
      return new POSSample(s.f0, s.f1, tags);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      porPosTagger = new POSTaggerME(new POSModel(
              OpenNLPMain.class.getResource("/opennlp-models/por-pos-maxent.bin")));
    }
  }

  private static class EngNameFinderMapFunction extends RichMapFunction<Tuple2<String, String[]>, NameSample> {

    private transient TokenNameFinder engNameFinder;

    @Override
    public NameSample map(Tuple2<String, String[]> s) {
      Span[] names = engNameFinder.find(s.f1);
      return new NameSample(s.f0, s.f1, names, null, true);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      engNameFinder = new NameFinderME(new TokenNameFinderModel(
              EngNameFinderMapFunction.class.getResource("/opennlp-models/en-ner-person.bin")));
    }
  }

  private static class PorNameFinderMapFunction extends RichMapFunction<Tuple2<String, String[]>, NameSample> {

    private transient TokenNameFinder porNameFinder;

    @Override
    public NameSample map(Tuple2<String, String[]> s) {
      Span[] names = porNameFinder.find(s.f1);
      return new NameSample(s.f0, s.f1, names, null, true);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      porNameFinder = new NameFinderME(new TokenNameFinderModel(
              OpenNLPMain.class.getResource("/opennlp-models/por-ner.bin")));
    }
  }
}
