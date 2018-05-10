package org.apache.opennlp.flink.examples.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.opennlp.flink.examples.annotation.Annotation;

import opennlp.tools.langdetect.Language;
import opennlp.tools.langdetect.LanguageDetector;
import opennlp.tools.langdetect.LanguageDetectorME;
import opennlp.tools.langdetect.LanguageDetectorModel;

public class LanguageDetectorFunction extends RichMapFunction<Annotation,Annotation> {

  private transient LanguageDetector languageDetector;

  @Override
  public void open(Configuration parameters) throws Exception {
    languageDetector = new LanguageDetectorME(new LanguageDetectorModel(
        LanguageDetectorFunction.class.getResource("/opennlp-models/langdetect-183.bin")));
  }

  @Override
  public Annotation map(Annotation value) throws Exception {
    Language language = languageDetector.predictLanguage(value.getSofa());
    value.setLanguage(language.getLang());
    return value;
  }
}
