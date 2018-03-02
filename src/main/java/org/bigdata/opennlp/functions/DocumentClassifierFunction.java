package org.bigdata.opennlp.functions;

import opennlp.tools.doccat.DoccatModel;
import opennlp.tools.doccat.DocumentCategorizerME;
import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.util.Span;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.bigdata.opennlp.Annotation;

import java.util.Arrays;

public class DocumentClassifierFunction extends RichMapFunction<Annotation,Annotation> {

  private transient DocumentCategorizerME categorizer;
  private final DoccatModel model;

  public DocumentClassifierFunction(DoccatModel model) {
    this.model = model;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    categorizer = new DocumentCategorizerME(model);
  }

  @Override
  public Annotation map(Annotation annotation) throws Exception {
    //String[] tokens = Arrays.stream(annotation.getTokens())
    // .flatMap(s -> Arrays.stream(Span.spansToStrings(s, annotation.getSofa())))
    // .toArray(String[]::new);
    String[] tokens = SimpleTokenizer.INSTANCE.tokenize(annotation.getSofa());
    String topic = categorizer.getBestCategory(categorizer.categorize(tokens));
    annotation.setTopic(topic);

    return annotation;
  }
}

