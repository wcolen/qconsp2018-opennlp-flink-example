package org.apache.opennlp.flink.examples.annotation;

import java.io.Serializable;

public class SimpleArticleAnnotationFactory extends AnnotationFactory implements Serializable {

  @Override
  public Annotation createAnnotation(String line) {
    Annotation annotation = new Annotation();
    annotation.setSofa(line);
    return annotation;
  }

  private SimpleArticleAnnotationFactory() {}

  public static AnnotationFactory getFactory() {
    return new SimpleArticleAnnotationFactory();
  }
}
