package org.bigdata.opennlp;

import java.io.IOException;

public abstract class AnnotationFactory<T> {
  public abstract Annotation<T> createAnnotation(String source) throws IOException;
}
