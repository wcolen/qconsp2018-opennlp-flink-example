package org.apache.flink.examples.news;

import com.fasterxml.jackson.databind.ObjectMapper;
import opennlp.tools.util.Span;
import org.bigdata.opennlp.Annotation;
import org.bigdata.opennlp.AnnotationFactory;

import java.io.IOException;
import java.io.Serializable;

public class NewsArticleAnnotationFactory extends AnnotationFactory<NewsArticle> implements Serializable {

  private static final String NEWLINE = "\n";
  private static final ObjectMapper mapper = new ObjectMapper();

  @Override
  public Annotation<NewsArticle> createAnnotation(String jsonString) throws IOException{
    NewsArticle newsArticle = mapper.readValue(jsonString, NewsArticle.class);

    StringBuilder sb = new StringBuilder(newsArticle.getHeadline());
    newsArticle.getBody().forEach(paragraph -> sb.append(NEWLINE).append(paragraph));
    Annotation<NewsArticle> annotation = new Annotation<>(newsArticle.getId(), sb.toString(), newsArticle);
    annotation.setHeadline(new Span(0, newsArticle.getHeadline().length()));
    return annotation;
  }

  private NewsArticleAnnotationFactory() {}

  public static AnnotationFactory<NewsArticle> getFactory() {
    return new NewsArticleAnnotationFactory();
  }
}
