package org.bigdata.opennlp;

import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;

import java.util.Collections;
import java.util.Set;

public class LanguageSelector implements OutputSelector<Annotation> {
  public static String OTHER_LANGUAGES = "OTHER_LANGUAGES";

  private final Set<String> supportedLanguages;

  public LanguageSelector(String ... languages) {
    supportedLanguages = Sets.newHashSet(languages);
  }

  @Override
  public Iterable<String> select(Annotation annotation) {
    if (supportedLanguages.contains(annotation.getLanguage()))
      return Collections.singletonList(annotation.getLanguage());
    else
      return Collections.singletonList(OTHER_LANGUAGES);
  }
}
