package org.bigdata.opennlp.functions;

import opennlp.tools.gender.GenderDetector;
import opennlp.tools.gender.GenderDetectorME;
import opennlp.tools.gender.GenderDetectorModel;
import opennlp.tools.tokenize.SimpleTokenizer;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.bigdata.opennlp.Annotation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.regex.Pattern;

public class GenderDetectorFunction extends RichMapFunction<Annotation,Annotation> {

  private final GenderDetectorModel model;
  private transient GenderDetector genderDetector;

  private static Pattern ALPHANUMSPACE = Pattern.compile("[\\p{L}\\s]+");

  public GenderDetectorFunction(GenderDetectorModel model) {
    this.model = model;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    genderDetector = new GenderDetectorME(model);
  }

  @Override
  public Annotation map(Annotation annotation) throws Exception {

    Collection<String> keys = new ArrayList<>();

    String[][] mentions = annotation.getPersonMentions();
    String[][] gender = new String[annotation.getSentences().length][];

    for (int i = 0; i < annotation.getSentences().length; i++) {
      gender[i] = new String[mentions[i].length];
      for (int j = 0; j < mentions[i].length; j++) {

        String[] tokens = SimpleTokenizer.INSTANCE.tokenize(mentions[i][j]);

        gender[i][j] = genderDetector.genderDetect(tokens);

      }
    }
    annotation.setPersonGenders(gender);
    return annotation;
  }
}
