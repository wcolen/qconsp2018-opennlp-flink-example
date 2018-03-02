package org.bigdata.opennlp.functions;

import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.util.Span;
import org.bigdata.opennlp.NewsPipeline;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class NameFinderFunctionTest {

  private static NameFinderFunction nff;

  @BeforeClass
  public static void beforeClass() {
    try {
      nff = new NameFinderFunction(new TokenNameFinderModel(NewsPipeline.class.getResource("/opennlp-models/en-ner.bin")));
    } catch (Exception e) {
      System.out.println("Load failure");
    }
    Assume.assumeNotNull(nff);
  }

  @Test
  public void testStopword() {
    String name = "aaaaa bbbbb ccccc Promoted ddddd";

    String expected = "bbbbb ccccc";
    String actual = nff.getCleanPersonEntity(name.split("\\s"), new Span(1,4));

    Assert.assertEquals(expected, actual);
  }
}
