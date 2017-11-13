package org.bigdata.opennlp;

import opennlp.tools.tokenize.SimpleTokenizer;

import java.util.Arrays;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ESSinkFunctionTest {


  private static Predicate<String> ALPHANUMSPACE = Pattern.compile("[\\p{L}\\p{Nd}\\s]+").asPredicate();

  public String entityKey(CharSequence mention) {

    String[] mentionTokens = SimpleTokenizer.INSTANCE.tokenize(mention.toString());
    // join and clean non alphanum
    return Arrays.stream(mentionTokens)
            .filter(ALPHANUMSPACE)
            .map(t -> t.toLowerCase())
            .collect(Collectors.joining(" "));

  }

  public static void main(String[] args) {

    ESSinkFunctionTest es = new ESSinkFunctionTest();

    System.out.println(">" + es.entityKey("Aaaaaa Bbbbbb. -")+ "<");



  }
}
