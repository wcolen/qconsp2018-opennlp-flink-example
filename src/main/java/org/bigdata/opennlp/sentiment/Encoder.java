package org.bigdata.opennlp.sentiment;

import opennlp.tools.tokenize.SimpleTokenizer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;

public class Encoder {

  private static final Map<String,Integer> wordMap = new HashMap<>();
  private static final Pattern stripSpecialChars = Pattern.compile("^A-Za-z0-9 ]+");
  private static final int UNK = 399999;
  private static final int MAX_TOKEN_LENGTH = 250;
  private static final int ROWS = 24;

  private int[][] getSample() {
    int[][] data = new int[ROWS][MAX_TOKEN_LENGTH];
    for (int b = 0; b < ROWS; b++)
      for (int w = 0; w < MAX_TOKEN_LENGTH; w++)
        data[b][w] = UNK;
    return data;
  }

  private int[][] padWord(Integer[] word, int row, int[][] data) {
    for(int i = 0; i < word.length && i < data.length; i++)
      data[row][i] = word[i];
    return data;
  }

  public Encoder() {
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(Encoder.class.getResourceAsStream("/wordlist"), StandardCharsets.UTF_8));
      String word;
      int i = 0;
      while ((word = reader.readLine()) != null) {
        if (!wordMap.containsKey(word)) {
          wordMap.put(word, i);
          i += 1;
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Cannot load word vector for Encoder.", e);
    }
  }

  public EncodedData encode(List<String> text) {
    EncodedData data = new EncodedData();
    for (int i = 0; i < text.size() && i < ROWS; i++) {
      data.data = encode(SimpleTokenizer.INSTANCE.tokenize(text.get(i)), i, data.data);
      data.rowsUsed += 1;
    }
    return data;
  }

  public int[][] encode(String text) {
    return encode(SimpleTokenizer.INSTANCE.tokenize(text));
  }

  public int[][] encode(String[] tokens) {
    return padWord(indexTokens(tokens), 0, getSample());
  }

  private int[][] encode(String[] tokens, int row, int[][] data) {
    return padWord(indexTokens(tokens), row, data);
  }

  Function<String,Integer> wordToIndex = new Function<String, Integer>() {
    @Override
    public Integer apply(String token) {
      Integer idx = wordMap.get(token);
      return (idx != null) ? idx : UNK;
    }
  };

  private Integer[] indexTokens(String[] tokens) {
    return Arrays.stream(tokens)
            .filter(t -> !stripSpecialChars.matcher(t).matches())
            .map(wordToIndex)
            .limit(MAX_TOKEN_LENGTH)
            .toArray(Integer[]::new);
  }

  public class EncodedData {
    public int[][] data = getSample();
    public int rowsUsed = 0;
  }
}
