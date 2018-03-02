package org.bigdata.opennlp.glove;

import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * Not optimized
 */
public class GloveModel {
  private Map<String, Double[]> vectorMap = Maps.newHashMap();

  public void load(String gloVeModel, int vectorSize) throws IOException {
    try(BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(gloVeModel)), "UTF-8"))) {

      String line;
      while ((line = reader.readLine()) != null) {

        String[] wordvec = line.split("\\s");

        if (wordvec.length != vectorSize+1) {
          System.out.println("Bad vector size: " + line);
          continue; // skip loading the vector
        }

        Double[] vector = new Double[vectorSize];
        for(int j=1; j<wordvec.length; j++) {
          vector[j] = Double.valueOf(wordvec[j]);
        }
        vectorMap.put(wordvec[0], vector);

      }
    }
  }

  /**
   * Get the vector for a given word
   * @param word
   * @return vector or null if word is not in vector map
   */
  public Double[] getVector(String word) {
    return vectorMap.get(word);
  }

}
