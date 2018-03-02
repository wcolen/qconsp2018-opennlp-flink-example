package org.bigdata.opennlp;

import opennlp.tools.util.StringUtil;
import opennlp.tools.util.wordvector.Glove;
import opennlp.tools.util.wordvector.VectorMath;
import opennlp.tools.util.wordvector.WordVector;
import opennlp.tools.util.wordvector.WordVectorTable;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

public class VectorTest {

  public static void main(String[] args) throws IOException {

    WordVectorTable vtable = Glove.parse(
            new GZIPInputStream(
                    new FileInputStream("/Users/thygesen/Projects/flink/DC-FlinkMeetup/src/main/resources/glove.6B.50d.txt.gz")));


    List<String> boys = loadNames(Paths.get("/Users/thygesen/Projects/flink/DC-FlinkMeetup/src/main/resources/census-dist-male-first.txt"));
    List<String> girls = loadNames(Paths.get("/Users/thygesen/Projects/flink/DC-FlinkMeetup/src/main/resources/census-dist-female-first.txt"));

    WordVector woman = vtable.get("woman");
    WordVector man = vtable.get("man");

    WordVector gender = VectorMath.substract(woman, man);

    System.out.println("boys:");

    int boycount = 0;
    int boypred = 0;
    int boyunknown = 0;
    for (String name : boys) {
      boycount++;
      WordVector candidate = vtable.get(name);
      if (candidate != null) {
        double pred = VectorMath.cosineSimilarity(candidate, gender);
        System.out.println(name + ": " + pred);
        if (pred < 0.0)
          boypred++;
      } else
        boyunknown++;
    }

    System.out.println("girls:");
    int girlcount = 0;
    int girlpred = 0;
    int girlunknown = 0;
    for (String name : girls) {
      girlcount++;
      WordVector candidate = vtable.get(name);
      if (candidate != null) {
        double pred = VectorMath.cosineSimilarity(candidate, gender);
        System.out.println(name + ": " + pred);
        if (pred > 0.0)
          girlpred++;
      } else
        girlunknown++;
    }

    System.out.println();
    System.out.println("boys: " + boycount);
    double pct = boypred/(double)boycount;
    System.out.println(String.format("%6.5f", pct));
    System.out.println();
    System.out.println("girls: " + girlcount);
    pct = girlpred/(double)girlcount;
    System.out.println(String.format("%6.5f", pct));
    System.out.println();
    System.out.println("unknown boys: " + boyunknown);
    System.out.println("unknown girls: " + girlunknown);
  }

  private static List<String> loadNames(Path path) throws IOException {
    return Files.readAllLines(path).stream().map(l -> l.split("\\s+")[0]).map(StringUtil::toLowerCase).collect(Collectors.toList());
  }

}
