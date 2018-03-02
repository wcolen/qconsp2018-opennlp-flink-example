package opennlp.tools.util.wordvector;

public class VectorMath {

  /**
   * Subtract two vectors
   * @param vector1
   * @param vector2
   * @return new vector
   */
  public static WordVector substract(WordVector vector1, WordVector vector2) {
    double[] vector = new double[vector1.dimension()];
    for(int i=0; i<vector1.dimension(); i++) {
      vector[i] = vector1.getAsDouble(i) - vector2.getAsDouble(i);
    }
    return new DoubleArrayVector(vector);
  }

  /**
   * Calculate Cosine Similarity between two vectors
   * @param vector1
   * @param vector2
   * @return similarity as a double
   */
  public static double cosineSimilarity(WordVector vector1, WordVector vector2) {

    if (vector1.dimension() != vector2.dimension())
      throw new IllegalArgumentException("vector must have same size!");

    double dotProduct = 0.0;
    double magnitudeVector1 = 0.0;
    double magnitudeVector2 = 0.0;

    // summation
    for(int i=0; i<vector1.dimension(); i++) {
      dotProduct += vector1.getAsDouble(i) * vector2.getAsDouble(i);
      magnitudeVector1 += Math.pow(vector1.getAsDouble(i), 2);
      magnitudeVector2 += Math.pow(vector2.getAsDouble(i), 2);
    }


    if (magnitudeVector1 != 0.0 && magnitudeVector2 != 0.0)
      return dotProduct / (Math.sqrt(magnitudeVector1) * Math.sqrt(magnitudeVector2));
    else
      return 0.0D;
  }

}
