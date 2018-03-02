package org.bigdata.opennlp.sentiment;

import org.tensorflow.SavedModelBundle;
import org.tensorflow.Session;
import org.tensorflow.Tensor;

import java.util.List;

public class SentimentDetectorTF implements SentimentDetector {

  private SavedModelBundle model;
  private Session session;
  private final Encoder encoder;

  public SentimentDetectorTF(String modelPath) {
    try {
      this.encoder = new Encoder();
      this.model = SavedModelBundle.load(modelPath, "serve");
      this.session = model.session();
    } catch (Exception e) {
      throw new RuntimeException("Could not load encoder or model for sentiment detector.");
    }
  }

  @Override
  public int[] sentimentDetect(List<String> sentences) {
    Encoder.EncodedData data = encoder.encode(sentences);
    float[][] sentimentVector =  sentimentDetect(data.data);

    int sum = 0;
    for (int i = 0; i < data.rowsUsed; i++) {
      if (sentimentVector[i][0] > sentimentVector[i][1])
        sum += 1;
      else
        sum -= 1;
    }
    int sentiment = (sentimentVector[0][0] > sentimentVector[0][1]) ? POSITIVE : NEGATIVE;
    int sentimentSum = (sum >= 0) ? POSITIVE : NEGATIVE;
    return new int[] {sentiment, sentimentSum};
  }

  @Override
  public int sentimentDetect(String text) {
    float[][] sentiment =  sentimentDetect(encoder.encode(text));
    return (sentiment[0][0] > sentiment[0][1]) ? POSITIVE : NEGATIVE;
  }

  @Override
  public int sentimentDetect(String[] tokens) {
    float[][] sentiment = sentimentDetect(encoder.encode(tokens));
    return (sentiment[0][0] > sentiment[0][1]) ? POSITIVE : NEGATIVE;
  }

  private float[][] sentimentDetect(int[][] inputData) {
    Tensor input_data = Tensor.create(inputData);
    float[][] sentiment = session.runner()
            .feed("Input_Data", input_data)
            .fetch("Prediction", 0)
            .run()
            .get(0).copyTo(new float[24][2]);

    return sentiment;
  }

  @Override
  public void close() {
    if (session != null)
      session.close();
    if (model != null)
      model.close();
  }
}
