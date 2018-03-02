package org.bigdata.opennlp.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import opennlp.tools.sentdetect.SentenceDetector;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import org.bigdata.opennlp.Annotation;

public class SentenceDetectorFunction extends RichMapFunction<Annotation,Annotation> {

    private transient SentenceDetector sentenceDetector;
    private final SentenceModel model;

    public SentenceDetectorFunction(final SentenceModel model) {
        this.model = model;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        sentenceDetector = new SentenceDetectorME(model);
    }

    @Override
    public Annotation map(Annotation annotation) throws Exception {
        annotation.setSentences(sentenceDetector.sentPosDetect(annotation.getSofa()));
        // TODO: headline should be it own sentence. if first sentence is longer than headline then split first span into 2 spans
        return annotation;
    }
}
