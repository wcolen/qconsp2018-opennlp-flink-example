package org.apache.flink.examples.news;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.io.DelimitedInputFormat;
import org.bigdata.opennlp.Annotation;
import org.bigdata.opennlp.AnnotationFactory;

import java.io.IOException;

/**
 * Borrowed from TextInputFormat
 */
public class AnnotationInputFormat<T> extends DelimitedInputFormat<Annotation<T>> {

  private static final long serialVersionUID = 1L;

  /**
   * Code of \r, used to remove \r from a line when the line ends with \r\n
   */
  private static final byte CARRIAGE_RETURN = (byte) '\r';

  /**
   * Code of \n, used to identify if \n is used as delimiter
   */
  private static final byte NEW_LINE = (byte) '\n';


  /**
   * The name of the charset to use for decoding.
   */
  private String charsetName = "UTF-8";
  private final AnnotationFactory<T> factory;

  public AnnotationInputFormat(AnnotationFactory<T> factory) {
    super();
    this.factory = factory;
  }

  @Override
  public Annotation<T> readRecord(Annotation<T> reusable, byte[] bytes, int offset, int numBytes) throws IOException {
    if (this.getDelimiter() != null && this.getDelimiter().length == 1
            && this.getDelimiter()[0] == NEW_LINE && offset+numBytes >= 1
            && bytes[offset+numBytes-1] == CARRIAGE_RETURN){
      numBytes -= 1;
    }
    return factory.createAnnotation(new String(bytes, offset, numBytes, this.charsetName));
  }

}
