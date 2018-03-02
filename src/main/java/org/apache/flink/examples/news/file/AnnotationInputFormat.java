package org.apache.flink.examples.news.file;

import org.apache.flink.api.common.io.DelimitedInputFormat;
import org.bigdata.opennlp.Annotation;
import org.bigdata.opennlp.AnnotationFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Borrowed from TextInputFormat
 */
public class AnnotationInputFormat extends DelimitedInputFormat<Annotation> {

  private static final long serialVersionUID = 1L;

  /**
   * Code of \r, used to remove \r from a line when the line ends with \r\n
   */
  private static final byte CARRIAGE_RETURN = (byte) '\r';

  /**
   * Code of \n, used to identify if \n is used as delimiter
   */
  private static final byte NEW_LINE = (byte) '\n';

  private final AnnotationFactory factory;

  public AnnotationInputFormat(AnnotationFactory factory) {
    super();
    this.factory = factory;
  }

  @Override
  public Annotation readRecord(Annotation reusable, byte[] bytes, int offset, int numBytes)
      throws IOException {
    if (this.getDelimiter() != null && this.getDelimiter().length == 1
            && this.getDelimiter()[0] == NEW_LINE && offset+numBytes >= 1
            && bytes[offset+numBytes-1] == CARRIAGE_RETURN){
      numBytes -= 1;
    }
    return factory.createAnnotation(new String(bytes, offset, numBytes, StandardCharsets.UTF_8));
  }

}
