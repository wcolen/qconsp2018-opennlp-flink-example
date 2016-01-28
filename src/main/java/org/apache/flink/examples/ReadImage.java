package org.apache.flink.examples;

import org.apache.flink.api.common.io.BinaryInputFormat;
import org.apache.flink.api.common.io.BinaryOutputFormat;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Record;

import java.io.File;
import java.io.IOException;

public class ReadImage {

  public static void main(String[] args) throws Exception {

    ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
    executionEnvironment.setParallelism(1);

    final BinaryInputFormat<Record> bif = new ImageBinaryInputFormat();
    final Configuration config = new Configuration();
    config.setLong(BinaryInputFormat.BLOCK_SIZE_PARAMETER_KEY, bif.createBlockInfo().getInfoSize());
    bif.setFilePath(new Path(ReadImage.class.getResource("/a.bmp").getFile()));
    bif.configure(config);

    DataSet<Record> in = executionEnvironment.readFile(bif, ReadImage.class.getResource("/a.bmp").getFile());
    System.out.println(in.count());

//    FileInputSplit[] inputSplits = bif.createInputSplits(1);
//
//    System.out.println(inputSplits.length + " " + inputSplits[0].getPath());

    final File tempFile = File.createTempFile("temp_", "", new File("/Users/smarthi/"));

    BinaryOutputFormat<Record> bof = new ImageBinaryOutputFormat();

    bof.setOutputDirectoryMode(FileOutputFormat.OutputDirectoryMode.ALWAYS);
    in.write(bof, tempFile.getPath(), FileSystem.WriteMode.OVERWRITE);

  }

  private static class ImageBinaryInputFormat extends BinaryInputFormat<Record> {

    @Override
    protected Record deserialize(Record record, DataInputView dataInputView) throws IOException {
//      record.read(dataInputView);
      return record;
    }
  }

  private static class ImageBinaryOutputFormat extends BinaryOutputFormat<Record> {

    @Override
    protected void serialize(Record record, DataOutputView dataOutputView) throws IOException {
      record.write(dataOutputView);
    }
  }
}
