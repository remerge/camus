package com.linkedin.camus.etl.kafka.common;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;
import events.protos.TestMessage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.proto.ProtoReadSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class ParquetRecordWriterTest {
    private static final String filename = "parquet_test";

    private ParquetRecordWriterProvider writer;
    private TaskAttemptContext taskCtx;
    private FileOutputCommitter committer;
    private Path committerPath;

    @Before
    public void beforeTest() throws IOException {
        Random rand = new Random();

        Configuration conf = new Configuration();
        TaskAttemptID taskId = new TaskAttemptID(
                "testTask",
                rand.nextInt(),
                TaskType.MAP,
                rand.nextInt(),
                rand.nextInt());

        taskCtx = new TaskAttemptContextImpl(conf, taskId);
        writer = new ParquetRecordWriterProvider(taskCtx);

        String tmpDir = System.getProperty("java.io.tmpdir");
        committer = new FileOutputCommitter(new Path(tmpDir), taskCtx);
        committerPath = new Path(committer.getWorkPath(), EtlMultiOutputFormat.getUniqueFile(taskCtx, filename, ParquetRecordWriterProvider.EXT));
    }

    @After
    public void afterTest() throws IOException {
        Files.deleteIfExists(Paths.get(committerPath.toString()));
    }

    @Test
    public void testGetFilenameExt() {
        ParquetRecordWriterProvider writer = new ParquetRecordWriterProvider(taskCtx);
        assertEquals(ParquetRecordWriterProvider.EXT, writer.getFilenameExtension());
    }

    @Test
    public void testWriteBasicTypes() throws IOException, InterruptedException {
        final int numberOfMessages = 10;

        TestMessage.BasicTypes expectedMsg = TestMessage.BasicTypes.newBuilder()
                .setIntValue(1)
                .setStringValue("test")
                .setFloatValue(1.0f)
                .build();


        CamusWrapper<TestMessage.BasicTypes> data = new CamusWrapper<>(expectedMsg);
        RecordWriter<IEtlKey, CamusWrapper> recordWriter = writer.getDataRecordWriter(taskCtx, filename, data, committer);

        for (int i = 0; i < numberOfMessages; i++) {
            recordWriter.write(new EtlKey(), data);
        }

        recordWriter.close(taskCtx);

        ParquetReader<TestMessage.BasicTypes> reader = ParquetReader
                .builder(new ProtoReadSupport<TestMessage.BasicTypes>(), committerPath)
                .build();

        for (int i = 0; i < numberOfMessages; i++) {
            TestMessage.BasicTypesOrBuilder msg = reader.read();

            assertEquals(expectedMsg.getIntValue(), msg.getIntValue());
            assertEquals(expectedMsg.getStringValue(), msg.getStringValue());
            assertEquals(expectedMsg.getFloatValue(), msg.getFloatValue(), 0.0001);
        }
    }

    @Test
    public void testWriteComplexTypes() throws IOException, InterruptedException {
        TestMessage.BasicTypes basicMsg = TestMessage.BasicTypes.newBuilder()
                .setIntValue(1)
                .setStringValue("test")
                .setFloatValue(1.0f)
                .build();

        TestMessage.ComplexTypesWithTimestamp expectedMsg = TestMessage.ComplexTypesWithTimestamp.newBuilder()
                .setBasic(basicMsg)
                .addRepeatedString("0")
                .addRepeatedString("1")
                .setReason(TestMessage.Reason.Invalid)
                .build();

        CamusWrapper<TestMessage.ComplexTypesWithTimestamp> data = new CamusWrapper<>(expectedMsg);
        RecordWriter<IEtlKey, CamusWrapper> recordWriter = writer.getDataRecordWriter(taskCtx, filename, data, committer);

        recordWriter.write(new EtlKey(), data);
        recordWriter.close(taskCtx);

        ParquetReader<TestMessage.ComplexTypesWithTimestamp> reader = ParquetReader
                .builder(new ProtoReadSupport<TestMessage.ComplexTypesWithTimestamp>(), committerPath)
                .build();

        TestMessage.ComplexTypesWithTimestampOrBuilder msg = reader.read();

        assertEquals(expectedMsg.getBasic(), msg.getBasic());
        assertEquals(expectedMsg.getRepeatedStringList(), msg.getRepeatedStringList());
        assertEquals(expectedMsg.getReason(), msg.getReason());
        assertEquals(0, msg.getRepeatedBasicWithTimestampCount());
    }
}
