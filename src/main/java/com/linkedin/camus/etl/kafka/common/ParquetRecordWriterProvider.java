package com.linkedin.camus.etl.kafka.common;

import com.google.protobuf.Message;
import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.proto.ProtoParquetWriter;

import java.io.IOException;

public class ParquetRecordWriterProvider implements RecordWriterProvider {
    public final static String EXT = ".gz";

    public final static String PARQUET_BLOCK_SIZE = "parquet.block.size";
    public final static int PARQUET_DEFAULT_BLOCK_SIZE = 134217728;

    public final static String PARQUET_PAGE_SIZE = "parquet.page.size";
    public final static int PARQUET_DEFAULT_PAGE_SIZE = 1048576;

    public final static String PARQUET_ENABLE_DICTIONARY = "parquet.enable.dictionary";
    public final static boolean PARQUET_DEFAULT_ENABLE_DICTIONARY = true;

    public final static String PARQUET_VALIDATION = "parquet.validation";
    public final static boolean PARQUET_DEFAULT_VALIDATION = false;

    private final int blockSize;
    private final int pageSize;
    private final boolean enableDictionary;
    private final boolean validation;

    public ParquetRecordWriterProvider(TaskAttemptContext context) {
        Configuration conf = context.getConfiguration();

        blockSize = conf.getInt(PARQUET_BLOCK_SIZE, PARQUET_DEFAULT_BLOCK_SIZE);
        pageSize = conf.getInt(PARQUET_PAGE_SIZE, PARQUET_DEFAULT_PAGE_SIZE);
        enableDictionary = conf.getBoolean(PARQUET_ENABLE_DICTIONARY, PARQUET_DEFAULT_ENABLE_DICTIONARY);
        validation = conf.getBoolean(PARQUET_VALIDATION, PARQUET_DEFAULT_VALIDATION);
    }

    @Override
    public String getFilenameExtension() {
        return EXT;
    }

    public RecordWriter<IEtlKey, CamusWrapper> getDataRecordWriter(
            TaskAttemptContext context,
            String fileName,
            CamusWrapper data,
            FileOutputCommitter committer) throws IOException, InterruptedException {

        Path path = new Path(committer.getWorkPath(), EtlMultiOutputFormat.getUniqueFile(context, fileName, EXT));

        final ProtoParquetWriter<Message> writer = new ProtoParquetWriter<>(
                path,
                ((Message) data.getRecord()).getClass(),
                CompressionCodecName.GZIP,
                blockSize,
                pageSize,
                enableDictionary,
                validation);

        return new RecordWriter<IEtlKey, CamusWrapper>() {
            @Override
            public void write(IEtlKey ignore, CamusWrapper data) throws IOException {
                Message msg = (Message) data.getRecord();
                writer.write(msg);
            }

            @Override
            public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
                writer.close();
            }
        };
    }
}
