package mapreduce.cday01.test03;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Create by GuoJF on 2019/5/22
 */
public class OwnRecordWriter extends RecordWriter<Text, NullWritable> {


    FSDataOutputStream fsDataOutputStream = null;

    public OwnRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException {


        FileSystem fileSystem = FileSystem.get(taskAttemptContext.getConfiguration());


        fsDataOutputStream = fileSystem.create(new Path("E:\\TestData\\test04\\1.txt"));


    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {

        fsDataOutputStream.write(text.getBytes());


    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {


        IOUtils.closeStream(fsDataOutputStream);
    }
}
