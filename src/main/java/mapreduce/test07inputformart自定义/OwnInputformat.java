package mapreduce.test07inputformart自定义;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Create by GuoJF on 2019/4/29
 */
public class OwnInputformat extends FileInputFormat<Text, BytesWritable> {


    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        OwnRecordReader ownRecordReader = new OwnRecordReader();

        ownRecordReader.initialize(inputSplit, taskAttemptContext);
        return ownRecordReader;
    }
}
