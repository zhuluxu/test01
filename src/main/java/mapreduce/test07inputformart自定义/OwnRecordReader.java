package mapreduce.test07inputformart自定义;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


import java.io.IOException;

/**
 * Create by GuoJF on 2019/4/29
 */
public class OwnRecordReader extends RecordReader<Text, BytesWritable> {

    FileSplit fileSplit;
    Configuration configuration;
    Text key = new Text();
    BytesWritable value = new BytesWritable();

    boolean isProgess = true;


    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        //初始化

        this.fileSplit = (FileSplit) inputSplit;

        configuration = taskAttemptContext.getConfiguration();


    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if (isProgess) {


            byte[] bytes = new byte[(int) fileSplit.getLength()];

            //获取fs对象
            Path path = fileSplit.getPath();
            FileSystem fileSystem = path.getFileSystem(configuration);


            //获取输入流
            FSDataInputStream fsDataInputStream = fileSystem.open(path);

            //直接拷贝
            IOUtils.readFully(fsDataInputStream, bytes, 0, bytes.length);


            //封装Value

            value.set(bytes, 0, bytes.length);


            //封装key

            key.set(path.toString());


            //关闭资源


            IOUtils.closeStream(fsDataInputStream);

            isProgess = false;

            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {

        return key;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
