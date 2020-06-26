package mapreduce.test05.chap01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Create by GuoJF on 2019/4/4
 */
public class SecondSortReducer extends Reducer<Text, NullWritable, NullWritable, Text> {


    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {


    }
}
