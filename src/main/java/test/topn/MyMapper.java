package test.topn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text,MyIntWritable,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split("\t");

        MyIntWritable k2 = new MyIntWritable(Integer.parseInt(words[1]));
        Text v2 = new Text(words[0]);

        context.write(k2,v2);
    }
}
