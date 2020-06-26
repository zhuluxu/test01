package mapreduce.test.MR1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text,Text,MyBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strings = value.toString().split(" ");

        Long up = Long.parseLong(strings[1]);
        Long down = Long.parseLong(strings[2]);
        Long sum = up+down;

        context.write(new Text(strings[0]),new MyBean(strings[0],up,down,sum));
    }
}
