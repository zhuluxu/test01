package mapreduce.test03.chap02;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Creat by GuoJF on ${date}
 */
public class FlowSortMapper extends Mapper<LongWritable, Text,FlowBean, NullWritable> {


    //拿到一行数据  切分出各个字段  封装成为一个flowbean

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


        String[] strings = value.toString().split(" ");

        String phone = strings[0];
        long up = Long.parseLong(strings[1]);
        Long down = Long.parseLong(strings[2]);

        context.write(new FlowBean(phone,up,down,up+down),NullWritable.get());


    }
}
