package mapreduce.clean01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.UUID;

/*
 * keyin:是map task 读取到数据的key的类型，是一行的起始偏移量      Long类型
 * valuein：是map task读取到的数据的value的类型，是一行的内容的    String类型
 *
 *
 *
 * keyout：是用户的自定义map方法要返回结果keyvalue数据的key类型，在wordcount中，我们需要返回的单词  String类型
 * valueout:是用户自定义map方法要返回的结果keyvalue数据的value类型，我wordcount中，我们需要返回的是证书  Integer
 *
 *
 *
 * 但是在进行mapreduce计算时，需要进行数据传输，需要进行序列化和反序列化，需要使用hadoop内置的序列化接口
 *
 *
 * 在JDK中常用基本数据类型：Long，String，Integer，Float，在Hadoop实现了序列化接口之后实现类是：LongWrite，Text，IntWrite
 *
 * */


public class CleanMapper extends Mapper<LongWritable, Text, Text, IntWritable> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        if (!line.contains("thisisshortvideoproject'slog")) {
            return;
        }

        String allData = line.replaceAll(" thisisshortvideoproject'slog", "");

        String uuid = UUID.randomUUID().toString();


        context.write(new Text(uuid+" "+allData),new IntWritable(1));

    }
}
