package mapreduce.test01;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Iterator;

/**
 * Create by GuoJF on 2019/4/3
 */
public class WCTest {

    public static class WCMap extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] values = value.toString().split(" ");


            for (String s : values) {

                context.write(new Text(s), new IntWritable(1));
            }


        }
    }


    public static class WCReduce extends Reducer<Text, IntWritable, Text, IntWritable> {


        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {


            int total = 0;

            for (IntWritable value : values) {

                total += value.get();

            }

            context.write(key, new IntWritable(total));
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);


        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputValueClass(TextOutputFormat.class);

        job.setJarByClass(WCTest.class);
        job.setCombinerClass(WCReduce.class);


        Path src = new Path("file:///E:/TestData/test01/in");
        TextInputFormat.addInputPath(job, src);
        Path dst = new Path("file:///E:/TestData/test01/out5");
        TextOutputFormat.setOutputPath(job, dst);


        job.setMapperClass(WCMap.class);
        job.setReducerClass(WCReduce.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        job.waitForCompletion(true);


    }


}
