package mapreduce.test02.uv;

import mapreduce.test01.WCTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Create by GuoJF on 2019/4/3
 */
public class Runner {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);



        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputValueClass(TextOutputFormat.class);

        job.setJarByClass(Runner.class);



        Path src = new Path(args[0]);
        TextInputFormat.addInputPath(job,src);
        Path dst = new Path(args[1]);
        TextOutputFormat.setOutputPath(job,dst);


        job.setMapperClass(UVMap.class);
        job.setReducerClass(UVReduce.class);



        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);


        job.waitForCompletion(true);

    }
}
