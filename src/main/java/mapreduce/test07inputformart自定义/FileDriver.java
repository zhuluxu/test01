package mapreduce.test07inputformart自定义;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Create by GuoJF on 2019/4/29
 */
public class FileDriver {

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {


        // System.setProperty("HADOOP_USER_NAME", "root");

        Configuration conf = new Configuration();
        //1  设置默认文件系统
        //conf.set("fs.defaultFS", "hdfs://TestVM:9000");


        //2 设置job提交到哪里去运行
        //conf.set("mapreduce.framework.name", "yarn");
        //conf.set("yarn.resourcemanager.hostname", "TestVM");

        //跨平台提交

        //conf.set("mapreduce.app-submission.cross-platform", "true");

        Job job = Job.getInstance(conf);


        job.setInputFormatClass(OwnInputformat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        //job.setJar("D:\\DevelopEnvironment\\IDEA_WorkSpace\\BigData\\HadoopTest\\target\\HadoopTest-1.0-SNAPSHOT.jar");
        job.setJarByClass(FileDriver.class);

        Path src = new Path("D:\\DevelopEnvironment\\IDEA_WorkSpace\\BigData\\HadoopTest\\src\\main\\java\\mapreduce\\test07inputformart自定义\\in");
        TextInputFormat.addInputPath(job, src);
        Path dst = new Path("D:\\DevelopEnvironment\\IDEA_WorkSpace\\BigData\\HadoopTest\\src\\main\\java\\mapreduce\\test07inputformart自定义\\out1");
        TextOutputFormat.setOutputPath(job, dst);


        job.setMapperClass(FileMapper.class);
        job.setReducerClass(FileReduce.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);


        job.waitForCompletion(true);


    }
}
