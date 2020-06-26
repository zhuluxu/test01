package mapreduce.clean01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
    public static void main(String[] args) throws Exception {


        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration conf = new Configuration();


        conf.set("mapreduce.app-submission.cross-platform", "true");

        conf.addResource("conf/core-site.xml");
        conf.addResource("conf/hdfs-site.xml");
        conf.addResource("conf/yarn-site.xml");
        conf.addResource("conf/mapred-site.xml");

        conf.set(MRJobConfig.JAR, "D:\\DevelopEnvironment\\IDEA_WorkSpace\\BigData\\HadoopTest\\target\\HadoopTest-1.0-SNAPSHOT.jar");


        Job job = Job.getInstance(conf);


        //2 封装参数  本次job所要调用的Mapper实现类，Reducer实现类


        job.setMapperClass(CleanMapper.class);

        job.setReducerClass(CleanReducer.class);


        //封装参数


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);


        FileInputFormat.setInputPaths(job, new Path("/access.log"));
        FileOutputFormat.setOutputPath(job, new Path("/clean01"));


        //想要启动的reduce task数量


        job.setNumReduceTasks(1);

        //6 提交

        boolean res = job.waitForCompletion(true);

        System.exit(res ? 0 : -1);


    }


}




