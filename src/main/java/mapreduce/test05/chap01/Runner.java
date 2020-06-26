package mapreduce.test05.chap01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Create by GuoJF on 2019/4/4
 */
public class Runner {
    public static void main(String[] args) throws Exception {


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




        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputValueClass(TextOutputFormat.class);

        //job.setJar("D:\\DevelopEnvironment\\IDEA_WorkSpace\\BigData\\HadoopTest\\target\\HadoopTest-1.0-SNAPSHOT.jar");
        job.setJarByClass(Runner.class);

        Path src = new Path("D:\\DevelopEnvironment\\IDEA_WorkSpace\\BigData\\HadoopTest\\src\\main\\java\\mapreduce\\test05\\in");
        TextInputFormat.addInputPath(job, src);
        Path dst = new Path("D:\\DevelopEnvironment\\IDEA_WorkSpace\\BigData\\HadoopTest\\src\\main\\java\\mapreduce\\test05\\out2");
        TextOutputFormat.setOutputPath(job, dst);

        job.setPartitionerClass(KeyPartitioner.class);
        job.setSortComparatorClass(SortComparator.class);
        job.setGroupingComparatorClass(GroupComparator.class);
        job.setMapperClass(SecondSortMapper.class);
        job.setReducerClass(SecondSortReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);


        job.waitForCompletion(true);
    }
}
