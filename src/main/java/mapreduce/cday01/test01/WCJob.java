package mapreduce.cday01.test01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Create by GuoJF on 2019/5/20
 */
public class WCJob {

    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "root");

        /*1 封装Job对象*/
        Configuration configuration = new Configuration();

        configuration.set("mapreduce.app-submission.cross-platform", "true");

        configuration.addResource("conf1/core-site.xml");
        configuration.addResource("conf1/hdfs-site.xml");
        configuration.addResource("conf1/yarn-site.xml");
        configuration.addResource("conf1/mapred-site.xml");

        configuration.set(MRJobConfig.JAR, "\\Users\\didi\\学习之路\\个人代码\\IDEA_WorkSpace\\BigData\\HadoopTest\\target\\HadoopTest-1.0-SNAPSHOT.jar");


        Job job = Job.getInstance(configuration);

        job.setJarByClass(WCJob.class);


        /*
         *2 设置数据的写入和写出的格式
         * */

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        /*
         * 3 设置数据的写入和写出路径
         * */


        //TextInputFormat.addInputPath(job, new Path("E:\\TestData\\test04\\in"));
        TextInputFormat.addInputPath(job, new Path("/1.log"));
        //TextOutputFormat.setOutputPath(job, new Path("E:\\TestData\\test04\\ou11t100112111231"));
        TextOutputFormat.setOutputPath(job, new Path("/test01/out1"));

        /*
         * 4 设置数据的计算逻辑
         * */

        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReduce.class);

        /*5     设置mapper和reduce的输出泛型
         * */

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        /*
         * 6 提交任务
         * */
//        job.submit();
        job.waitForCompletion(true);

    }
}
