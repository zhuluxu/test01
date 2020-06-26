package mapreduce.cday01.test02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Create by GuoJF on 2019/5/20
 */
public class WCJob {

    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "root");

        /*1 封装Job对象*/
        Configuration configuration = new Configuration();

       /* configuration.set("mapreduce.app-submission.cross-platform", "true");

        configuration.addResource("conf/core-site.xml");
        configuration.addResource("conf/hdfs-site.xml");
        configuration.addResource("conf/yarn-site.xml");
        configuration.addResource("conf/mapred-site.xml");

        configuration.set(MRJobConfig.JAR,"D:\\DevelopEnvironment\\IDEA_WorkSpace\\BigData\\HadoopTest\\target\\HadoopTest-1.0-SNAPSHOT.jar");
*/

        Job job = Job.getInstance(configuration);

        job.setJarByClass(WCJob.class);


        /*
         *2 设置数据的写入和写出的格式
         * */

        job.setInputFormatClass(NLineInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);



        /*
         * 3 设置数据的写入和写出路径
         * */

        NLineInputFormat.setNumLinesPerSplit(job,3);


        NLineInputFormat.addInputPath(job, new Path("E:\\TestData\\test04\\in1"));
        //NLineInputFormat.addInputPath(job, new Path("/test01/in"));
        TextOutputFormat.setOutputPath(job, new Path("E:\\TestData\\test04\\ou111t10011111231"));
        //TextOutputFormat.setOutputPath(job, new Path("/test01/out11sssd111"));

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
        //job.submit();
        job.waitForCompletion(true);

    }
}
