package mapreduce.test.MR1;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Runner {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //封装job对象
        Configuration configuration = new Configuration();
        /* configuration.addResource("/resour/core-site.xml");
        configuration.addResource("/resour/hdfs-site.xml");
         configuration.addResource("/resour/mapred-site.xml");
         configuration.addResource("/resour/yarn-site.xmll");
        configuration.set(MRJobConfig.JAR,"F:\\ideaSubject\\BigDatas\\hadoopdemo01\\target\\hadoopdemo01-1.0-SNAPSHOT.jar");
        */

        Job job = Job.getInstance(configuration);


       // System.setProperty("HADOOP_USER_NAME","root");
        //设置数据的输入和写出的格式
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setJarByClass(Runner.class);
        //设置数据处理的路径
        TextInputFormat.addInputPath(job,new Path("D:\\DevelopEnvironment\\IDEA_WorkSpace\\BigData\\HadoopTest\\src\\main\\java\\mapreduce\\test03\\flow.dat"));
        TextOutputFormat.setOutputPath(job,new Path("D:\\DevelopEnvironment\\IDEA_WorkSpace\\BigData\\HadoopTest\\src\\main\\java\\mapreduce\\test06\\out111"));

        //设置处理逻辑的类
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReduce.class);

        //设置mapper的输出泛型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MyBean.class);
        //设置reducer的输出泛型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MyBean.class);
        //等待任务执行完成
        job.waitForCompletion(true);
    }
}
