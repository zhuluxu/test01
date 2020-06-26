package mapreduce.test03.cha01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Create by GuoJF on 2019/4/3
 */
public class Runner {
    public static void main(String[] args) throws Exception {



        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);



        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputValueClass(TextOutputFormat.class);

        job.setJarByClass(Runner.class);



        Path src = new Path("D:\\DevelopEnvironment\\IDEA_WorkSpace\\BigData\\HadoopTest\\src\\main\\java\\mapreduce\\test03\\flow.dat");
        TextInputFormat.addInputPath(job,src);
        Path dst = new Path("D:\\DevelopEnvironment\\IDEA_WorkSpace\\BigData\\HadoopTest\\src\\main\\java\\mapreduce\\test03\\cha01\\out1");
        TextOutputFormat.setOutputPath(job,dst);


        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReduce.class);



        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);


        job.waitForCompletion(true);

    }
}
