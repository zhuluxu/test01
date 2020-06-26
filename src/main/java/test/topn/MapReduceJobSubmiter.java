package test.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MapReduceJobSubmiter {
    public static void main(String[] args) throws Exception{
        String inpath = "/src/data3";
        String outpath = "/dest";

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, MapReduceJobSubmiter.class.getName());
        //作业以jar包形式  运行
        job.setJarByClass(MapReduceJobSubmiter.class);

        // InputFormat
        Path path = new Path(inpath);
        TextInputFormat.addInputPath(job,path);

        //Map
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(MyIntWritable.class);
        job.setMapOutputValueClass(Text.class);

        //shuffle 默认的方式处理 无需设置

        //reduce
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //输出目录一定不能存在，由MR动态创建
        Path out = new Path(outpath);
        FileSystem fileSystem = FileSystem.get(conf);
        fileSystem.delete(out,true);
        TextOutputFormat.setOutputPath(job,out);
        //运行job作业
        job.waitForCompletion(true);
    }
}
