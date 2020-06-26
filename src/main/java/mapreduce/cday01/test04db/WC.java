package mapreduce.cday01.test04db;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


import java.io.IOException;

/**
 * Create by GuoJF on 2019/5/22
 */
public class WC {

    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strings = value.toString().split(" ");
            for (String string : strings) {

                context.write(new Text(string), new IntWritable(1));

            }


        }
    }

    public static class MyReducer extends Reducer<Text, IntWritable, MyDBWriter, NullWritable> {


        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int total = 0;

            for (IntWritable value : values) {

                total += value.get();


            }


            MyDBWriter myDBWriter = new MyDBWriter();

            myDBWriter.setWord(key.toString());
            myDBWriter.setCout(total);
            context.write(myDBWriter, NullWritable.get());

        }
    }


    public static void main(String[] args) throws Exception {


        System.setProperty("HADOOP_USER_NAME", "root");

        /*1 封装Job对象*/
        Configuration configuration = new Configuration();

       /* configuration.set("mapreduce.app-submission.cross-platform", "true");

        configuration.addResource("conf/core-site.xml");
        configuration.addResource("conf/hdfs-site.xml");
        configuration.addResource("conf/yarn-site.xml");
        configuration.addResource("conf/mapred-site.xml");

        configuration.set(MRJobConfig.JAR, "file:///D:\\DevelopEnvironment\\IDEA_WorkSpace\\BigData\\HadoopTest\\target\\HadoopTest-1.0-SNAPSHOT.jar");
*/


        configuration.set("tmpjars", "file:///D:\\DevelopEnvironment\\IDEA_WorkSpace\\BigData\\HadoopTest\\src\\main\\java\\mapreduce\\class01\\test04db\\mysql-connector-java-5.1.6.jar");
        configuration.set(DBConfiguration.DRIVER_CLASS_PROPERTY, "com.mysql.jdbc.Driver");
        configuration.set(DBConfiguration.URL_PROPERTY, "jdbc:mysql://localhost:3306/test");
        configuration.set(DBConfiguration.USERNAME_PROPERTY, "root");
        configuration.set(DBConfiguration.PASSWORD_PROPERTY, "ym1211.");


        Job job = Job.getInstance(configuration);

        job.setJarByClass(WC.class);


        /*
         *2 设置数据的写入和写出的格式
         * */

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(DBOutputFormat.class);



        /*
         * 3 设置数据的写入和写出路径
         * */

        //NLineInputFormat.setNumLinesPerSplit(job,3);


        TextInputFormat.addInputPath(job, new Path("E:\\TestData\\test04\\in1"));
        //TextInputFormat.addInputPath(job, new Path("/test01/in"));
        DBOutputFormat.setOutput(job, "testbaizhi", "ip", "pcount");//TextOutputFormat.setOutputPath(job, new Path("/test01/out11sssd111"));

        /*
         * 4 设置数据的计算逻辑
         * */

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        /*5     设置mapper和reduce的输出泛型
         * */

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);


        job.setOutputKeyClass(DBWritable.class);
        job.setOutputValueClass(NullWritable.class);


        /*
         * 6 提交任务
         * */
        //job.submit();
        job.waitForCompletion(true);


    }

}


