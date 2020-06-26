package mapreduce.test04;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Create by GuoJF on 2019/4/4
 */
public class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {

    public static final String stu_info = "student_info.txt";
    public static final String stu_info_class = "student_info_class.txt";
    public static final String stu_info_flag = "a";
    public static final String stu_info_class_flag = "b";

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String path = ((FileSplit) context.getInputSplit()).getPath().toString();

        //System.out.println(path);
        String mapKey = null;
        String mapValue = null;
        String fileFlag = null;

        String[] split = value.toString().split(" ");




        //判断数据来自哪个文件

        if (path.contains(stu_info)) {
            fileFlag = stu_info_flag;


            mapKey = split[1];
            mapValue = split[0];


        } else if (path.contains(stu_info_class)) {
            fileFlag = stu_info_class_flag;


            mapKey = split[0];
            mapValue = split[1];

        }


        context.write(new Text(mapKey), new Text(mapValue + " " + fileFlag));


    }
}
