package mapreduce.test04;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;


/**
 * Create by GuoJF on 2019/4/4
 */
public class JoinReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


        String stuName = null;
        ArrayList<String> classNameList = new ArrayList<>();
        for (Text value : values) {


            String[] split = value.toString().split(" ");
            if (split[1].equals("a")) {
                stuName = split[0];
            } else if (split[1].equals("b")) {

                classNameList.add(split[0]);

            }

        }


        String className = "";

        for (String name : classNameList) {
            className += name + " ";
        }


        context.write(key, new Text(stuName + " " + className));

    }
}
