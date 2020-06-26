package mapreduce.test03.chp04;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Creat by GuoJF on ${date}
 */
public class FlowSortMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


        String[] values = StringUtils.split(value.toString(), " ");

        String area = values[3];
        String phone = values[0];
        Long up = Long.parseLong(values[1]);
        Long down = Long.parseLong(values[2]);
        context.write(new Text(phone +" "+ area), new FlowBean(phone, up, down, up + down));


    }
}
