package mapreduce.test03.chp04;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Creat by GuoJF on ${date}
 */
public class FlowSortReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long up = 0;
        long down = 0;


        for (FlowBean flowBean : values) {

            up += flowBean.getUp();
            down += flowBean.getDown();

        }

        context.write(key, new FlowBean(key.toString(), up, down, up + down));


    }
}
