package mapreduce.test.MR1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReduce extends Reducer<Text,MyBean,Text,MyBean> {
    @Override
    protected void reduce(Text key, Iterable<MyBean> values, Context context) throws IOException, InterruptedException {
        long up = 0;
        long down = 0;
        for (MyBean value : values) {
            up+=value.getUp();
            down+=value.getDown();
        }

        context.write(key,new MyBean(key.toString(),up,down,up+down));
    }
}
