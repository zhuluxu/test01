package mapreduce.test03.chap02;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Creat by GuoJF on ${date}
 */
public class FlowSortReducer extends Reducer<FlowBean, NullWritable, FlowBean, NullWritable> {
    @Override
    protected void reduce(FlowBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        context.write(key, NullWritable.get());


    }
}
