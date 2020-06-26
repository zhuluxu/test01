package mapreduce.test05.chap01;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Create by GuoJF on 2019/4/4
 */
public class KeyPartitioner extends Partitioner<Text, NullWritable> {
    @Override
    public int getPartition(Text key, NullWritable value, int numReduceTasks) {
        return (key.toString().split(" ")[0].hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}
