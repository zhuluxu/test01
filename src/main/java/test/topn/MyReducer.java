package test.topn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<MyIntWritable, Text, Text, LongWritable> {
    private Integer n = 0;
    @Override
    protected void reduce(MyIntWritable k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {
              if(n<3){
                  Text k3  = null;
                  for (Text v2 : v2s) {
                      k3 = v2;
                  }
                  LongWritable v3 = new LongWritable(k2.getId());
                  n++;
                  context.write(k3,v3);
              }
    }
}
