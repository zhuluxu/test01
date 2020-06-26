package mapreduce.test03.chp04;

import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * Creat by GuoJF on ${date}
 */
public class FlowPartitoner<KEY, VALUE> extends Partitioner<KEY, VALUE> {


    private static HashMap<String, Integer> areaMap = new HashMap<String, Integer>();

    static {

        areaMap.put("hn", 0);
        areaMap.put("zz", 1);
        areaMap.put("bj", 2);
        areaMap.put("kf", 3);
        areaMap.put("xy", 4);
    }

    @Override
    public int getPartition(KEY key, VALUE value, int i) {

        return areaMap.get(key.toString().split(" ")[1]) == null ? 5 : areaMap.get(key.toString().split(" ")[1]);
    }
}
