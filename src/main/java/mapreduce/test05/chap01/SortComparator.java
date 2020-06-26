package mapreduce.test05.chap01;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Create by GuoJF on 2019/4/4
 */
public class SortComparator extends WritableComparator {
    @Override
    public int compare(WritableComparable a, WritableComparable b) {


        String[] key01 = a.toString().split(" ");
        String[] key02 = b.toString().split(" ");

        System.out.println(a.toString() + " "+b.toString());
        if (Integer.parseInt(key01[0]) == Integer.parseInt(key02[0])) {

            if (Integer.parseInt(key01[1]) > Integer.parseInt(key02[1])) return 1;
            if (Integer.parseInt(key01[1]) == Integer.parseInt(key02[1])) return 0;
            if (Integer.parseInt(key01[1]) < Integer.parseInt(key02[1])) return -1;


        } else {
            if (Integer.parseInt(key01[0]) > Integer.parseInt(key02[0])) return 1;
            if (Integer.parseInt(key01[0]) < Integer.parseInt(key02[0])) return -1;


        }
        return 0;


    }
}
