package mapreduce.test05.chap01;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Create by GuoJF on 2019/4/4
 */
public class GroupComparator extends WritableComparator {
    protected GroupComparator(){
        super(Text.class,true);
    }
    @Override
    public int compare(WritableComparable w1, WritableComparable b) {
        if(Integer.parseInt(w1.toString().split(" ")[0])==Integer.parseInt(w1.toString().split(" ")[0])){
            return 0;
        }else if(Integer.parseInt(w1.toString().split(" ")[0])>Integer.parseInt(w1.toString().split(" ")[0])){
            return 1;
        }else if(Integer.parseInt(w1.toString().split(" ")[0])<Integer.parseInt(w1.toString().split(" ")[0])){
            return -1;
        }
        return 0;
    }
}
