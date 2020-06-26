package mapreduce.test03.cha01;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Create by GuoJF on 2019/4/3
 */
public class FlowBean implements Writable {

    private String phone;
    private Long up;
    private Long down;
    private Long sum;

    public FlowBean() {
    }

    public FlowBean(String phone ,Long up, Long down, Long sum) {
        super();
        this.phone = phone;
        this.up = up;
        this.down = down;
        this.sum = sum;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public Long getUp() {
        return up;
    }

    public void setUp(Long up) {
        this.up = up;
    }

    public Long getDown() {
        return down;
    }

    public void setDown(Long down) {
        this.down = down;
    }

    public Long getSum() {
        return sum;
    }

    public void setSum(Long sum) {
        this.sum = sum;
    }

    @Override
    public String toString() {
        return " up:" + up +
                " down:" + down +
                " sum:" + sum ;
    }

    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeUTF(this.phone);
        dataOutput.writeLong(this.up);
        dataOutput.writeLong(this.down);
        dataOutput.writeLong(this.sum);

    }

    public void readFields(DataInput dataInput) throws IOException {


        this.phone = dataInput.readUTF();
        this.up = dataInput.readLong();
        this.down = dataInput.readLong();
        this.sum = dataInput.readLong();

    }
}
