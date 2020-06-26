package mapreduce.test.MR1;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 根据自己的业务封装的一个实体
 */
public class MyBean implements Writable {
    private String phone;
    private Long up;
    private Long down;
    private Long sum;

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

    public MyBean() {
    }

    public MyBean(String phone, Long up, Long down, Long sum) {
        this.phone = phone;
        this.up = up;
        this.down = down;
        this.sum = sum;
    }

    @Override
    public String toString() {
        return "MyBean{" +
                "up=" + up +
                ", down=" + down +
                ", sum=" + sum +
                '}';
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.phone);
        dataOutput.writeLong(this.up);
        dataOutput.writeLong(this.down);
        dataOutput.writeLong(this.sum);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.phone = dataInput.readUTF();
        this.up = dataInput.readLong();
        this.down = dataInput.readLong();
        this.sum = dataInput.readLong();
    }
}
