package test.topn;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class MyIntWritable implements WritableComparable<MyIntWritable> {
    private Integer id;



    @Override
    public String toString() {
        return id.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MyIntWritable that = (MyIntWritable) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public MyIntWritable() {
    }

    public MyIntWritable(Integer id) {
        this.id = id;
    }

    @Override
    public int compareTo(MyIntWritable o) {
        return -id.compareTo(o.getId());
    }

    @Override
    public void write(DataOutput out) throws IOException {
           out.writeInt(id);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
           id = in.readInt();
    }
}
