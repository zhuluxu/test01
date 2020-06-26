package mapreduce.test04;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * Create by GuoJF on 2019/4/4
 */
public class Student implements Writable {
    private String stuNum;
    private String name;
    private List<String> classList;

    public Student(String stuNum, String name, List<String> classList) {
        this.stuNum = stuNum;
        this.name = name;
        this.classList = classList;
    }

    public Student() {
    }

    public String getStuNum() {
        return stuNum;
    }

    public void setStuNum(String stuNum) {
        this.stuNum = stuNum;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getClassList() {
        return classList;
    }

    public void setClassList(List<String> classList) {
        this.classList = classList;
    }

    @Override
    public String toString() {
        return "Student{" +
                "stuNum='" + stuNum + '\'' +
                ", name='" + name + '\'' +
                ", classList=" + classList +
                '}';
    }


    public void write(DataOutput dataOutput) throws IOException {

        if (this.stuNum != null) {
            dataOutput.writeUTF(this.stuNum);
        }


        if (this.name != null) {
            dataOutput.writeUTF(this.name);
        }

        if (this.classList != null) {
            this.classList.forEach((a) -> {
                try {
                    dataOutput.writeUTF(a);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }

    }

    public void readFields(DataInput dataInput) throws IOException {

        this.stuNum = dataInput.readUTF();
        this.name = dataInput.readUTF();



    }
}
