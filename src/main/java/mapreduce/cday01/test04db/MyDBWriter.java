package mapreduce.cday01.test04db;

import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Create by GuoJF on 2019/5/22
 */
public class MyDBWriter implements DBWritable {

    private String word;
    private int cout;

    public MyDBWriter() {
    }

    public MyDBWriter(String word, int cout) {
        this.word = word;
        this.cout = cout;
    }


    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getCout() {
        return cout;
    }

    public void setCout(int cout) {
        this.cout = cout;
    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {

        preparedStatement.setString(0, word);
        preparedStatement.setInt(1, cout);

    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {

    }
}
