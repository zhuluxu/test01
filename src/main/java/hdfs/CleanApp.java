package hdfs;

import java.io.*;

/**
 * Create by GuoJF on 2019/5/19
 */
public class CleanApp {

    public static void main(String[] args) throws IOException {

        File file = new File("E:\\home\\logs\\access.tmp2019-05-19-10-28.log");

        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));

        FileWriter fileWriter = new FileWriter("E:\\home\\logs\\clean_access.tmp2019-05-19-10-28.log");


        while (true) {


            String line = bufferedReader.readLine();
            if (line == null) return;


            boolean contains = line.contains("thisisshortvideoproject'slog");
            if (contains) {

                String s = line.split("thisisshortvideoproject'slog")[0];
                fileWriter.write(s+"\n");
                fileWriter.flush();
            }
        }



    }
}
