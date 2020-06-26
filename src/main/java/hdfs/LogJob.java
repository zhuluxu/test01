package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.quartz.Job;
import org.quartz.JobExecutionContext;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * Create by GuoJF on 2019/5/19
 */
public class LogJob implements Job {

    private static FileSystem fileSystem;
    private static Configuration configuration;


    @Override
    public void execute(JobExecutionContext jobExecutionContext) {


        System.setProperty("HADOOP_USER_NAME", "root");
        configuration = new Configuration();

        try {
            fileSystem = FileSystem.newInstance(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }

        configuration.addResource("conf/core-site.xml");
        configuration.addResource("conf/hdfs-site.xml");

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-hh-mm");

        String format = simpleDateFormat.format(new Date(new Date().getTime() - 120000));

        String filename = "access.tmp" + format + ".log";


        System.out.println(filename);

        File file = new File("E:\\home\\logs\\"+filename);

        if (file.exists()) {

            System.out.println("开始上传");

            try {
                fileSystem.copyFromLocalFile(new Path(file.getAbsolutePath()), new Path("/log/" + filename));

                boolean exists = fileSystem.exists(new Path("/log/" + filename));
                if (exists) {

                    System.out.println("上传成功");
                } else {


                    System.out.println("上传失败");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }else {

            System.out.println("该文件不存在，无需上传");
        }


        try {
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // System.out.println("1212312312");


    }
}
