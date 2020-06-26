package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Create by GuoJF on 2019/5/19
 */
public class App {

    private FileSystem fileSystem;
    private Configuration configuration;

    @Before
    public void getFileSystem() {

        System.setProperty("HADOOP_USER_NAME", "root");

        configuration = new Configuration();

        /*
         * 第一种方式：直接使用代码设置相关参数
         * */
        // configuration.set("fs.defaultFS", "hdfs://Hadoop01:8020");


        /*
         * 第二种方式 使用配置文件的方式
         *
         * */

        //configuration.addResource("core-site.xml");
        //configuration.addResource("hdfs-site.xml");

        try {
            fileSystem = FileSystem.newInstance(configuration);

        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    @Test
    public void testConfig() {

        String s = configuration.get("fs.defaultFS");
        System.out.println(s);


    }

    @Test
    public void testUpload() throws Exception {


        FileInputStream inputStream = new FileInputStream("E:\\1.txt");

        FSDataOutputStream outputStream = fileSystem.create(new Path("/2.txt"), new Progressable() {
            @Override
            public void progress() {
                System.out.println("正在创建");
            }
        });


        IOUtils.copyBytes(inputStream, outputStream, 1024, true);


    }


    @Test
    public void testUpload02() throws Exception {

        fileSystem.copyFromLocalFile(new Path("E:\\1.txt"), new Path("/3.txt"));

    }


    @Test
    public void testDownload01() throws Exception {

        FileOutputStream fileOutputStream = new FileOutputStream("E:\\5.txt");


        Path path = new Path("/1.txt");

        FSDataInputStream inputStream = fileSystem.open(path);

        IOUtils.copyBytes(inputStream, fileOutputStream, 1024, true);


    }


    @Test
    public void testDownload02() throws Exception {


        fileSystem.copyToLocalFile(new Path("/1.txt"), new Path("E:\\6.txt"));

    }

    @Test
    public void testDelete() throws Exception {

        boolean delete = fileSystem.delete(new Path("/1.txt"));

        if (delete) {

            System.out.println("删除成功");
        }
    }


    @Test
    public void testExists() throws Exception {

        boolean exists = fileSystem.exists(new Path("/1.txt"));

        if (exists) {
            System.out.println("file exists");
        } else {

            System.out.println("file is  not exists");
        }


    }

    @Test
    public void testMkdir() throws Exception {
        if (fileSystem.exists(new Path("/baizhi/test01"))) return;

        boolean mkdirs = fileSystem.mkdirs(new Path("/baizhi/test01"));


        if (mkdirs) {

            System.out.println("the dir is created");
        } else {

            System.out.println("create failed");
        }


    }


    @Test
    public void testListFile() throws IOException {


        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"),true);


        while (listFiles.hasNext()){

            LocatedFileStatus fileStatus = listFiles.next();

            System.out.println(fileStatus.getPath()+" "+fileStatus.isFile()+" "+fileStatus.getLen());
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();

            for (BlockLocation location : blockLocations) {

                System.out.println("offset:"+location.getOffset()+",length:"+location.getLength());
            }

        }

    }



    @Test
    public void deleteToTrash() throws IOException {

        Trash trash = new Trash(fileSystem, configuration);
        boolean b = trash.moveToTrash(new Path("/3.txt"));
        if (b){

            System.out.println("already move to trash");
        }

    }


    @After
    public void closeStream() throws Exception {

        fileSystem.close();

    }


}
