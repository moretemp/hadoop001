package day1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;


public class HDFS {
    FileSystem fileSystem;
    @Before
    public void init() throws Exception{
        Configuration conf = new Configuration();
        fileSystem = FileSystem.get(new URI("hdfs://hadoop01:9000"),conf,"root");
    }

    //上传一个文件到HDFS
    @Test
    public void upload(){
        try {
            fileSystem.copyFromLocalFile(new Path("F:\\a.txt"),new Path("/hadoop"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //下载文件
    @Test
    public void download(){
        try {
            fileSystem.copyToLocalFile(new Path("/hadoop/a.txt"),new Path("D:\\Hello"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @After
    public void close(){
        try {
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
