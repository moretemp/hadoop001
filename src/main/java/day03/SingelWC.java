package day03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public class SingelWC {
    public static void main(String[] args) throws Exception {
        Map<String,Integer> map = new HashMap<String, Integer>();
        //获取连接
        Configuration conf = new Configuration();
        //conf.set("fs.defaultFS");
        //FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop01:9000"),conf,"root"); //hfds端
        FileSystem fileSystem = FileSystem.get(conf);//获取本地连接
        //读取数据
        FSDataInputStream open = fileSystem.open(new Path("F:\\a.txt"));
        BufferedReader read = new BufferedReader(new InputStreamReader(open));

        //map处理数据(k，v)
        String line;
        while((line=read.readLine())!=null){
            String[] splits = line.split(" ");
            for (String word : splits) {
                Integer count = map.getOrDefault(word, 0);
                count++;
                map.put(word,count);
            }
        }


        //给reduce
            //1.给一个写出去的路径
        FSDataOutputStream out = fileSystem.create(new Path("E:\\a.txt"));
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
            //2.遍历map
        Set<Map.Entry<String, Integer>> entries =   map.entrySet();
        List<Map.Entry<String, Integer>> list = new ArrayList(entries);
        list.sort(new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                return o1.getValue()-o2.getValue();
            }
        });
        for (Map.Entry<String, Integer> stringIntegerEntry : list) {
            writer.write(stringIntegerEntry.getKey()+"="+stringIntegerEntry.getValue());
            writer.newLine();
        }


        //关流
        writer.close();
        read.close();
    }
}
