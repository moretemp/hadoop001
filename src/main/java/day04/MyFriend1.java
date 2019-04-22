package day04;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyFriend1 {
    public static class MapTesk extends Mapper<LongWritable, Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //
            String[] split = value.toString().split("\t");
            String val = split[0];
            String user = split[1];
            String[] zuhe = user.split(",");
            for (int i=0;i<zuhe.length;i++){
                for (int j=i+1;j<zuhe.length;j++){
                    String line = zuhe[i]+"-"+zuhe[j];
                    context.write(new Text(line),new Text(val));
                }
            }
        }
    }


    public static class Reduce extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer s = new StringBuffer();
            boolean falg = true;
        }
    }


}
