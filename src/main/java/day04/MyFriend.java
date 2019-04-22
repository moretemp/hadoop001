package day04;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyFriend {
    public static class MapTask extends Mapper<LongWritable, Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split(":");
            String user = splits[0];
            String lists = splits[1];
            String[] list = lists.split(",");
            for (String friend : list) {
                context.write(new Text(friend),new Text(user));
            }
        }
    }



    public static class Reduce extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //用来存放字符串
            StringBuffer s = new StringBuffer();
            boolean flag = true;
            for (Text value : values) {
                if (flag){
                    s.append(value);
                    flag=false;
                }else {
                    s.append(",").append(value);
                }
            }
            context.write(new Text(key),new Text(s.toString()));
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置类型
        job.setMapperClass(MyFriend.MapTask.class);
        job.setReducerClass(MyFriend.Reduce.class);
        job.setJarByClass(MyFriend.class);

        //设置输出参数
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //设置路径路径
        FileInputFormat.addInputPath(job,new Path("D:\\Documents\\WeChat Files\\jc18270788521\\FileStorage\\File\\2019-04\\friend.txt"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\friend1.txt"));//不能存在名字一样的


        //
        boolean completion = job.waitForCompletion(true);
        System.out.println(completion?"读写完成":"读写失败 ");
    }
}
