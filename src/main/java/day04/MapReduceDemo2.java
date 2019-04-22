package day04;

import day03.MapReduceDemo;
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

public class MapReduceDemo2 {
    public static class MapTesk extends Mapper<LongWritable, Text, Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");
            if (splits.length>=7){
                String phone = splits[0].substring(0,3);
                String province = splits[1];
                String yys = splits[3];
                context.write(new Text(phone+"\t"+province),new Text(yys));
            }
        }
    }



    public static class ReduceTask extends Reducer<Text,Text, Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(new Text(key),new Text(value));
                break;
            }
        }
    }



    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置类型
        job.setMapperClass(MapReduceDemo2.MapTesk.class);
        job.setReducerClass(MapReduceDemo2.ReduceTask.class);
        job.setJarByClass(MapReduceDemo2.class);

        //设置输出参数
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //设置路径路径
        FileInputFormat.addInputPath(job,new Path("D:\\Documents\\WeChat Files\\jc18270788521\\FileStorage\\File\\2019-04\\Phone(1).txt"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\phone(1).txt"));//不能存在名字一样的


        //
        boolean completion = job.waitForCompletion(true);
        System.out.println(completion?"读写完成":"读写失败 ");
    }
}
