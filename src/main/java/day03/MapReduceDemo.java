package day03;

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

public class MapReduceDemo {
    //前两个参数是固定，Long是下标，输出端Text相当于String，IntWritable相当于int
    public static class MapTask extends Mapper<LongWritable, Text,Text, IntWritable> {
        //map端读取数据并处理
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split(" ");
            for (String word : splits) {
                context.write(new Text(word), new IntWritable(1));
            }
        }
    }

        //reduce端(前两个和map后两个一样)
     public static class ReduceTask extends Reducer<Text,IntWritable, Text,IntWritable>{
        @Override
         protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
             int count = 0;
             for (IntWritable value : values) {
                 count++;
              }
              context.write(key,new IntWritable(count));
         }
     }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置类型
        job.setMapperClass(MapTask.class);
        job.setReducerClass(ReduceTask.class);
        job.setJarByClass(MapReduceDemo.class);

        //设置输出参数
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置路径路径
        FileInputFormat.addInputPath(job,new Path("F:\\a.txt"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\a1.txt"));//不能存在名字一样的


        //
        boolean completion = job.waitForCompletion(true);
        System.out.println(completion?"读写完成":"读写失败 ");
    }
}
