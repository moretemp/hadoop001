package day05;

import day04.MapReduceDemo3;
import day04.MovieBean;
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
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class movie {
    public static class MapTask extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //先把json数据转化为正常数据
            ObjectMapper mapper = new ObjectMapper();
            //利用mapper对象转化json
            MovieBean movieBean = null;
            movieBean = mapper.readValue(value.toString(), MovieBean.class);
            String movie = movieBean.getMovie();
            int rate = movieBean.getRate();
            context.write(new Text(movie+"\t"),new IntWritable(rate));
        }
    }


    public static class reduce extends Reducer<Text, IntWritable,Text,IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            List list = new ArrayList();
            for (IntWritable value : values) {
                list.add(value.get());
            }
            Collections.reverse(list);
            List newlist = list.subList(0,5);
            for (Object o : newlist) {
                int a = (int)o;
                context.write(key,new IntWritable(a));
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置类型
        job.setMapperClass(movie.MapTask.class);
        job.setReducerClass(movie.reduce.class);
        job.setJarByClass(movie.class);

        //设置输出参数
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置路径路径
        FileInputFormat.addInputPath(job,new Path("D:\\Documents\\WeChat Files\\jc18270788521\\FileStorage\\File\\2019-04\\movie.json"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\movie2.txt"));//不能存在名字一样的


        //
        boolean completion = job.waitForCompletion(true);
        System.out.println(completion?"读写完成":"读写失败 ");
    }
}
