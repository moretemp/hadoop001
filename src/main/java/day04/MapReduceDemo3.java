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
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

public class MapReduceDemo3 {
    public static class MapTask extends Mapper<LongWritable,Text, Text, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //先把json数据转化为正常数据
            ObjectMapper mapper = new ObjectMapper();
            //利用mapper对象转化json
            MovieBean movieBean = null;
            movieBean = mapper.readValue(value.toString(),MovieBean.class);
            String movie = movieBean.getMovie();
            int rate = movieBean.getRate();
            context.write(new Text(movie+"\t"),new IntWritable(rate));
        }
    }



    public static class reduce extends Reducer<Text, IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count=0;
            int sum =0;
            for (IntWritable value : values) {
                int c = value.get();
                sum = sum+c;
                count++;
            }
            context.write(key,new IntWritable(sum/count));
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置类型
        job.setMapperClass(MapReduceDemo3.MapTask.class);
        job.setReducerClass(MapReduceDemo3.reduce.class);
        job.setJarByClass(MapReduceDemo3.class);

        //设置输出参数
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置路径路径
        FileInputFormat.addInputPath(job,new Path("D:\\Documents\\WeChat Files\\jc18270788521\\FileStorage\\File\\2019-04\\movie.json"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\movie.txt"));//不能存在名字一样的


        //
        boolean completion = job.waitForCompletion(true);
        System.out.println(completion?"读写完成":"读写失败 ");
    }
}
