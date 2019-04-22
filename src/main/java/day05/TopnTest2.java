package day05;


import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

public class TopnTest2 {
    public static class MapTask extends Mapper<LongWritable,Text,Text,MovieBean> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //json解析
            try {
                ObjectMapper mapper = new ObjectMapper();
                MovieBean bean = mapper.readValue(value.toString(), MovieBean.class);
                String movie = bean.getMovie();
                context.write(new Text(movie), bean);
            }catch (Exception e){
                System.err.println("跳过该错误");
            }
        }
    }
    public static class ReduceTask extends Reducer<Text,MovieBean,MovieBean,NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<MovieBean> values, Context context) throws IOException, InterruptedException {

            //当数据量特别大的时候，内存会溢出
            TreeSet<MovieBean> tree = new TreeSet<MovieBean>(new Comparator<MovieBean>() {
                @Override
                public int compare(MovieBean o1, MovieBean o2) {
                    return o2.getRate()-o1.getRate();
                }
            });
            //拿到数据添加到treeset
            for (MovieBean value : values) {
                if(tree.size()<5){
                    tree.add(value);
                }else{
                    MovieBean last = tree.last();
                    if(last.getRate()<value.getRate()){
                        tree.remove(last);
                        tree.add(value);
                    }
                }
            }
            for (MovieBean movieBean : tree) {
                context.write(movieBean,NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //conf.set("fs.defaultFS","hdfs://hadoop01:9000");
        Job job = Job.getInstance(conf);
        //设置任务使用的类都有哪些
        job.setMapperClass(MapTask.class);
        job.setReducerClass(ReduceTask.class);
        job.setJarByClass(TopnTest2.class);
        //设置输出参数类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MovieBean.class);
        job.setOutputKeyClass(MovieBean.class);
        job.setOutputValueClass(NullWritable.class);
        //设置reducetask的数量
        job.setNumReduceTasks(1);
        //判断文件夹是否存在
        String path="D:\\test5";
        File file = new File(path);
        if(file.exists()){
            FileUtils.deleteDirectory(file);
        }
        //设置数据输入输出目录
        FileInputFormat.addInputPath(job,new Path("D:\\test\\movie.json"));
        FileOutputFormat.setOutputPath(job,new Path(path));
        //查看返回结果
        boolean completion = job.waitForCompletion(true);
        System.out.println(completion?"老铁，没毛病":"出错了，快去看看哪里的毛病");
    }

}
