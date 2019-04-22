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
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;

//需求：每部电影的评分的前5个最高分，对每个电影,取前5个比较高的分数
public class Topn1 {
    public static class MapTask extends Mapper<LongWritable,Text,Text,MovieBean>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            ObjectMapper mapper = new ObjectMapper();
            MovieBean movieBean = mapper.readValue(value.toString(), MovieBean.class);
            String movie = movieBean.getMovie();
            context.write(new Text(movie),movieBean);
        }
    }
    //TreeSet
    public static class ReduceTask extends Reducer<Text,MovieBean,MovieBean,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<MovieBean> values, Context context) throws IOException, InterruptedException {
            ArrayList<MovieBean> lists = new ArrayList<>();
            for (MovieBean value : values) {
                lists.add(value);
            }
            //排序
            lists.sort(new Comparator<MovieBean>() {
                @Override
                public int compare(MovieBean o1, MovieBean o2) {
                    return o2.getRate()-o1.getRate();
                }
            });
            //求取前5
            for (int i=0;i<5;i++) {
                context.write(lists.get(i), NullWritable.get());
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
        job.setJarByClass(Topn1.class);
        //设置输出参数类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MovieBean.class);
        job.setOutputKeyClass(MovieBean.class);
        job.setOutputValueClass(NullWritable.class);
        //设置reducetask的数量
        job.setNumReduceTasks(1);
        //判断文件夹是否存在
        String path="D:\\test4";
        File file = new File(path);
        if(file.exists()){
            FileUtils.deleteDirectory(file);
        }
        //设置数据输入输出目录
        FileInputFormat.addInputPath(job,new Path("D:\\Documents\\WeChat Files\\jc18270788521\\FileStorage\\File\\2019-04\\movie.json"));
        FileOutputFormat.setOutputPath(job,new Path(path));
        //查看返回结果
        boolean completion = job.waitForCompletion(true);
        System.out.println(completion?"老铁，没毛病":"出错了，快去看看哪里的毛病");
    }

}
