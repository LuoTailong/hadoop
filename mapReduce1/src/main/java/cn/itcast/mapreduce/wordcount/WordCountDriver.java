package cn.itcast.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCountDriver{
    public static void main(String[] args) throws Exception {
        //1 获取job配置信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2 指定jar包所在位置
        job.setJarByClass(WordCountDriver.class);

        //3 指定mapper和reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //4 设置map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5 设置reduce输出了类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //6 设置输入和输出的文件路径
        FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
        FileOutputFormat.setOutputPath(job, new Path("/wordcount/output"));

        //7 提交代码
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
