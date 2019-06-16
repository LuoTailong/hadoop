package com.s.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1 获取job配置信息
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //2 获取jar包位置
        job.setJarByClass(WordCountDriver.class);

        //3 关联自定义mapper和reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //8 设置读取文件切片的类
//        job.setInputFormatClass(CombineTextInputFormat.class);
//        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);// 4m
//        CombineTextInputFormat.setMinInputSplitSize(job, 2097152);// 2m

        //4 设置map输出数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5 设置Reduce输出数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //8 combiner添加
//        job.setCombinerClass(WordCountReducer.class);

        //6 设置数据输入和输出的文件路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //7 提交代码
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
}
