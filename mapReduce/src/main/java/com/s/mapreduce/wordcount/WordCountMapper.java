package com.s.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text k = new Text();
    IntWritable v = new IntWritable(1);

    //hello world
    //hadoop
    //spark

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1 一行内容转换成string
        String line = value.toString();

        //2 切割
        String[] words = line.split(" ");

        //3 循环写出到下一个阶段
        for (String word : words) {
            k.set(word);
            context.write(k, v);
        }
    }
}
