package com.s.mapreduce.weblog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.xml.soap.Text;
import java.io.IOException;

public class LogDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args=  new String[]{"C:/hadoop/test/input/inputlog","C:/hadoop/test/output/outputlog"};

        //1 获取job信息
        Configuration configuration = new Configuration();
        Job job = Job.getInstance();

        //2 加载jar包
        job.setJarByClass(LogDriver.class);

        //3 关联map
        job.setMapperClass(LogMapper.class);

        //4 设置最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //5 设置输入和输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //6 提交
        job.waitForCompletion(true);
    }
}
