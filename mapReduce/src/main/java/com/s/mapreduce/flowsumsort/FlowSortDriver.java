package com.s.mapreduce.flowsumsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowSortDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1 获取job对象
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //6 指定本程序的jar所在的本地路径
        job.setJarByClass(FlowSortDriver.class);

        //2 指定本业务的job要使用的mapper和reducer业务类
        job.setMapperClass(FlowSortMapper.class);
        job.setReducerClass(FlowSortReducer.class);

        //3 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        //4 指定reducer输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //5 指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //设置分区
        job.setPartitionerClass(FlowSortPartitioner.class);
        job.setNumReduceTasks(5);

        //7 将job中的配置的相关参数及job所用的jar包，提交给yarn去运行
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
