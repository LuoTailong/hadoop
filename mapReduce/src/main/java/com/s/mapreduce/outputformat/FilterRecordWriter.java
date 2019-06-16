package com.s.mapreduce.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class FilterRecordWriter extends RecordWriter<Text, NullWritable> {
    private Configuration configuration;
    FSDataOutputStream rootOut = null;
    FSDataOutputStream otherOut = null;

    public FilterRecordWriter(TaskAttemptContext taskAttemptContext) {
        // 1 获取文件系统
        FileSystem fs;
        try {
            fs = FileSystem.get(taskAttemptContext.getConfiguration());

            //2 创建输出文件路径
            Path rootPath = new Path("C:/hadoop/test/root.log");
            Path otherPath = new Path("C:/hadoop/test/other.log");

            //3 创建输出流
            rootOut = fs.create(rootPath);
            otherOut = fs.create(otherPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        configuration = taskAttemptContext.getConfiguration();
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        //判断是否包含"root"输出到不同文件
        if (key.toString().contains("root")){
            rootOut.write(key.toString().getBytes());
        }else {
            otherOut.write(key.toString().getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //关闭资源
        if (rootOut!=null){
            rootOut.close();
        }

        if (otherOut!=null){
            otherOut.close();
        }
    }
}
