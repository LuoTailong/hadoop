package com.s.mapreduce.inputformat;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class SequenceFileMapper extends Mapper<NullWritable, ByteWritable, Text,ByteWritable> {
    Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //1 获取文件切片信息
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        //2 获取切片名称
        String name = inputSplit.getPath().toString();
        //3 设置key的输出
        k.set(name);
    }

    @Override
    protected void map(NullWritable key, ByteWritable value, Context context) throws IOException, InterruptedException {
        context.write(k,value);
    }
}
