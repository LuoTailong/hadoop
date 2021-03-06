package com.s.mapreduce.flowsumsort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
    FlowBean k = new FlowBean();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1363157993055  13560436666 C4-17-FE-BA-DE-D9:CMCC 120.196.100.99  18 15 1116  954  200
        //1 获取一行
        String line = value.toString();

        //2 切割字段
        String[] fields = line.split(" ");

        //3 封装对象
        long upFlow = Long.parseLong(fields[1]);
        long downFlow = Long.parseLong(fields[2]);
        k.set(upFlow, downFlow);
        v.set(fields[0]);

        //4 写出
        context.write(k, v);

    }
}
