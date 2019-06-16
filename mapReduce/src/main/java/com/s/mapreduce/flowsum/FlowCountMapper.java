package com.s.mapreduce.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    Text k = new Text();
    FlowBean v = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1363157993055  13560436666 C4-17-FE-BA-DE-D9:CMCC 120.196.100.99  18 15 1116  954  200
        //1 获取一行
        String line = value.toString();

        //2 切割字段
        String[] fields = line.split(" ");

        //3 封装对象
        //取出手机号码
        String phoneNum = fields[1];
        //取出上行流量和下行流量
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);

        //4 写出
        context.write(new Text(phoneNum),new FlowBean(upFlow,downFlow));

    }
}
