package cn.itcast.mapreduce.province;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowProvinceMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
    FlowBean k = new FlowBean();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");

        String phoneNum = fields[0];
        long upFlow = Long.parseLong(fields[1]);
        long downFlow = Long.parseLong(fields[2]);

        k.set(upFlow,downFlow);
        v.set(phoneNum);
        context.write(k,v);
    }
}
