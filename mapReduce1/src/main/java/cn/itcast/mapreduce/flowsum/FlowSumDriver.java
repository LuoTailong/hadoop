package cn.itcast.mapreduce.flowsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class FlowSumDriver {
    public static void main(String[] args) throws Exception {
        //1 构造配置文件
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2 指定jar包所在的类
        job.setJarByClass(FlowSumDriver.class);

        //3 指定map和reduce类
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        //4 指定map输出的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //5 指定reduce输出的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //6 指定输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("C:\\hadoop\\test\\input\\inputphone"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\hadoop\\test\\output\\outputphone"));

        //7 提交
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
