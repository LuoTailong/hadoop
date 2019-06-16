package cn.itcast.mapreduce.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class FlowSortDriver {
    public static void main(String[] args) throws Exception {
        //1 指定配置文件
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2 指定jar包的类
        job.setJarByClass(FlowSortDriver.class);

        //3 指定map和reduce类
        job.setMapperClass(FlowSortMapper.class);
        job.setReducerClass(FlowSortReducer.class);

        //4 指定map输出类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        //5 指定reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //6 指定文件输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("C:\\hadoop\\test\\output\\outputphone"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\hadoop\\test\\output\\outputphonesort"));

        //7 提交
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
