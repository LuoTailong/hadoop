package com.s.mapreduce.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class SequenceFileDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"C:/hadoop/test/input/inputformat", "C:/hadoop/test/output/outputformat"};
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);
        job.setJarByClass(SequenceFileDriver.class);
        job.setMapperClass(SequenceFileMapper.class);
        job.setReducerClass(SequenceFileReducer.class);

        //设置输入的inputFormat
        job.setInputFormatClass(WholeFileInputformat.class);
        //设置输出的outputFormat
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(ByteWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ByteWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
