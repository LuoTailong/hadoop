package com.s.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {


    //hello     1
    //world     1
    //hadoop    1
    //spark     1

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //1 统计单词总个数
        int sum = 0;
        for (IntWritable count : values) {
            sum+=count.get();
        }

        //2 输出单词总个数
        context.write(key,new IntWritable(sum));
    }
}
