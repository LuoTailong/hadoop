package com.s.mapreduce.inputformat;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SequenceFileReducer extends Reducer<Text, BooleanWritable, Text, BooleanWritable> {
    @Override
    protected void reduce(Text key, Iterable<BooleanWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key,values.iterator().next());
    }
}
