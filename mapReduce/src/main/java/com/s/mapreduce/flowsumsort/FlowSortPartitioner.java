package com.s.mapreduce.flowsumsort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowSortPartitioner extends Partitioner<FlowBean, Text> {
    @Override
    public int getPartition(FlowBean key, Text value, int i) {
        int partition = 0;

        String preNum = value.toString().substring(0, 3);

        if (" ".equals(preNum)) {
            partition = 5;
        } else if ("136".equals(preNum)) {
            partition = 1;
        } else if ("137".equals(preNum)) {
            partition = 2;
        } else if ("138".equals(preNum)) {
            partition = 3;
        } else if ("139".equals(preNum)) {
            partition = 4;
        }

        return partition;

    }
}
