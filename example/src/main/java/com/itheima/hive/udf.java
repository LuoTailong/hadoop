package com.itheima.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

public class udf extends UDF {
    public String evaluate(String input) {
        return input.toLowerCase();
    }

    public int evaluate(int a, int b) {
        return a + b;
    }

}
