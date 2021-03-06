package cn.itcast.mapreduce.province;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowProvinceReducer extends Reducer<FlowBean, Text, Text, FlowBean> {
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //因为当前values中只有一个,可用下述方法, 有多个的时候后用增强for遍历
        Text phoneNum = values.iterator().next();
        context.write(phoneNum, key);
    }
}
