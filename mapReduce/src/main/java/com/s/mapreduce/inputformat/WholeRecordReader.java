package com.s.mapreduce.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class WholeRecordReader extends RecordReader<NullWritable, BytesWritable> {
    private Configuration configuration;
    private FileSplit split;
    private boolean isProcess = false;
    private BytesWritable value = new BytesWritable();

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //初始化
        this.split = (FileSplit) inputSplit;
        configuration = taskAttemptContext.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        //读取一个一个的文件
        if (!isProcess) {
            //0 缓冲区
            byte[] buf = new byte[(int) split.getLength()];

            FileSystem fs = null;
            FSDataInputStream fis = null;
            try {
                //1 获取文件系统
                Path path = split.getPath();
                fs = path.getFileSystem(configuration);

                //2 打开文件输入流
                fis = fs.open(path);

                //3 流的拷贝
                IOUtils.readFully(fis, buf, 0, buf.length);

                //4 拷贝缓存区的数据到最终输出
                value.set(buf, 0, buf.length);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                IOUtils.closeStream(fis);
                IOUtils.closeStream(fs);
            }
        }

        isProcess = true;
        return true;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return isProcess ? 1 : 0;
    }

    @Override
    public void close() throws IOException {

    }
}
