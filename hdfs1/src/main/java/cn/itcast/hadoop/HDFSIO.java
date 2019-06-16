package cn.itcast.hadoop;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;

public class HDFSIO {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"),conf,"root");
        FileInputStream in = new FileInputStream(new File("C:\\hadoop\\test\\hadoop.txt"));
        FSDataOutputStream out = fs.create(new Path("/2"));
        IOUtils.copy(in,out);
        fs.close();
    }
}
