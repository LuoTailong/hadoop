package cn.itcast.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class HDFSClient {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"), conf, "root");

        fs.create(new Path("/hdfsByJava"),false);
        fs.copyFromLocalFile(new Path("C:\\hadoop\\test\\hadoop.txt"),new Path("/1"));
        fs.close();
    }
}
