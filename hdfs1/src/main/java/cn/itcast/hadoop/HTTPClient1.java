package cn.itcast.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.net.URI;

public class HTTPClient1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "2");
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"), conf, "root");
        fs.mkdirs(new Path("/a/b"));
        fs.create(new Path("/a/b/1"), true);
        fs.copyFromLocalFile(new Path("C:\\hadoop\\test\\input\\inputword\\hello.txt"), new Path("/a"));
        fs.copyToLocalFile(false, new Path("/a/hello.txt"),new Path("C:/hello.txt"),true);
        fs.rename(new Path("/a/hello.txt"),new Path("/a/helloHadoop.txt"));

        //是否是文件夹
        FileStatus[] fileStatus = fs.listStatus(new Path("/"));
        for (FileStatus status : fileStatus) {
            if (status.isFile()) {
                System.out.println("文件："+status.getPath().getName());
            }else {
                System.out.println("文件夹："+status.getPath().getName());
            }
        }

        //遍历文件详情
        RemoteIterator<LocatedFileStatus> list = fs.listFiles(new Path("/"), true);
        while (list.hasNext()) {
            LocatedFileStatus status = list.next();
            System.out.println("文件名："+status.getPath().getName());
            System.out.println("文件大小："+status.getLen());
            System.out.println("所属组："+status.getGroup());
            System.out.println("权限："+status.getPermission());

            BlockLocation[] blockLocations = status.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println("获取块的存储的主机节点："+host);
                }
            }
        }

        fs.delete(new Path("/zookeeper.out"),true);
        fs.close();
    }
}
