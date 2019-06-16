package com.s.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.net.URI;

public class HdfsClient {

    /**
     * HDFS获取文件系统
     */
    @Test
    public void initHDFS() throws Exception {
        // 1 创建配置信息对象
        Configuration configuration = new Configuration();
        // 2 获取文件系统
        FileSystem fs = FileSystem.get(configuration);
        // 3 打印文件系统
        System.out.println(fs.toString());
    }

    /**
     * HDFS文件上传(测试参数优先级)
     */
    @Test
    public void testCopyFromLocalFIle() throws Exception {
        //1 获取文件系统
        Configuration configuration = new Configuration();
        //配置在集群上运行
        //configuration.set("fs.defaultFS","2");
        //configuration.set("fs.replication","1");默认是3
        //FileSystem fs = FileSystem.get(configuration);
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"), configuration, "root");

        //2 上传文件
        fs.copyFromLocalFile(new Path("c:/hello.txt"), new Path("/hello.txt"));

        //3 关闭资源
        fs.close();
        System.out.println("over");
    }

    /**
     * HDFS文件的下载
     */
    @Test
    public void testCopyToLocalFile() throws Exception {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"), configuration, "root");
        // 2 执行下载操作
        //boolean delSrc 指是否将原文件删除
        //Path src 指要下载的文件路径
        //Path dst 指将文件下载到的路径
        //boolean useRawLocalFileSystem 是否开启文件校验
        fs.copyToLocalFile(false, new Path("/hello.txt"), new Path("C:/hadoop.txt"), true);
        //3 关闭资源
        fs.close();
        System.out.println("over");
    }

    @Test
    /**
     * HDFS文件目录的创建
     */
    public void testMkdir() throws Exception {
        //1.获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"), configuration, "root");
        //2.创建目录
        fs.mkdirs(new Path("/0906/zhangsan"));
        //3.关闭资源
        fs.close();
    }

    /**
     * HDFS文件的删除
     */
    @Test
    public void testDelete() throws Exception {
        //1.获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"), configuration, "root");

        //2.执行删除
        fs.delete(new Path("/ok.txt"), true);
        //3.关闭资源
        fs.close();
    }

    /**
     * HDFS 文件名的修改
     */
    @Test
    public void testRename() throws Exception {
        //1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"), configuration, "root");
        //2 修改文件名称
        fs.rename(new Path("/hello.txt"), new Path("/Im.txt"));
        //3. 关闭资源
        fs.close();
    }

    @Test
    public void testListFiles() throws Exception {
        //1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"), configuration, "root");
        //2 获取文件详情
        /**
         * 思考:为什么返回迭代器不是list之类的容器
         * 集合占内存多 一次全部拿取
         * 迭代器一次拿一个
         */
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while (listFiles.hasNext()) {
            LocatedFileStatus status = listFiles.next();
            //输出详情
            //文件名称
            System.out.println("文件名:" + status.getPath().getName());
            //长度
            System.out.println("文件大小" + status.getLen());
            //获取组
            System.out.println("所属组:" + status.getGroup());
            //权限
            System.out.println("权限:" + status.getPermission());

            //获取存储的块信息
            BlockLocation[] blockLocations = status.getBlockLocations();

            for (BlockLocation blockLocation : blockLocations) {
                //获取块的存储的主机节点
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println("获取块的存储的主机节点:" + host);
                }
            }
            System.out.println("-------------------------");
        }

        fs.close();
    }

    /**
     * HDFS文件和文件夹的判断
     */
    @Test
    public void testListStatus() throws Exception {
        //1 获取文件配置信息
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"), configuration, "root");
        //2 判断是文件还是文件夹
        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        for (FileStatus fileStatus : listStatus) {
            //如果是文件
            if (fileStatus.isFile()) {
                System.out.println("文件:" + fileStatus.getPath().getName());
            } else {
                System.out.println("文件夹:" + fileStatus.getPath().getName());
            }
        }
        //关闭资源
        fs.close();
    }
}
