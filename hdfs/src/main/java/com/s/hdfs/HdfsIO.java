package com.s.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsIO {
    /**
     * HDFS 文件上传
     */
    @Test
    public void putFile2Hdfs() throws Exception {
        //1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.6.222:9000"), configuration, "root");

        //2 获取输入流
        FileInputStream fis = new FileInputStream(new File("c:/hello.txt"));

        //3 获取输出流
        FSDataOutputStream fos = fileSystem.create(new Path("/hello2.txt"));

        //4 流的拷贝
        IOUtils.copyBytes(fis, fos, configuration);

        //5 关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }

    /**
     * HDFS 文件下载
     **/
    @Test
    public void getFileFromHDFS() throws Exception {
        //1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.6.222:9000"), configuration, "root");

        //2 获取输入流
        FSDataInputStream fis = fileSystem.open(new Path("/hello2.txt"));

        //3 获取输出流
        IOUtils.copyBytes(fis, System.out, configuration);

        //4 流的拷贝

        //5 关闭资源
        IOUtils.closeStream(fis);
    }

    /**
     * 定位文件读取
     */
    //下载第一模块
    @Test
    public void readFileSeek1() throws Exception {
        //1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.6.222:9000"), configuration, "root");

        //2 获取输入流
        FSDataInputStream fis = fileSystem.open(new Path("/user/ltl/test/hadoop-2.7.2.tar.gz"));

        //3 创建输出流
        FileOutputStream fos = new FileOutputStream(new File("c:/hadoop-2.7.2.tar.gz.part1"));

        //4 流的拷贝
        byte[] b = new byte[1024];
        for (int i = 0; i < 1024 * 128; i++) {
            fis.read(b);
            fos.write(b);
        }

        //5 关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }

    //下载第二模块
    @Test
    public void readFileSeek2() throws Exception {
        //1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.6.222:9000"), configuration, "root");

        //2 获取输入流
        FSDataInputStream fis = fileSystem.open(new Path("/user/ltl/test/hadoop-2.7.2.tar.gz"));

        //3 定位输入数据位置
        fis.seek(1024 * 1024 * 128);

        //4 创建输出流
        FileOutputStream fos = new FileOutputStream(new File("c:/hadoop-2.7.2.tar.gz.part2"));

        //5 流的拷贝
        IOUtils.copyBytes(fis, fos, configuration);

        //5 关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }

    //一致性模型
    @Test
    public void putFile() throws Exception {
        //1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.6.222:9000"), configuration, "root");

        // 2 获取输出流
        FSDataOutputStream fos = fileSystem.create(new Path("/hello3.txt"));

        // 3 写数据
        fos.write("hello".getBytes());

        // 4 一致性刷新
        fos.hflush();

        //6关闭流
        IOUtils.closeStream(fos);
        fileSystem.close();
    }
}
