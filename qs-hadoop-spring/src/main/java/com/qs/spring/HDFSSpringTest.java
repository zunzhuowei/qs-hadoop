package com.qs.spring;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.IOException;

/**
 * Created by zun.wei on 2018/6/10.
 * To change this template use File|Default Setting
 * |Editor|File and Code Templates|Includes|File Header
 */
public class HDFSSpringTest {

    private FileSystem fileSystem;
    private ApplicationContext applicationContext;

    @Test
    public void listFiles() throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            String dir = fileStatus.isDirectory() ? "文件夹" : "文件";
            short replication = fileStatus.getReplication();
            long len = fileStatus.getLen();
            String path = fileStatus.getPath().toString();
            System.out.println(dir + "\t" + replication + "\t" + len + "\t" + path);
        }
    }

    @Test
    public void getFile() throws IOException {
        Path localPath = new Path("F:\\hdfs-output");
        Path hdfsPath = new Path("/hdfsapi/test02/dept.sql");
        fileSystem.copyToLocalFile(false, hdfsPath, localPath, true);
    }


    /**
     * 查看文件
     * @throws IOException
     */
    @Test
    public void catInfo() throws IOException {
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/hdfsapi/test03/idea2.txt"), 1024);
        IOUtils.copyBytes(fsDataInputStream, System.out, 1024);
        fsDataInputStream.close();
    }



    @Before
    public void setUp() {
        applicationContext = new ClassPathXmlApplicationContext("beans.xml");
        fileSystem = (FileSystem) applicationContext.getBean("fileSystem");

    }

    @After
    public void endOff() throws IOException {
        fileSystem.close();
        applicationContext = null;

    }

}
