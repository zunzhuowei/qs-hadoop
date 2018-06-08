package com.qs.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by zun.wei on 2018/6/8 15:13.
 * Description:
 */
public class HdfsApp {

    private static final String HDFS_URL = "hdfs://hadoop00:8020";
    FileSystem fileSystem = null;
    Configuration configuration = null;//00:0c:29:15:40:64

    /**
     * 创建目录
     * @throws IOException
     */
    @Test
    public void mkdir() throws IOException {
        fileSystem.mkdirs(new Path("/hdfsapi/test02"));
    }

    /**
     * 拷贝本地文件到hdfs
     * @throws IOException
     */
    @Test
    public void putFile() throws IOException {
        Path src = new Path("D:\\xueyou_play_show.sql");
        Path dst = new Path("/hdfsapi/test02");
        fileSystem.copyFromLocalFile(src,dst);
    }


    /**
     * 创建文件
     * @throws IOException
     */
    @Test
    public void creatFile() throws IOException {
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/hdfsapi/test03/idea.txt"), true);
        fsDataOutputStream.write("I'm from idea ide info ".getBytes());
        fsDataOutputStream.close();
    }

    /**
     * 查看文件
     * @throws IOException
     */
    @Test
    public void catInfo() throws IOException {
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/hdfsapi/test03/idea.txt"), 1024);
        IOUtils.copyBytes(fsDataInputStream, System.out, 1024);
        fsDataInputStream.close();
    }

    /**
     * 重命名
     * @throws IOException
     */
    @Test
    public void rename() throws IOException {
        boolean b = fileSystem.rename(new Path("/hdfsapi/test03/idea.txt"), new Path("/hdfsapi/test03/idea2.txt"));
        System.out.println("b = " + b);
    }


    @Test
    public void getFile() throws IOException {
        Path localPath = new Path("E:/xueyou_play_show.sql");
        Path hdfsPath = new Path("/hdfsapi/test02/xueyou_play_show.sql");
        fileSystem.copyToLocalFile(hdfsPath,localPath);
    }

    @Test
    public void listFiles() {

    }

    @Test
    public void deleteFile() {

    }


    @Before
    public void startUP() throws URISyntaxException, IOException, InterruptedException {
        System.out.println("hdfs startup\n");
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_URL),configuration,"hadoop");

    }

    @After
    public void endOff() {
        FileSystem fileSystem = null;
        Configuration configuration = null;
        System.out.println("\nhdfs endoff");

    }


}
