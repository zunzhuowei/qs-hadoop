package com.qs.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
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
    private FileSystem fileSystem = null;
    private Configuration configuration = null;//00:0c:29:15:40:64

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
        Path src = new Path("I:\\dept.sql");
        Path dst = new Path("/hdfsapi/test02/dept.sql");
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
        Path localPath = new Path("F:\\hdfs-output");
        Path hdfsPath = new Path("/hdfsapi/test02/dept.sql");
        fileSystem.copyToLocalFile(false, hdfsPath, localPath, true);
    }

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
    public void deleteFile() throws IOException {
        fileSystem.delete(new Path(""), true);//第二个参数表示是否递归删除，默认递归。
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
        System.out.println("\nhdfs end off");

    }


}
