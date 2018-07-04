package com.qs.springboot;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.hadoop.fs.FsShell;

import javax.annotation.Resource;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;


/**
 * 运行启动类即可
 */
@SpringBootApplication
@Configuration
public class HDFSSrpingBootApp implements CommandLineRunner {



    public static void main(String[] args) {
        //System.setProperty("HADOOP_USER_NAME", "hadoop");
        SpringApplication.run(HDFSSrpingBootApp.class, args);
    }


    //以下为自定义FsShell的代码。
    @Resource(name = "myFsShell")
    private FsShell shell;
    private static final String HDFS_URL = "hdfs://hadoop00:8020";
    @Bean("myFsShell")
    public FsShell configurationFsShell() throws URISyntaxException, IOException, InterruptedException {
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_URL),configuration,"hadoop");
        return new FsShell(configuration, fileSystem);
    }



    @Override
    public void run(String... strings) throws Exception {
        //如果不自定义FsShell对象则有些hdfs目录权限不能访问，只能访问本机器生成的目录。
        //如以下该目录  不能  查询结果
        Collection<FileStatus> fileStatuses = shell.lsr("/tmp");

        //如果不自定义FsShell对象则有些hdfs目录权限不能访问，只能访问本机器生成的目录。
        //如以下该目录  可以  查询结果
        //Collection<FileStatus> fileStatuses = shell.lsr("/hdfsapi");


        for (FileStatus fileStatus : fileStatuses) {
            System.out.println("fileStatus = " + fileStatus.getPath());
        }

        System.out.println("------------------------ ");
        Collection<String> texts = shell.text("/hdfsapi/test03/idea2.txt");
        for (String text : texts) {
            System.out.println("text = " + text);
        }
    }




}
