package com.qs.game.streaming.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * HBase操作工具类：Java工具类建议采用单例模式封装
 */
@Component
public class HBaseUtils {


    @Value("${zookeeper.url}")
    private String zk;

    @Value("${hbase.url}")
    private String hbase;

    @Value("${hadoop.home.dir}")
    private String hadoopHome;

    @Value("${hadoop.user.name}")
    private String hadoopUserName;



    private static HBaseAdmin admin = null;
    private static Configuration configuration = null;


    /**
     * 私有改造方法
     */
    private HBaseUtils() {

    }

    private Configuration createConfiguration() {
        if (null == configuration) {
            Configuration conf = new Configuration();
            conf.set("hbase.zookeeper.quorum", zk);
            conf.set("hbase.rootdir", hbase);
            System.setProperty("hadoop.home.dir", hadoopHome);
            System.setProperty("HADOOP_USER_NAME", hadoopUserName);
            configuration = conf;
            return conf;
        } else {
            return configuration;
        }
    }

    private HBaseAdmin createHBaseAdmin() throws IOException {
        if (null == admin) {
            return new HBaseAdmin(this.createConfiguration());
        } else {
            return admin;
        }
    }


    /**
     * 根据表名获取到HTable实例
     */
    public HTable getTable(String tableName) {
        HTable table = null;
        try {
            table = new HTable(this.createConfiguration(), tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return table;
    }

    /**
     * 添加一条记录到HBase表
     *
     * @param tableName HBase表名
     * @param rowkey    HBase表的rowkey
     * @param cf        HBase表的columnfamily
     * @param column    HBase表的列
     * @param value     写入HBase表的值
     */
    public void put(String tableName, String rowkey, String cf, String column, String value) throws IOException {
        HTable table = getTable(tableName);

        Put put = new Put(Bytes.toBytes(rowkey));
        put.add(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(value));

        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if (table != null)
                table.close();
        }
    }

    public boolean tableExists(String tableName) throws IOException {
        return this.createHBaseAdmin().tableExists(TableName.valueOf(tableName));
    }

    /**
     * 创建表，如果存在则删除表
     * @param tableName 表名
     * @param familyName 列族名字
     */
    public void createTableIfExeitDrop(String tableName,String familyName) throws IOException {
		if (this.tableExists(tableName)) {
            this.createHBaseAdmin().disableTable(TableName.valueOf(tableName));
            this.createHBaseAdmin().deleteTable(TableName.valueOf(tableName));
        }
        synchronized (this) {
            if (!this.tableExists(tableName)) {
                HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
                table.addFamily(new HColumnDescriptor(familyName).setCompressionType(Compression.Algorithm.GZ));
                System.out.print("Creating table. ");
                this.createHBaseAdmin().createTable(table);
            }
        }
        //admin.close();
    }

    /**
     *  扫描列表
     * @param tableName 表名
     * @param familyName 列族
     * @return  List<Map<String, String>> 返回该表该列族的所有列的值列表
     * @throws IOException
     */
    public List<Map<String, String>> scanFimaly(String tableName,String familyName) throws IOException {
        HTable table = this.getTable(tableName);
        //Table table = admin.getTgetTable(TableName.valueOf("t_java"));
        ResultScanner rs = table.getScanner(familyName.getBytes());
        List<Map<String, String>> result = new ArrayList<>();
        rs.forEach(e -> {
            List<Cell> lc = e.listCells();
            lc.forEach(ee -> {
                byte[] key = CellUtil.cloneQualifier(ee);
                byte[] v = CellUtil.cloneValue(ee);
                //System.out.println(new String(key) + " --- " + new String(v));
                Map<String, String> map = new HashMap<>();
                try {
                    map.put(new String(key,"UTF-8"), Bytes.toLong(v) + "");
                } catch (UnsupportedEncodingException e1) {
                    e1.printStackTrace();
                }
                result.add(map);
            });
        });
        table.close();
       return result;
    }


    /**
     * 根据表名和输入条件获取HBase的记录数
     */
    public Map<String, Long> query(String tableName, String cf, String qualifier, String condition) throws Exception {

        Map<String, Long> map = new HashMap<>();

        HTable table = getTable(tableName);
        //String cf = "info";
        //String qualifier = "click_count";

        Scan scan = new Scan();

        Filter filter = new PrefixFilter(Bytes.toBytes(condition));
        scan.setFilter(filter);

        ResultScanner rs = table.getScanner(scan);
        for (Result result : rs) {
            String row = Bytes.toString(result.getRow());
            long clickCount = Bytes.toLong(result.getValue(cf.getBytes(), qualifier.getBytes()));
            map.put(row, clickCount);
        }

        return map;
    }


    /*public static void main(String[] args) throws IOException {
        System.setProperty("hadoop.home.dir", "E:\\hadoop");
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        HBaseUtils hBaseUtils = HBaseUtils.getInstance();
        String tableName = "qs_access_log";
        String fimalyName = "info";
        //hBaseUtils.createTableIfExeitDrop(tableName, fimalyName);

        HTable table = HBaseUtils.getInstance().getTable(tableName);
        System.out.println(table.getName().getNameAsString());

        String rowkey = "20171111_88";
        String cf = "info";
        String column = "click_count";
        String value = "2";

        HBaseUtils.getInstance().put(tableName, rowkey, cf, column, value);

        List<Map<String, String>> mapList = HBaseUtils.scanFimaly(tableName, fimalyName);
        mapList.forEach(e -> System.out.println("e = " + e));

    }*/


}
