package com.qs.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * HBase操作工具类：Java工具类建议采用单例模式封装
 */
public class HBaseUtils {

    private HBaseAdmin admin = null;
    private Configuration configuration = null;


    /**
     * 私有改造方法
     */
    private HBaseUtils() {
        configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum", "docker:2181");
        configuration.set("hbase.rootdir", "hdfs://hadoop000:8020/hbase");

        try {
            admin = new HBaseAdmin(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static HBaseUtils instance = null;

    public static synchronized HBaseUtils getInstance() {
        if (null == instance) {
            instance = new HBaseUtils();
        }
        return instance;
    }


    /**
     * 根据表名获取到HTable实例
     */
    public HTable getTable(String tableName) {

        HTable table = null;

        try {
            table = new HTable(configuration, tableName);
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
        return admin.tableExists(TableName.valueOf(tableName));
    }

    /**
     * 创建表，如果存在则删除表
     * @param tableName 表名
     * @param familyName 列族名字
     */
    public void createTableIfExeitDrop(String tableName,String familyName) throws IOException {
		if (this.tableExists(tableName)) {
			admin.disableTable(TableName.valueOf(tableName));
			admin.deleteTable(TableName.valueOf(tableName));
		}
		HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
		table.addFamily(new HColumnDescriptor(familyName).setCompressionType(Compression.Algorithm.GZ));
		System.out.print("Creating table. ");
		admin.createTable(table);
		//admin.close();
    }

    /**
     *  扫描列表
     * @param tableName 表名
     * @param familyName 列族
     * @return  List<Map<String, String>>
     * @throws IOException
     */
    public static List<Map<String, String>> scanFimaly(String tableName,String familyName) throws IOException {
        HTable table = HBaseUtils.getInstance().getTable(tableName);
        //Table table = admin.getTgetTable(TableName.valueOf("t_java"));
        ResultScanner rs = table.getScanner(familyName.getBytes());
        List<Map<String, String>> result = new ArrayList<>();
        rs.forEach(e -> {
            List<Cell> lc = e.listCells();
            lc.forEach(ee -> {
                byte[] key = CellUtil.cloneQualifier(ee);
                byte[] v = CellUtil.cloneValue(ee);
                System.out.println(new String(key) + " --- " + new String(v));
                Map<String, String> map = new HashMap<>();
                map.put(new String(key), new String(v));
                result.add(map);
            });
        });
        table.close();
       return result;
    }

    public static void main(String[] args) throws IOException {
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

    }


}
