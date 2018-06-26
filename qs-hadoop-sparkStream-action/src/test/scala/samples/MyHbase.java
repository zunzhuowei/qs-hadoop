package samples;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


/**
 * Created by zun.wei on 2018/6/26 11:22.
 * Description:
 */
public class MyHbase {

    private static Admin admin = null;
    private static Connection connection = null;

    @Before
    public void init() throws IOException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "node1,node2,node3,node4");
        connection = ConnectionFactory.createConnection(config);
        admin = connection.getAdmin();
    }


    @After
    public void after() throws IOException {
        admin.flush(TableName.valueOf("t_java"));//写入hfile磁盘中
        admin.close();
    }


    @Test
    public void createTableIfExeitDrop() throws IOException {
		/*String tableName = "t_java";
		String familyName = "bean";
		if (admin.tableExists(TableName.valueOf(tableName))) {
			admin.disableTable(TableName.valueOf(tableName));
			admin.deleteTable(TableName.valueOf(tableName));
		}
		HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
		table.addFamily(new HColumnDescriptor(familyName).setCompressionType(Algorithm.GZ));
		System.out.print("Creating table. ");
		admin.createTable(table);*/
    }

    @Test
    public void put() throws IOException {
		/*Table table = connection.getTable(TableName.valueOf("t_java"));
		String row = "17620329870_" + System.nanoTime();
		Put put = new Put(row.getBytes());
		put.addColumn("bean".getBytes(), "time".getBytes(), "2018-01-11 12:09:58".getBytes());
		put.addColumn("bean".getBytes(), "type".getBytes(), "1".getBytes());
		put.addColumn("bean".getBytes(), "from".getBytes(), "18775729870".getBytes());
		table.put(put);
		table.close();*/
    }

    @Test
    public void get() throws IOException {
        Table table = connection.getTable(TableName.valueOf("t_java"));
        String row = "17620329870_41473801510029";
        Get get = new Get(row.getBytes());
        Result result = table.get(get);
        CellScanner cs = result.cellScanner();
        while (cs.advance()) {
            Cell cell = cs.current();
            byte[] b = CellUtil.cloneValue(cell);
            System.out.println(new String(b));
        }
        table.close();
        System.out.println("--------------->> get() end");
    }

    @Test
    public void scanFimaly() throws IOException {
        Table table = connection.getTable(TableName.valueOf("t_java"));
        ResultScanner  rs = table.getScanner("bean".getBytes());
        rs.forEach(e -> {
            List<Cell> lc = e.listCells();
            lc.forEach(ee -> {
                byte[] key = CellUtil.cloneQualifier(ee);
                byte[] v = CellUtil.cloneValue(ee);
                System.out.println(new String(key) + " --- " + new String(v));
            });
        });
        table.close();
        System.out.println("--------------->> scanFimaly() end");
    }

}
