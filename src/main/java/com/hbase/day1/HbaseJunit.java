package com.hbase.day1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.NavigableMap;

public class HbaseJunit {
    private Connection conn;

    @Test
    public void createTable() throws Exception {
        //获取Admin(DDL) 或者 Table(DML)
        Admin admin = conn.getAdmin();
        TableName tableName = TableName.valueOf("bd05:count");
        //列族
        ColumnFamilyDescriptor cf1 = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("f")).
                setMaxVersions(3).build();
        //表
        TableDescriptor emp = TableDescriptorBuilder.newBuilder(tableName).setColumnFamily(cf1).build();
        admin.createTable(emp);
        admin.close();
    }

    @Test
    public void putData() throws Exception {
        //定义表的名称
        TableName tableName = TableName.valueOf("bd05:emp");
        //获取表对象
        Table table = conn.getTable(tableName);
        //准备数据
        String rowKey = "3";
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes("staff"), Bytes.toBytes("id"), Bytes.toBytes("1"));
        put.addColumn(Bytes.toBytes("staff"), Bytes.toBytes("name"), Bytes.toBytes("Tom"));
        put.addColumn(Bytes.toBytes("staff"), Bytes.toBytes("age"), Bytes.toBytes("25"));
        //添加数据
        table.put(put);
        table.close();
    }

    @Test
    public void getData() throws Exception {
        //定义表名称
        TableName tableName = TableName.valueOf("bd05:emp");
        //获取表
        Table table = conn.getTable(tableName);
        //准备数据
        String rowKey = "1";
        //拼装查询条件
        Get get = new Get(Bytes.toBytes(rowKey));
        //查询数据
        Result result = table.get(get);
        //打印数据
        List<Cell> cells = result.listCells();
        for (Cell cell : cells) {
            // 打印rowkey,family,qualifier,value
            System.out.println(Bytes.toString(CellUtil.cloneRow(cell))
                    + "==> " + Bytes.toString(CellUtil.cloneFamily(cell))
                    + "{" + Bytes.toString(CellUtil.cloneQualifier(cell))
                    + ":" + Bytes.toString(CellUtil.cloneValue(cell)) + "}");
        }
        table.close();
    }

    @Test
    public void Scan() throws Exception {

/*        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes("rowkey_1"));
        scan.setStopRow(Bytes.toBytes("rowkey_2"));*/
        //定义表的名称
        TableName tableName = TableName.valueOf("pc:forum_webpage");
        //获取表
        Table table = conn.getTable(tableName);
        //全表扫描
        Scan scan = new Scan();
        //获取扫描结果
        ResultScanner scanner = table.getScanner(scan);
        scanner.forEach(result -> {
            System.out.println("行键"+Bytes.toString(result.getRow()));
            show(result);
        });
/*        Result result = null;
        //迭代数据
        while ((result = scanner.next()) != null) {
            //打印数据获取所有单元格
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                //打印rowKey,family,qualifier,value
                System.out.println(Bytes.toString(CellUtil.cloneRow(cell))
                        + "==> " + Bytes.toString(CellUtil.cloneFamily(cell))
                        + "{" + Bytes.toString(CellUtil.cloneQualifier(cell))
                        + ":" + Bytes.toString(CellUtil.cloneValue(cell)) + "}"
                );
            }
        }*/
        table.close();
    }

    @Test
    public void Delete() throws Exception {
        Admin admin = conn.getAdmin();
        TableName tableName = TableName.valueOf("pc:briup_webpage");
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        admin.close();
    }

    public void show(Result result) {
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();
        map.forEach((liezu, map2) -> {
            map2.forEach((lie, map3) -> {
                map3.forEach((t, v) -> {
                    System.out.println(
                            "列族:" + Bytes.toString(liezu) +
                                    "列名:" + Bytes.toString(lie) +
                                    "时间戳:" + t +
                                    "值:" + Bytes.toString(v)
                    );
                });
            });
        });

        map.forEach((cf, map2) -> {
            map2.forEach((q, map3) -> {
                map3.forEach((t, v) -> {
                    System.out.println("列族:" + Bytes.toString(cf) + ",列名:" + Bytes.toString(q) + ",时间戳:" + t + ",值:" + Bytes.toString(v));
                });
            });
        });
    }

    @Before
    public void before() throws Exception {
        //1.获取HBaseConfiguration
        Configuration conf = HBaseConfiguration.create();
        //hbase集群zookeeper地址
        conf.set("hbase.zookeeper.quorum", "fake:2181");
        //2.获取连接对象
        conn = ConnectionFactory.createConnection(conf);
    }

    @After
    public void after() throws Exception {
        if (conn != null) {
            conn.close();
        }
    }
}
