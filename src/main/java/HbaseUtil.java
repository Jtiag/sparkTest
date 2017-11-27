import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by xubo5 on 2017/10/16.
 */
public class HbaseUtil {
    private static Configuration configuration;
    private static Connection connection;
    private static Admin admin;
    private static String hbaseIp = "cdhmanager,cdhslave2,cdhslave3";

    public static void main(String[] args) throws IOException {
//        listTables();
//        scanData("BLUETOOTH_DATA_GOME", "gome10000000018293_1506195849146", "gome10000000019858_150781865");
//        String[] cls = {"row1","cf1","value"};
//        createTable("ts11",cls);
        scanData("BLUETOOTH_DATA_GOME",null,null);
    }

    /**
     * 初始化链接
     */
    public static void init() {
        configuration = HBaseConfiguration.create();
        /**
         * 300 环境
         */
//        configuration.set(HConstants.ZOOKEEPER_QUORUM,"hadooptest01,hadooptest02,hadooptest03");
//        configuration.set(HConstants.ZOOKEEPER_ZNODE_PARENT,"/hbase-unsecure");

        /**
         * 800 环境
//         */
        configuration.set(HConstants.ZOOKEEPER_QUORUM, hbaseIp);
        configuration.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase");

        configuration.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //关闭连接
    public static void close() {
        try {
            if (null != admin) {
                admin.close();
            }
            if (null != connection) {
                connection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    //建表
    public static void createTable(String tableNmae, String[] cols) throws IOException {
        init();
        TableName tableName = TableName.valueOf(tableNmae);
        if (admin.tableExists(tableName)) {
            System.out.println("talbe is exists!");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            for (String col : cols) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
        }
        close();
    }

    //获取所以列名
    public static void getCloumnName(String tablename) throws IOException {
        init();
        TableName tableName = TableName.valueOf(tablename);
        if (!admin.tableExists(tableName)) {
            System.out.println("talbe is not exists!");
        } else {
            HColumnDescriptor[] columnFamilies = admin.getTableDescriptor(tableName)
                    .getColumnFamilies();// 获取所有的列名
            ArrayList<String> columnsName = new ArrayList<>();
            for (HColumnDescriptor cName : columnFamilies) {
                String name = Bytes.toString(cName.getName());
                System.out.println("cloumn name:" + name);
                columnsName.add(name);
            }
        }
        close();
    }

    //删表
    public static void deleteTable(String tableName) throws IOException {
        init();
        TableName tn = TableName.valueOf(tableName);
        if (admin.tableExists(tn)) {
            admin.disableTable(tn);
            admin.deleteTable(tn);
        }
        close();
    }

    //查看已有表
    public static void listTables() throws IOException {
        init();
        HTableDescriptor[] hTableDescriptors = admin.listTables();
        for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
            System.out.println(hTableDescriptor.getNameAsString());
        }
        close();
    }

    //插入数据
    public static void insterRow(String tableName, String rowkey, String colFamily, String col, String val) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(val));
        table.put(put);

        //批量插入
       /* List<Put> putList = new ArrayList<Put>();
        puts.add(put);
        table.put(putList);*/
        table.close();
        close();
    }

    //删除数据
    public static void deleRow(String tableName, String rowkey, String colFamily, String col) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        //删除指定列族
        //delete.addFamily(Bytes.toBytes(colFamily));
        //删除指定列
        //delete.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col));
        table.delete(delete);
        //批量删除
       /* List<Delete> deleteList = new ArrayList<Delete>();
        deleteList.add(delete);
        table.delete(deleteList);*/
        table.close();
        close();
    }

    //根据rowkey查找数据
    public static void getData(String tableName, String rowkey, String colFamily, String col) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowkey));
        //获取指定列族数据
        get.addFamily(Bytes.toBytes(colFamily));
        //获取指定列数据
        //get.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col));
        Result result = table.get(get);
        showCell(result);
        table.close();
        close();
    }

    //格式化输出
    public static void showCell(Result result) {
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("rowKey:" + new String(CellUtil.cloneRow(cell)) + " ");
            System.out.println("timestamp:" + cell.getTimestamp() + " ");
            System.out.println("column Family:" + new String(CellUtil.cloneFamily(cell)) + " ");
            System.out.println("qualifier:" + new String(CellUtil.cloneQualifier(cell)) + " ");
            System.out.println("value:" + new String(CellUtil.cloneValue(cell)) + " ");
        }
    }

    //批量查找数据
    public static void scanData(String tableName, String startRow, String stopRow) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
//        scan.setStartRow(Bytes.toBytes(startRow));
//        scan.setStopRow(Bytes.toBytes(stopRow));
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            showCell(result);
        }
        table.close();
        close();
    }

    public static void selectRowKey(String tableName, String rowKey) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        Result rs = table.get(get);
        Cell[] cells = rs.rawCells();
        for (Cell cell : cells) {
            System.out.println(Arrays.toString(cell.getRowArray()));
            System.out.println(Arrays.toString(cell.getFamilyArray()));
        }
        for (KeyValue kv : rs.raw()) {
            System.out.println("--------------------" + new String(kv.getRow()) + "----------------------------");
            System.out.println("Column Family: " + new String(kv.getFamily()));
            System.out.println("Column       :" + new String(kv.getQualifier()) + "t");
            System.out.println("value        : " + new String(kv.getValue()));
        }
    }

    //根据rowKey 批量查找数据
    public static Long scanDataByRowKeyFilter(String tableName, String rowKey) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        //scan.setStopRow(Bytes.toBytes(stopRow));
        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                new SubstringComparator(rowKey));//保护包含rowKey的行
        scan.setFilter(filter);
        ResultScanner resultScanner = table.getScanner(scan);
        long count = 0L;
        for (Result result : resultScanner) {
            count += result.size();
        }
        table.close();
        close();
        return count;
    }
}
