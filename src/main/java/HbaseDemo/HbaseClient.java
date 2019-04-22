package HbaseDemo;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseClient {
    public static void main(String[] args) throws Exception {
        System.out.println(exits("test2"));
        createtable("test2","cf1","cf2","cf3");
    }

    //全局连接对象
    public static Connection connection;


    //静态代码块
    static{
        //获取连接
        HBaseConfiguration conf = new HBaseConfiguration();
        //设置参数
        conf.set("hbase.zookeeper.quorum","192.168.222.114");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        //获取连接对象
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    //判断表是否存在
    public static boolean exits(String table) throws Exception{
        //
        Admin admin = connection.getAdmin();
        //判断表是否存在
        boolean b = admin.tableExists(TableName.valueOf(table));
        return b;
    }

    //创建表
    public static void createtable(String table,String...cf) throws Exception{
        Admin admin = connection.getAdmin();
        HTableDescriptor hTableDescriptor = new HTableDescriptor(table);
        //遍历
        for (String s : cf) {
            hTableDescriptor.addFamily(new HColumnDescriptor(s));
        }
        //创建
        admin.createTable(hTableDescriptor);
        System.out.println("创建成功！");

    }

    //删除表
    public static void delete(String table) throws Exception{
        Admin admin = connection.getAdmin();
        if (exits(table)){
            //下线
            admin.disableTableAsync(TableName.valueOf(table));
            //删除
            admin.deleteTable(TableName.valueOf(table));
            System.out.println("删除成功");
        }else{
            System.out.println("表不存在");
        }
    }

    //添加数据
    public static void addtable(String table,String rowkey,String columnfamily,String column,String value) throws Exception{
        //获取连接(表)
        Table table1 = connection.getTable(TableName.valueOf(table));
        //new Put
        Put put = new Put(Bytes.toBytes(rowkey));
        //添加
        put.add(Bytes.toBytes(columnfamily),Bytes.toBytes(column),Bytes.toBytes(value));

        table1.put(put);
        System.out.println("添加成功");
    }

    //查看scan
    public static void scan(String table) throws Exception{
        //获取连接(表)
        Table table1 = connection.getTable(TableName.valueOf(table));
        Scan scan = new Scan();
        ResultScanner results = table1.getScanner(scan);
        for (Result result:results){
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println(cell.getFamilyArray().toString());
                System.out.println(cell.getRow().toString());
                System.out.println(cell.getQualifier().toString());
                System.out.println(cell.getValue().toString());
            }
        }
    }

    //get 获取
    public static void get(String table) throws IOException {
        //获取连接(表)
        Table table1 = connection.getTable(TableName.valueOf(table));
        Get get = new Get(Bytes.toBytes(table));
        Result result= table1.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println(cell.getFamilyArray().toString());
            System.out.println(cell.getRow().toString());
            System.out.println(cell.getQualifier().toString());
            System.out.println(cell.getValue().toString());
        }
    }
}
