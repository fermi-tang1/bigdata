package utils;

import com.google.common.collect.Lists;
import model.ColumnParam;
import model.Student;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @Author: Fermi.Tang
 * @Date: Created in 16:22,2022/7/22
 */
public class HBaseUtils {

    private static Logger logger = LogManager.getLogger(HBaseUtils.class);


    private static Configuration conf = HBaseConfiguration.create();
    private static ExecutorService pool = Executors.newScheduledThreadPool(20);
    private static Connection connection = null;
    private static HBaseUtils instance = null;
    private static Admin admin = null;

    private HBaseUtils() {
        if (connection == null) {
            try {
                conf.set("hbase.zookeeper.quorum", "myhbase");
                conf.set("hbase.zookeeper.property.clientPort", "2181");
                conf.set("hbase.rootdir", "/hbase");
                connection = ConnectionFactory.createConnection(conf, pool);
                admin = connection.getAdmin();
            } catch (IOException e) {
                logger.error("Failed to init HbaseUtils：" + e.getMessage(), e);
            }
        }
    }

    public static synchronized HBaseUtils getInstance() {
        if (instance == null) {
            instance = new HBaseUtils();
        }
        return instance;
    }

    public boolean isTableExist(String tableName) throws Exception {
        boolean exists = admin.tableExists(TableName.valueOf(tableName));
        return exists;
    }

    public void createTable(String tableName, String... columnFamily) throws Exception {
        if (this.isTableExist(tableName)) {
            logger.warn("Table already exists！");
            return;
        }
        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String family : columnFamily) {
            descriptor.addFamily(new HColumnDescriptor(family));
        }
        admin.createTable(descriptor);
        admin.close();
    }

    public void deleteTable(String tableName) throws IOException {
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
        admin.close();
    }

    public void addData(String tableName, Student student) throws Exception {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(student.getRowKey()));
        for (ColumnParam columnParam : student.getColumnParams()) {
            put.addColumn(Bytes.toBytes(columnParam.getColumnFamily()), Bytes.toBytes(columnParam.getQualifier()), Bytes.toBytes(columnParam.getValue()));
        }
        table.put(put);
        table.close();
    }

    public Result queryOne(String tableName, String rowKey) throws Exception {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);
        return result;
    }

    public void deleteOne(String tableName, String rowKey) throws Exception {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
    }


    public static void main(String[] args) {
        HBaseUtils hBaseUtils = HBaseUtils.getInstance();
        try {
            String tableName = "student";
            hBaseUtils.createTable(tableName, "name", "info", "score");
            Student student = Student.builder()
                .rowKey("1")
                .columnParams(Lists.newArrayList(
                    ColumnParam.builder()
                        .columnFamily("name")
                        .qualifier("")
                        .value("Fermi Tang")
                        .build(),
                    ColumnParam.builder()
                        .columnFamily("info")
                        .qualifier("student_id")
                        .value("G20220735040059")
                        .build(),
                    ColumnParam.builder()
                        .columnFamily("info")
                        .qualifier("class")
                        .value("bigdata1")
                        .build(),
                    ColumnParam.builder()
                        .columnFamily("score")
                        .qualifier("understanding")
                        .value("95")
                        .build(),
                    ColumnParam.builder()
                        .columnFamily("info")
                        .qualifier("programming")
                        .value("90")
                        .build()))
                .build();
            hBaseUtils.addData(tableName, student);
            logger.info(hBaseUtils.queryOne(tableName, "1"));
            hBaseUtils.deleteOne(tableName, "1");
        } catch (Exception e) {
            logger.error("Failed to handling hbase: " + e.getMessage());
        }
    }


}
