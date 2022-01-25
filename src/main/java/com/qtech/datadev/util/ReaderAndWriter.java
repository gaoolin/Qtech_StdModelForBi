package com.qtech.datadev.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import static com.qtech.datadev.comm.AppConstant.*;
import static org.apache.commons.lang.StringUtils.join;
import static org.apache.commons.lang.StringUtils.repeat;
import static org.apache.spark.sql.types.DataTypes.createStructField;

/**
 *Project : StdModelForBi
 *Author  : zhilin.gao
 *Date    : 2021/12/21 14:59
 */


public class ReaderAndWriter {

    public static Configuration getConf() {

        System.setProperty("HADOOP_USER_NAME", "zcgx");

        Configuration conf = new Configuration();

        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("fs.hdfs.impl.disable.cache", "ture");
        conf.set("fs.defaultFs", "hdfs://nameservice");
        conf.set("dfs.nameservices", "nameservices");
        conf.set("dfs.ha.namenodes.nameservice", "bigdata01, bigdata02");
        conf.set("dfs.namenode.rpc-address.nameservice.bigdata01", "10.170.3.11:8020");
        conf.set("dfs.namenode.rpc-address.nameservice.bigdata02", "10.170.3.12:8020");
        conf.set("dfs.client.failover.proxy.provider.nameservice", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

        return  conf;
    }


    public static void  write(String time, String path) throws IOException {

        Calendar cal = Calendar.getInstance();

        cal.add(Calendar.MINUTE, 0);

        FileSystem fileSystem =  FileSystem.get(getConf());

        try {
            OutputStream output = fileSystem.create(new Path(path));

            output.write(time.getBytes(StandardCharsets.UTF_8));

            fileSystem.close();

        } catch (Exception e) {

            e.printStackTrace();
        }
    }


    public static String read(String readPath, JavaSparkContext sc) {

        String lines = sc.textFile(readPath).rdd().first();

        System.out.println(lines);
        return lines;
    }


    public static Dataset<Row> readFromCsv(String path, SparkSession ss) throws IOException {

        FileSystem fileSystem = FileSystem.get(getConf());

        Dataset<Row> df = ss.read().format("csv")
                .option("header", "true")
                .option("mode", "FAILFAST")
                .option("inferSchema", "true")
                .load(path);

        return df;
    }


    public static Dataset<Row> readFromKudu(String kudumaster, SparkSession ss, String tablename, String selectStr, String filter) {

        Dataset<Row> getDF = ss.read().format("kudu").option("kudu.master", kudumaster)
                .option("kudu.table", tablename)
                .load();

        getDF.createOrReplaceTempView(tablename);

        String filterSql = selectStr + " " + tablename + " " + filter;

        return ss.sql(filterSql);
    }

    public static Dataset<Row> readFromKudu(String kuduMaster, SparkSession ss, String tableName, String selectStr) {

        String filterStr = "";

        return readFromKudu(kuduMaster, ss, tableName, selectStr, filterStr);
    }

    public static Dataset<Row> readFromKudu(String kuduMaster, SparkSession ss, String tableName) {

        String selectStr = "SELECT * FROM";
        String filterStr = "";

        return readFromKudu(kuduMaster, ss, tableName, selectStr);
    }


    public static Dataset<Row> readFromDbaseJDBC(String tablename, String selectstr, String filterstr, SparkSession ss, String jdbcdriver, String connectionurl) throws ClassNotFoundException, SQLException {

        Class.forName(jdbcdriver);
        Connection conn = DriverManager.getConnection(connectionurl);

        Dataset<Row> df = null;
        try {
            Statement statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            ResultSet rs = statement.executeQuery(selectstr + " " + tablename + " " + filterstr);

            List<Row> rows = new ArrayList<>();

            ArrayList<String> lines = new ArrayList<>();

            ArrayList<String> colNames = new ArrayList<>();

            ResultSetMetaData rsmetaData = rs.getMetaData();

            int columnCount = rsmetaData.getColumnCount();

            while (rs.next()) {

                lines.clear();

                for (int i = 1; i <= columnCount; i++) {
                    lines.add(rs.getString(i));
                }

                // 略慢 800
//           rows.add(new GenericRow(lines.toArray()));

                // 略快 561
                rows.add(RowFactory.create(lines.toArray()));
            }

            for (int i = 1; i <= columnCount; i++) {

                colNames.add(rsmetaData.getColumnName(i));
            }

            StructType structType = new StructType();

            for (String colName : colNames)
                structType = structType.add(createStructField(colName, DataTypes.StringType, false));

            df = ss.createDataFrame(rows, structType);

        } catch (Exception e) {

            e.printStackTrace();

        } finally {

            conn.close();
        }

        return df;
    }

    public static Dataset<Row> readFromKuduJDBC(String tablename, String selectstr, String filterstr, SparkSession ss) throws SQLException, ClassNotFoundException {

        String jdbcdriver = "com.cloudera.impala.jdbc41.Driver";
        String connectionurl = "jdbc:impala://10.170.3.15:21050;UseSasl=0;AuthMech=3;UID=qtkj;PWD=;";

        return readFromDbaseJDBC(tablename, selectstr, filterstr, ss, jdbcdriver, connectionurl);
    }

    public static Dataset<Row> readFromKuduJDBC(String tablename, String filterstr, SparkSession ss) throws SQLException, ClassNotFoundException {

        String selectStr = "SELECT * FROM";

        return readFromKuduJDBC(tablename, selectStr, filterstr, ss);
    }

    public static Dataset<Row> readFromKuduJDBC(String tablename, SparkSession ss) throws SQLException, ClassNotFoundException {

        String filterStr = "";

        return readFromKuduJDBC(tablename, filterStr, ss);
    }


    public static void write2DBaseJDBC(String tableName, @NotNull Dataset<Row> df, String JDBC_DRIVER, String CONNECTION_URL) throws ClassNotFoundException, SQLException {

        Class.forName(JDBC_DRIVER);
        Connection conn = DriverManager.getConnection(CONNECTION_URL, ORACLE_USER, ORACLE_PASSWORD);

        String[] columns = df.columns();
        int length = columns.length;

        PreparedStatement pstm = conn.prepareStatement(String.format("INSERT INTO %s(%s) values(%s)", tableName, join(columns, ","), join(repeat("?/", length).split("/"), ",")));

        int count = 0;

//       int j = 0;

        conn.setAutoCommit(false);

        try {

            for (Row line : df.collectAsList()) {

                count ++;

                for (int i = 0; i < length; i++) {

                    if (i == -1) {  // 不需此执行模块的时候可以设置为负数。

                        pstm.setInt(i + 1, Integer.parseInt(line.getString(i)));  // Integer.valueOf(line.getString(i))
                    } else {

                        pstm.setString(i + 1, String.valueOf(line.get(i)));
                    }
                }
                // Oracel 10G的JDBC Driver限制最大Batch size是16383条，如果addBatch超过这个限制，那么executeBatch时就会出现“无效的批值”（Invalid Batch Value） 异常。因此在如果使用的是
                // Oracle10G，在此bug减少前，Batch size需要控制在一定的限度。

                pstm.addBatch();

                // 500批作为一组执行效率较好
                if (count % 500 == 0) {

//                   j ++;
//                   System.out.println(j);
                    int[] res = pstm.executeBatch();
                    conn.commit();
                    pstm.clearBatch();
                }
            }
            int[] res = pstm.executeBatch();
            conn.commit();
            pstm.clearBatch();

        } catch (SQLException throwables) {

            try {
                conn.rollback();

            } catch (SQLException e) {

                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            throwables.printStackTrace();
        }
        finally {

            try {

                conn.setAutoCommit(true);
                conn.close();

            } catch (SQLException e) {

                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }


    public static void write2DBaseJDBC(String tableName, @NotNull Dataset<Row> df) throws SQLException, ClassNotFoundException {

        write2DBaseJDBC(tableName, df, IMPALA_JDBC_DRIVER, IMPALA_CONNECTION_URL);
    }


    public static void delDaByJDBC(String tableName, String filterStr, String JDBC_DRIVER, String JDBC_URL) throws ClassNotFoundException, SQLException {

        Class.forName(JDBC_DRIVER);

        Connection conn = DriverManager.getConnection(JDBC_URL, ORACLE_USER, ORACLE_PASSWORD);

        PreparedStatement prep = conn.prepareStatement(String.format("DELETE FROM %s %s", tableName, filterStr));

        prep.execute();

        conn.close();
    }

    public static void delDaByJDBC(String tableName, String JDBC_DRIVER, String JDBC_URL) throws SQLException, ClassNotFoundException {
        String filterStr = "";
        delDaByJDBC(tableName, filterStr, JDBC_DRIVER, JDBC_URL);
    }

}
