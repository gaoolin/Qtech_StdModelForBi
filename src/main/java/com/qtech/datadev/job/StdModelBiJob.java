package com.qtech.datadev.job;


import com.qtech.datadev.util.ReaderAndWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Properties;

import static com.qtech.datadev.comm.AppConstant.*;

/**
 *Project : StdModelForBi
 *Author  : zhilin.gao
 *Date    : 2021/12/21 11:55
 */


public class StdModelBiJob {

    private static final Logger logger = Logger.getLogger(StdModelBiJob.class);
    public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException {

        logger.setLevel(Level.INFO);
        Properties properties = new Properties();
        InputStream inputStream = StdModelBiJob.class.getClassLoader().getResourceAsStream("StdModelBi.properties");
        properties.load(inputStream);

        String kuduMaster = properties.getProperty("kuduMaster");
        String stdModelPath = properties.getProperty("stdModelPath");


        SparkSession spark = SparkSession.builder()
                .appName("Standard Model For BI")
                // .master("local[*]")
                .getOrCreate();

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        jsc.setLogLevel("error");

        Configuration hadoopConfig = jsc.hadoopConfiguration();
        hadoopConfig.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        hadoopConfig.set("fs.hdfs.impl.disable.cache", "true");
        hadoopConfig.set("fs.defaultFS", "hdfs://nameservice");
        hadoopConfig.set("dfs.nameservices", "nameservice");
        hadoopConfig.set("dfs.ha.namenodes.nameservice", "bigdata01,bigdata02");
        hadoopConfig.set("dfs.namenode.rpc-address.nameservice.bigdata01", "10.170.3.11:8020");
        hadoopConfig.set("dfs.namenode.rpc-address.nameservice.bigdata02", "10.170.3.12:8020");
        hadoopConfig.set("dfs.client.failover.proxy.provider.nameservice", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");


        /**
         * read data from HDFS csv
         */

        Dataset<Row> stdMod = spark.read().format("csv")
                .option("header", "true")
                .option("mode", "FAILFAST")
                .option("inferSchema", "true")
                .load(stdModelPath);

        // stdMod.show();

       // spark.read().format("jdbc")
       //         .option("driver", ORACLE_JDBC_DRIVER)
       //         .option("url", ORACLE_JDBC_URL)
       //         .option("dbtable", String.format("(SELECT * FROM %s) t", ORACLE_TABLE_STDMDBI))
       //         .load()
       //         .createOrReplaceTempView(ORACLE_TABLE_STDMDBI);
       //
       //
       // Dataset<Row> stdModDF = spark.sql(String.format("SELECT * FROM %s", ORACLE_TABLE_STDMDBI));

        ReaderAndWriter.delDaByJDBC(ORACLE_TABLE_STDMDBI, ORACLE_JDBC_DRIVER, ORACLE_JDBC_URL);
        logger.info(">>> Truncate table done.");

        ReaderAndWriter.write2DBaseJDBC(ORACLE_TABLE_STDMDBI, stdMod, ORACLE_JDBC_DRIVER, ORACLE_JDBC_URL);
        logger.info(">>> Insert into table done.");

    }
}
