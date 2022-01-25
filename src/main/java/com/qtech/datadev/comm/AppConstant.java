package com.qtech.datadev.comm;

/**
 *Project : StdModelForBi
 *Author  : zhilin.gao
 *Date    : 2021/12/21 14:38
 */


public class AppConstant {

    public final static String ORACLE_JDBC_DRIVER = "oracle.jdbc.driver.OracleDriver";
    public final static String ORACLE_JDBC_URL = "jdbc:oracle:thin:@//10.170.3.85:1521/QTGAOZL";
    public final static String ORACLE_USER = "gaozhilin";
    public final static String ORACLE_PASSWORD = "ee786549";
    public final static String ORACLE_TABLE_STDMDBI = "STDMODEL_BI";

    public static final String IMPALA_JDBC_DRIVER = "com.cloudera.impala.jdbc41.Driver";
    public static final String IMPALA_CONNECTION_URL = "jdbc:impala://10.170.3.15:21050/ods_machine_extract;UseSasl=0;AuthMech=3;UID=qtkj;PWD=;characterEncoding=utf-8";

}
