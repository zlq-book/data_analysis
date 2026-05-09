package com.travelsky.dataplatform.constans.trp.utils;

/**
 * 变量定义
 *
 * @author zhangyu
 */
public class PropertiesConstants {

    /**
     * properties文件
     */
    public static final String FILEPATH = "/system.properties";
    /**
     * 航空公司
     */
    public static final String AIR_COMPANY = "air.company";
    /**
     * 航空公司小写
     */
    public static final String AIR_LINE_LOWER = "air.line.lower";
    /**
     * hdfs运行时用户
     */
    public static final String HADOOP_USER_NAME = "hadoop.user.name";
    /**
     * 主机.z文件使用的编码
     */
    public static final String CHARSET = "charset";
    /**
     * 低并行度
     */
    public static final String PARALLELISM = "parallelism";
    /**
     * 高并行度
     */
    public static final String HIGHPARALLELISM = "highparallelism";

    /**
     * 数据处理的测试目录(/temp/data)false和真实存储目录(/data)true切换
     */
    public static final String FINISH_MODE = "finish.mode";
    /**
     * hbase刷写数据量阈值
     */
    public static final String HBASE_FLUSH_NUM = "hbase.flush.num";
    /**
     * 盐，用于混交md5
     */
    public static final String SLAT = "slat";
    /**
     * zookeeper地址
     */
    public static final String ZK_SERVER = "zk.server";
    /**
     * zookeeper端口
     */
    public static final String ZK_PORT = "zk.port";
    /**
     * hdfs地址
     */
    public static final String HDFS_SERVER = "hdfs.server";
    public static final String HBASE_INDEX_TABLE_FAMILY = "hbase.index.table.family";

    public static final String HBASE_RPC_TIMEOUT = "hbase.rpc.timeout";
    public static final String HBASE_CLIENT_OPERATION_TIMEOUT = "hbase.client.operation.timeout";
    public static final String HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD = "hbase.client.scanner.timeout.period";


    public static final String CODEC_BZIP2 = "codec.bzip2";
    public static final String CODEC_LZ4 = "codec.lz4";
    public static final String CODEC_DEFLATE = "codec.deflate";
    public static final String CODEC_SNAPPY = "codec.snappy";
    public static final String CODEC_GZIP = "codec.gzip";
    public static final String CODEC_LZO = "codec.lzo";
    public static final String CODEC_BROTLI = "codec.brotli";
    public static final String CODEC_ZSTARNDARD = "codec.zstarndard";


    public static final String HBASE_NAMESPACE = "hbase.namespace";
    public static final String HBASE_TABLE_ORDER_SEARCH = "hbase.table.order.search";
    public static final String HBASE_TABLE_ORDER_SPNR = "hbase.table.order.spnr";
    public static final String HBASE_TABLE_ORDER_FETCH_LOG = "hbase.table.order.fetch.log";
    public static final String HBASE_TABLE_ORDER_RUN_LOG = "hbase.table.order.run.log";
    public static final String HBASE_TABLE_ORDER_DATA_LOG = "hbase.table.order.data.log";

    public static final String ARK_MONITOR_EVENT_SERVICE_URL = "ark.monitor.event.service.url";
    public static final String HTTP_CLIENT_CONF = "http.client.conf";
    public static final String HTTP_CLIENT_CODE = "http.client.code";
    public static final String KEYTAB = "keytab";

    public static final String KRB5 = "krb5";
    public static final String LOGIN_USER = "login.user";
    public static final String GET_SUMMARY_MESSAGE_HEAD = "get.summary.message.head";
    public static final String GET_SUMMARY_MESSAGE_END = "get.summary.message.end";
    public static final String GET_SPNR_MESSAGE_HEAD = "get.spnr.message.head";
    public static final String GET_SPNR_MESSAGE_END = "get.spnr.message.end";
    public static final String GET_MESSAGE_URL = "get.spnr.message.url";
    public static final String GET_MESSAGE_URL_BIG = "get.spnr.message.url.big";
    public static final String GET_MESSAGE_URL_BIG_TK = "get.spnr.message.url.big.tk";
    public static final String ORDER_HISTORY_PATH = "order.history.path";
    public static final String CONTINUOUS_FAILURE_THRESHOLD = "continuous.failure.threshold";
    public static final String TOTAL_FAILURE_THRESHOLD = "total.failure.threshold";

    public static final String NUMBER_OF_RETRIES = "number.of.retries";

    // DBC
    public static final String ORACLE_DRIVER = "oracle.driver";
    public static final String ORACLE_URL = "oracle.url";
    public static final String ORACLE_USERNAME = "oracle.username";
    public static final String ORACLE_PASSWORD = "oracle.password";

    // 分片库一
    public static final String ORACLE_SHARDING1_URL = "oracle.sharding1.url";
    public static final String ORACLE_SHARDING1_USERNAME = "oracle.sharding1.username";
    public static final String ORACLE_SHARDING1_PASSWORD = "oracle.sharding1.password";

    // 分片库二
    public static final String ORACLE_SHARDING2_URL = "oracle.sharding2.url";
    public static final String ORACLE_SHARDING2_USERNAME = "oracle.sharding2.username";
    public static final String ORACLE_SHARDING2_PASSWORD = "oracle.sharding2.password";

    // search库
    public static final String ORACLE_SEARCH_URL = "oracle.search.url";
    public static final String ORACLE_SEARCH_USERNAME = "oracle.search.username";
    public static final String ORACLE_SEARCH_PASSWORD = "oracle.search.password";

    public static final String SERVICE_IP = "service.ip";
    public static final String CHARACTER = "character";
    public static final String GET_SPNR_TIMEOUT = "get.spnr.timeout";
    public static final String GET_SUMMARY_MESSAGE_NUMBER_OF_RETRIES = "get.summary.message.number.of.retries";
    public static final String GET_SPNR_MESSAGE_NUMBER_OF_RETRIES = "get.spnr.message.number.of.retries";
    public static final String PRODUCTION ="http.client.conf" ;
}
