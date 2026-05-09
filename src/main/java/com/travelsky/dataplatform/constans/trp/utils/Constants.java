package com.travelsky.dataplatform.constans.trp.utils;

public class Constants {

    /**
     * 航空公司
     */
    public static final String AIR_LINE = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.AIR_COMPANY);
    public static final String AIR_LINE_LOWER = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.AIR_LINE_LOWER);
    /**
     * hdfs运行时用户
     */
    public static final String HADOOP_USER_NAME = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.HADOOP_USER_NAME);
    /**
     * 主机.z文件使用的编码
     */
    public static final String CHARSET = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.CHARSET);
    /**
     * 低并行度
     */
    public static final int PARALLELISM = Integer.parseInt(PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.PARALLELISM));
    /**
     * 高并行度
     */
    public static final int HIGHPARALLELISM = Integer.parseInt(PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.HIGHPARALLELISM));

    /**
     * 数据处理的测试目录(/TEMP/DATA)false和真实存储目录(/DATA)true切换
     */
    public static final boolean FINISH_MODE = Boolean.parseBoolean(PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.FINISH_MODE));
    /**
     * HBase刷写数据量阈值
     */
    public static final int HBASE_FLUSH_NUM = Integer.parseInt(PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.HBASE_FLUSH_NUM));
    /**
     * 盐，用于混交md5
     */
    public static final String SLAT = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.SLAT);
    /**
     * zookeeper地址
     */
    public static final String ZK_SERVER = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.ZK_SERVER);
    /**
     * zookeeper端口
     */
    public static final String ZK_PORT = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.ZK_PORT);
    /**
     * HDFS地址
     */
    public static final String HDFS_SERVER = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.HDFS_SERVER);


    public static final int HBASE_RPC_TIMEOUT = Integer.parseInt(PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.HBASE_RPC_TIMEOUT));
    public static final int HBASE_CLIENT_OPERATION_TIMEOUT = Integer.parseInt(PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.HBASE_CLIENT_OPERATION_TIMEOUT));
    public static final int HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD = Integer.parseInt(PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD));


    public static final String CODEC_BZIP2 = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.CODEC_BZIP2);
    public static final String CODEC_LZ4 = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.CODEC_LZ4);
    public static final String CODEC_DEFLATE = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.CODEC_DEFLATE);
    public static final String CODEC_SNAPPY = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.CODEC_SNAPPY);
    public static final String CODEC_GZIP = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.CODEC_GZIP);
    public static final String CODEC_LZO = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.CODEC_LZO);
    public static final String CODEC_BROTLI = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.CODEC_BROTLI);
    public static final String CODEC_ZSTARNDARD = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.CODEC_ZSTARNDARD);

//HBase相关
    /**
     * 主键
     */
    public static final String HBASE_TABLE_COLUMN_ROWKEY = "ROWKEY";
    /**
     * 列簇
     */
    public static final String HBASE_TABLE_COLUMN_FAMILY = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.HBASE_INDEX_TABLE_FAMILY);
    /**
     * HBase表命名空间
     */
    public static final String HBASE_NAMESPACE = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.HBASE_NAMESPACE);
    /**
     * 订单search表：（order_search）
     */
    public static final String HBASE_TABLE_ORDER_SEARCH = HBASE_NAMESPACE + ":" + AIR_LINE_LOWER + "_" + PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.HBASE_TABLE_ORDER_SEARCH);
    /**
     * 订单报文表：(order_message)
     */
    public static final String HBASE_TABLE_ORDER_SPNR = HBASE_NAMESPACE + ":" + AIR_LINE_LOWER + "_" + PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.HBASE_TABLE_ORDER_SPNR);
    /**
     * 订单报文获取记录表（order_fetch_log）
     */
    public static final String HBASE_TABLE_ORDER_FETCH_LOG = HBASE_NAMESPACE + ":" + AIR_LINE_LOWER + "_" + PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.HBASE_TABLE_ORDER_FETCH_LOG);
    /**
     * 作业运行记录表（run_log）
     */
    public static final String HBASE_TABLE_ORDER_RUN_LOG = HBASE_NAMESPACE + ":" + AIR_LINE_LOWER + "_" + PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.HBASE_TABLE_ORDER_RUN_LOG);
    /**
     * 迁移数据统计表（data_log）
     */
    public static final String HBASE_TABLE_ORDER_DATA_LOG = HBASE_NAMESPACE + ":" + AIR_LINE_LOWER + "_" + PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.HBASE_TABLE_ORDER_DATA_LOG);
    /**
     * 记录存在问题的spnrid
     */
    public static final String HBASE_TABLE_ORDER_SPNRID_MISSING = HBASE_NAMESPACE + ":" + AIR_LINE_LOWER + "_" + "spnr_missing";
    /**
     * 告警
     */
    public static final String ARK_MONITOR_EVENT_SERVICE_URL = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.ARK_MONITOR_EVENT_SERVICE_URL);
    /**
     * 获取spnr报文接口
     */
    public static final String GET_MESSAGE_URL = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.GET_MESSAGE_URL);
    public static final String GET_MESSAGE_URL_BIG = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.GET_MESSAGE_URL_BIG);
    public static final String GET_MESSAGE_URL_BIG_TK = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.GET_MESSAGE_URL_BIG_TK);


    public static final String HTTP_CLIENT_CONF = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.HTTP_CLIENT_CONF);
    public static final String HTTP_CLIENT_CODE = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.HTTP_CLIENT_CODE);


    /**
     * 数据目录
     */
    public static final String ORDER_HISTORY_PATH = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.ORDER_HISTORY_PATH);
    /**
     * 操作代码
     */
    public static final String EXTRACT = "extract";
    public static final String FILESAVE = "filesave";
    public static final String LOAD_SEARCH = "load";
    public static final String LOAD_FETCH = "loadFetch";
    public static final String SPNR_SERVICE = "spnrService";
    public static final String SUMMARY_SERVICE = "summaryService";
    public static final String DELETE = "delete";
    /**----------------数值类型定义-----------------*/
    /**
     * 耗时时间秒
     */
    public static final double S_TO_MS_ELAPSED = 1000.0;

    /**
     * 一秒包含毫秒
     */
    public static final int S_TO_MS = 1000;

    /**
     * 一分钟包含秒
     */
    public static final int M_TO_S = 60;

    /**
     * 一小时包含分钟
     */
    public static final int H_TO_M = 60;

    /**
     * 10分钟
     */
    public static final int MI_10 = 10;
    /**
     * 20分钟
     */
    public static final int MI_20 = 20;

    /**
     * 一天包含小时
     */
    public static final int D_TO_H = 24;
    /**
     * spnr数据存储hdfs滚动小时数(6小时)
     */
    public static final int H_6 = 6;
    /**
     * 2小时包含秒(标签处理)
     */
    public static final int H2_TO_S = 7200;

    /**
     * 飞行分钟
     */
    public static final int M_TO_MS = 60000;

    /**
     * 每隔10s进行启动一个检查点【设置checkpoint的周期】
     */
    public static final long CHECKPOINTING = 120000L;

    /**
     * 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
     */
    public static final long MIN_PAUSE_BETWEEN_CHECKPOINTS = 5000L;

    /**
     * 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
     */
    public static final long CHECKPOINT_TIMEOUT = 90000L;

    /**
     * 缓存数量
     */
    public static final int BUFFER_SIZE_1024 = 1024;
    public static final int BUFFER_SIZE_4 = 4;
    public static final int BUFFER_SIZE_128 = 128;


    /**
     * 请求缓存
     */
    public static final int HBASE_CACHING = 1000;
    public static final int HBASE_PARTITION = 10000;

    /**
     * newHashMapWithExpectedSize
     */
    public static final int NEW_HASHMAP_WITH_EXPECTED_SIZE = 8;

    /**
     * pauseSecond
     */
    public static final int PAUSE_SECOND = 5;

    /**
     * ByteBuffer
     */
    public static final int BYTE_BUFFER_SIZE_8 = 8;

    /**
     * mobile length
     */
    public static final int MOBILE_LENGTH = 11;

    /**
     * MD5 size
     */
    public static final int MD5_SIZE = 32;
    public static final int MD5_SIZE_20 = 20;


    /**
     * keytab 文件
     */
    public static final String KEYTAB = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.KEYTAB);

    /**
     * krb5 文件
     */
    public static final String KRB5 = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.KRB5);
    public static final String SLASH = "/";


    /**
     * 编码
     */
    public static final String ENCODING = "utf-8";
    /**
     * Hbase 客户端检索超时时间
     */
    public static final int HBASE_CLIENT_SCANNER_TIMEOUT = 180000;
    /**
     * 登录用户
     */
    public static final String LOGIN_USER = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.LOGIN_USER);

    public static final String GET_SUMMARY_MESSAGE_HEAD = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.GET_SUMMARY_MESSAGE_HEAD);
    public static final String GET_SUMMARY_MESSAGE_END = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.GET_SUMMARY_MESSAGE_END);
    public static final String GET_SPNR_MESSAGE_HEAD = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.GET_SPNR_MESSAGE_HEAD);
    public static final String GET_SPNR_MESSAGE_END = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.GET_SPNR_MESSAGE_END);

    /**
     * 加密秘钥
     */
    public static final String CRYPTO_MASTERKEY_NAME_CONF_KEY = "erf@1455r";

    /**
     * 副本数量
     * 数据的冗余在底层是由hdfs控制的都是三副本机制
     */
    public static final int REGION_REPLICATION_SIZE = 1;
    /**
     * 设置文件分割大小1024 * 1024 * 1024L;
     */
    public static final Long REGION_SIZE = 1024 * 1024 * 1024L;
    /**
     * 线程池最大检索线程数量
     */
    public static final int THREAD_MAX = 100;
    /**
     * 报文获取最大重试次数
     */
    public static final int MAX_RETRIES = 5;

    public static final long GROUP_BOUNDARY = 100L;

    public static final String SEARCH = "/SEARCH/";
    public static final String SEARCH_UNIQUE = "SEARCH_UNIQUE/";
    public static final String ITINERARY = "/ITINERARY/";

    // 数据分片
    public static final String ITINERARY_0 = "/ITINERARY_0/";
    public static final String ITINERARY_1 = "/ITINERARY_1/";

    // DBC
    public static final String ORACLE_DRIVER = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.ORACLE_DRIVER);
    public static final String ORACLE_URL = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.ORACLE_URL);
    public static final String ORACLE_USERNAME = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.ORACLE_USERNAME);
    //    #No need to modify - belongs to the variable name
    public static final String ORACLE_PASSWORD = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.ORACLE_PASSWORD);


    // 分片一
    public static final String ORACLE_SHARDING1_URL = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.ORACLE_SHARDING1_URL);
    public static final String ORACLE_SHARDING1_USERNAME = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.ORACLE_SHARDING1_USERNAME);
    //    #No need to modify - belongs to the variable name
    public static final String ORACLE_SHARDING1_PASSWORD = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.ORACLE_SHARDING1_PASSWORD);

    // 分片二
    public static final String ORACLE_SHARDING2_URL = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.ORACLE_SHARDING2_URL);
    public static final String ORACLE_SHARDING2_USERNAME = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.ORACLE_SHARDING2_USERNAME);
    //    #No need to modify - belongs to the variable name
    public static final String ORACLE_SHARDING2_PASSWORD = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.ORACLE_SHARDING2_PASSWORD);

    // search库
    public static final String ORACLE_SEARCH_URL = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.ORACLE_SEARCH_URL);
    public static final String ORACLE_SEARCH_USERNAME = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.ORACLE_SEARCH_USERNAME);
    public static final String ORACLE_SEARCH_PASSWORD = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.ORACLE_SEARCH_PASSWORD);


    public static final String HDFS_FLODER = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.ORDER_HISTORY_PATH);

    public static final Integer NUMBER_OF_RETRIES = Integer.valueOf(PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.NUMBER_OF_RETRIES));

    public static final String PATTERN = "yyyyMMdd";
    public static final String TIME_PATTERN = "yyyyMMddHHmmss";
    public static final String DATA_PATTERN = "yyyy-MM-dd HH:mm:ss";
    public static final String YYYY_MM_DD = "yyyy-MM-dd";
    public static final String RUN_LOG_PATTERN = "yyyyMMddHHmmssSSS";

    public static final String ERROR_CODE_E001 = "E001";
    public static final String ERROR_CODE_E002 = "E001";
    public static final String ERROR_CODE_E003 = "E001";
    public static final String ERROR_CODE_E004 = "E001";
    public static final String ERROR_MSG_E001 = "FTP下载文件失败。";
    public static final String ERROR_MSG_E002 = "FDM层处理异常。";
    public static final String ERROR_MSG_E003 = "亲爱的用户${0},你好，上次登录时间为${1}";
    public static final String ERROR_MSG_E004 = "FTP文件不存在。";
    public static final String ERROR_MSG_E005 = "重置日期错误。";
    public static final String ERROR_MSG_E006 = "标签字段值为空";
    public static final String ERROR_MSG_E007 = "刷新写入Hbase";
    public static final String ERROR_MSG_E008 = "没有访问权限";
    public static final String ERROR_MSG_E009 = "写入失败";
    public static final String ERROR_MSG_E010 = "XML.toString(Document): ";

    public static final String SUMMARY_FLAG = "summaryFlag";
    public static final String SPNR_FLAG = "spnrFlag";
    public static final String ORDER_CREATE_DATE = "orderCreateDate";
    public static final String GET_XML_FAIL_MARK = "2";
    public static final String GET_XML_SUCCESS_MARK = "1";

    public static final String NEW_SPNRID_MARK = "0";
    public static final String SUMMARY_FETCH_FAIL = "summaryFetchFail";
    public static final String SPNR_FETCH_FAIL = "spnrFetchFail";
    public static final String BROADCAST_MAP_NAME = "broadCastMapName";
    public static final String GET_SPNR_MESSAGE_COUNT = "getSpnrMessageCount";
    public static final String GET_SUMMARY_MESSAGE_COUNT = "getSummaryMessageCount";
    public static final String CONTINUOUS_FAILURE_THRESHOLD = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.CONTINUOUS_FAILURE_THRESHOLD);
    public static final String GET_SUMMARY_MESSAGE_NUMBER_OF_RETRIES = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.GET_SUMMARY_MESSAGE_NUMBER_OF_RETRIES);
    public static final String GET_SPNR_MESSAGE_NUMBER_OF_RETRIES = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.GET_SPNR_MESSAGE_NUMBER_OF_RETRIES);

    public static final String TOTAL_FAILURE_THRESHOLD = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.TOTAL_FAILURE_THRESHOLD);

    public static final String THREE_SPACES = "   ";
    public static final String PROFILE = "PROFILE";
    public static final String DELIMITER = "|";
    public static final String MULTIPLE_DATA_SEPARATOR = ",";
    public static final String PREFIX_MAP_ITINERARY = "ITINERARY_";
    public static final String MAP_SPNRID = "SPNRID";
    public static final String ALREADY_WRITE_COL = "add_time";
    public static final String FETCHL_OG_QUALIFIER_FILTER = "spnr_flag";
    public static final String RESERVATIONNUM = "RESERVATIONNUM";
    public static final String SPNR_XML = "spnrXml";
    public static final String SUMMARY_XML = "summaryXml";
    public static final String SERVICE_IP = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.SERVICE_IP);

    public static final String SPNR_XML_LOG_003 = "<Success/>";
    public static final String SPNR_XML_LOG_004 = "<Errors>";
    public static final String SPNR_XML_LOG_006 = "OJ_SuperPNR";
    public static final String SPNR_XML_LOG_007 = "ota:OJ_SuperPNR";
    public static final String SPNR_XML_LOG_011 = "dataDate";

    public static final String NULL = "null";
    public static final String COMMA = ",";
    public static final String TYPENAME = "typeName:";
    public static final String STRING = "String";
    public static final String STRING_CLASS_PATH = "java.lang.String";
    public static final String LONG = "long";
    public static final String LONG_CLASS_PATH = "java.lang.Long";
    public static final String INT = "int";
    public static final String INTEGER = "Integer:";
    public static final String INTEGER_CLASS_PATH = "java.lang.Integer";
    public static final String DOUBLE = "double";
    public static final String DOUBLE_CLASS_PATH = "java.lang.Double";
    public static final String UTF = "UTF-8";
    public static final String COMPRESSION = "compression";
    public static final String GZIP = "1";
    public static final String SNAPPY = "2";
    public static final String LZ4 = "3";
    public static final String LZO = "4";

    public static final String CHARACTER = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.CHARACTER);

    public static final Integer GET_SPNR_TIMEOUT = Integer.valueOf(PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.GET_SPNR_TIMEOUT));
    public static final String PRODUCTION = PropertiesUtil.getInstance().getValueByKey(PropertiesConstants.PRODUCTION);
    ;
    public static final int MEMORY_SIZE = 50 * 1024 * 1024;

    public static final int HBASE_CELL_LIMIT = 10485760;

    // DBS对比的字段
    public static final String RESERVATIONDATE = "RESERVATIONDATE";
    public static final String RESERVATIONSTART = "RESERVATIONSTART";
    public static final String RESERVATIONEND = "RESERVATIONEND";
    public static final String SHOPID = "SHOPID";
    public static final String LASTAUDITID = "LASTAUDITID";
    public static final String RESERVATIONSTATUS = "RESERVATIONSTATUS";

    // DBS分片标记
    public static final  String ITINERARY_FRAGMENTNUM ="ITINERARY_FRAGMENTNUM";
    public static final String SEARCH_SEPARATOR="|";

    // 性别编码：男
    public static String M = "M";
    // 性别编码：女
    public static String F = "F";
    // 性别编码：未知
    public static String U = "U";

    // 直销用户状态DIRECT_USER_STATUS 冻结2
    public static String DIRECT_USER_STATUS_2 = "2";
    // 直销用户状态DIRECT_USER_STATUS 正常1
    public static String DIRECT_USER_STATUS_1 = "1";
    // 直销用户状态DIRECT_USER_STATUS 注销0
    public static String DIRECT_USER_STATUS_0 = "0";

}

