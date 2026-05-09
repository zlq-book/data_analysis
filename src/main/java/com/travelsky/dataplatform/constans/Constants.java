package com.travelsky.dataplatform.constans;

import com.travelsky.dataplatform.utils.SM4Utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.exception.NacosException;

/**
 * @author
 * @date 2025/9/27 10:40
 */
public class Constants {
    // 国航接口
    public static String CA_CRM_INTERFACE;
    public static String CA_CRM_KEY;
    public static String CA_CRM_CHANNEL;
    public static String CA_CRM_SUBCHANNEL;
    public static String CA_CRM_SC_SUB_CHANNEL;
    public static String CA_CRM_SC_CHANNEL;

    public static long CHECKPOINTING;
    public static long MIN_PAUSE_BETWEEN_CHECKPOINTS;
    public static long CHECKPOINT_TIMEOUT;
    public static String SM4_KEY;
    public static String ODS_USER;
    public static String DWD_USER;
    public static String DIM_USER;
    public static String ODS_PWD;
    public static String DWD_PWD;
    public static String DIM_PWD;
    public static String ODS_DB;
    public static String DWD_DB;
    public static String DIM_DB;
    public static String DWQ_DB;
    public static String DORIS_IP;
    public static String DORIS_PORT;
    public static String DORIS_FE_IP;
    public static String DORIS_FE_PORT;
    public static String DORIS_BE_IP;
    public static String DORIS_BE_PORT;
    public static Integer DORIS_FLIGHT_PORT;
    public static String FE_URL;
    public static String BE_URL;
    public static String DORIS_JDBC_URL;
    public static String DORIS_BATCH_SIZE;
    public static String DORIS_READ_TIMEOUT;

    // 达梦
    public static String DM_IP;
    public static String DM_PORT;
    public static String DM_JDBC_URL;
    public static String DM_SCHEMA;
    public static String DM_DRIVER;
    public static String DM_PWD;
    public static String DM_USER;

    public static String WXAP_AES_KEY;
    public static String WXAP_USER;
    public static String WXAP_PWD;
    public static String WXAP_DB;
    public static String WXAP_IP;
    public static String WXAP_PORT;
    public static String WXAP_SCHEMA;
    public static String ALP_AES_KEY;
    public static String ALP_USER;
    public static String ALP_PWD;
    public static String ALP_DB;
    public static String ALP_IP;
    public static String ALP_PORT;
    public static String ALP_SCHEMA;
    public static String HYTD_AES_KEY;
    public static String HYTD_USER;
    public static String HYTD_PWD;
    public static String HYTD_DB;
    public static String HYTD_IP;
    public static String HYTD_PORT;
    public static String HYTD_SCHEMA;
    public static String ORACLE_DRIVER;

    public static String ZXYH_AES_KEY;
    public static String ZXYH_USER;
    public static String ZXYH_PWD;
    public static String ZXYH_DB;
    public static String ZXYH_IP;
    public static String ZXYH_PORT;
    public static String ZXYH_SCHEMA;

    public static String CLK_AES_KEY;
    public static String CLK_USER;
    public static String CLK_PWD;
    public static String CLK_DB;
    public static String CLK_IP;
    public static String CLK_PORT;
    public static String CLK_SCHEMA;
    public static String CLK_FILE_PATH;
    public static String CLK_FILE_FTP_PATH;
    public static String GROUPING_FILE_PATH;
    public static Integer GROUPING_CSV_SIZE;

    public static String CLK_DW_USER;
    public static String CLK_DW_PWD;
    public static String CLK_DW_DB;
    public static String CLK_DW_IP;
    public static String CLK_DW_PORT;
    public static String CLK_DW_SCHEMA;
    public static String CLK_ODS_SCHEMA;

    // 常旅客SFTP--国航里程账单数据
    public static String CLK_MILE_FTP_HOST;
    public static String CLK_MILE_FTP_USERNAME;
    public static String CLK_MILE_FTP_PWD;
    public static String CLK_MILE_FTP_PORT;

    // 常旅客SFTP--业务人员上传文件
    public static String CLK_BUSINESS_FTP_HOST;
    public static String CLK_BUSINESS_FTP_USERNAME;
    public static String CLK_BUSINESS_FTP_PWD;
    public static String CLK_BUSINESS_FTP_PORT;

    // TRP-FTP
    public static String TRP_FTP_HOST;
    public static String TRP_FTP_USERNAME;
    public static String TRP_FTP_PWD;
    public static String TRP_FTP_PORT;
    public static String TRP_FTP_DOWNLOAD_PATH;
    public static String TRP_AES_KEY;

    // 国际销售数据FTP
    public static String INTL_SALES_FTP_HOST;
    public static String INTL_SALES_FTP_USERNAME;
    public static String INTL_SALES_FTP_PWD;
    public static String INTL_SALES_FTP_PORT;
    public static String INTL_SALES_FTP_PATH;

    public static String TRP_FTP_HDFS;
    public static String YXW_FTP_HDFS;
    public static String CLK_FTP_HDFS;
    public static String RATE_FTP_HDFS;

    public static String HSD_KAFKA_SERVICES;
    public static String HSD_KAFKA_USERNAME;
    public static String HSD_KAFKA_PASSWORD;
    public static String HSD_KAFKA_TOPIC;
    public static String HSD_KAFKA_GROUPID;
    public static String HSD_KAFKA_JAAS_CLAZZ;
    public static String HSD_KAFKA_SECURITY_PROTOCOL;
    public static String HSD_KAFKA_SASLME_CHANISM;
    public static Integer HSD_KAFKA_PARALLELISM;

    public static String LYX_AES_KEY;
    public static String LYX_USER;
    public static String LYX_PWD;
    public static String LYX_DB;
    public static String LYX_IP;
    public static String LYX_PORT;
    public static String LYX_SCHEMA;

    public static String ZSF_AES_KEY;
    public static String ZSF_USER;
    public static String ZSF_PWD;
    public static String ZSF_DB;
    public static String ZSF_IP;
    public static String ZSF_PORT;
    public static String ZSF_SCHEMA;
    public static String MYSQL_DRIVER;

    // Variflight API Configuration
    public static String VARIFLIGHT_APPSECURITY;
    public static String VARIFLIGHT_BASE_URL;
    public static String VARIFLIGHT_APP_ID;

    public static String LYGJ_AES_KEY;
    public static String LYGJ_IP;
    public static String LYGJ_SCHEMA;
    public static String LYGJ_USER;
    public static String LYGJ_PWD;
    public static String LYGJ_PORT;
    public static String LYGJ_DB;

    // 机上升舱&预购升舱
    public static String JSSC_AES_KEY;
    public static String JSSC_IP;
    public static String JSSC_SCHEMA;
    public static String JSSC_USER;
    public static String JSSC_PWD;
    public static String JSSC_PORT;
    public static String JSSC_DB;
    // 鲁雁管家灾备
    public static String LYGJ_ZB_AES_KEY;
    public static String LYGJ_ZB_IP;
    public static String LYGJ_ZB_SCHEMA;
    public static String LYGJ_ZB_USER;
    public static String LYGJ_ZB_PWD;
    public static String LYGJ_ZB_PORT;
    public static String LYGJ_ZB_DB;
    // 大客户管理系统
    public static String DKHXT_AES_KEY;
    public static String DKHXT_IP;
    public static String DKHXT_SCHEMA;
    public static String DKHXT_USER;
    public static String DKHXT_PWD;
    public static String DKHXT_PORT;
    public static String DKHXT_DB;
    // 客服白屏系统
    public static String KHBP_AES_KEY;
    public static String KHBP_IP;
    public static String KHBP_SCHEMA;
    public static String KHBP_USER;
    public static String KHBP_PWD;
    public static String KHBP_PORT;
    public static String KHBP_DB;
    // 大客户差旅
    public static String DKHCL_AES_KEY_AIR_ORDER_PASSENGER;
    public static String DKHCL_AES_KEY_CRM_EMPLOYEE;
    public static String DKHCL_AES_KEY_CRM_EMPLOYEE_CERT;
    public static String DKHCL_IP;
    public static String DKHCL_SCHEMA;
    public static String DKHCL_USER;
    public static String DKHCL_PWD;
    public static String DKHCL_PORT;
    public static String DKHCL_DB;
    // 企微私域
    public static String QWSY_AES_KEY;
    public static String QWSY_IP;
    public static String QWSY_SCHEMA;
    public static String QWSY_USER;
    public static String QWSY_PWD;
    public static String QWSY_PORT;
    public static String QWSY_DB;
    //TRP
    public static String TRP_KAFKA_SERVICES;
    public static String TRP_KAFKA_USERNAME;
    public static String TRP_KAFKA_PASSWORD;
    public static String TRP_KAFKA_TOPIC;
    public static String TRP_KAFKA_GROUPID;

    // 登机口升舱
    public static String DJKSC_AES_KEY;
    public static String DJKSC_USER;
    public static String DJKSC_PWD;
    public static String DJKSC_DB;
    public static String DJKSC_IP;
    public static String DJKSC_PORT;
    public static String DJKSC_SCHEMA;

    // 标准数据系统
    public static String BZSJ_USER;
    public static String BZSJ_PWD;
    public static String BZSJ_DB;
    public static String BZSJ_IP;
    public static String BZSJ_PORT;
    public static String BZSJ_SCHEMA;

    // 数据采集与分析系统
    public static String SDEXT_USER;
    public static String SDEXT_PWD;
    public static String SDEXT_DB;
    public static String SDEXT_IP;
    public static String SDEXT_PORT;
    public static String SDEXT_SCHEMA;

    //上游kafka
    public static String PRIMARY_KAFKA_BOOTSTRAP_SERVERS;
    public static String PRIMARY_KAFKA_GROUPID;
    public static String PRIMARY_KAFKA_TOPICS;
    public static String PRIMARY_KAFKA_USERNAME;
    public static String PRIMARY_KAFKA_PASSWORD;
    public static String PRIMARY_KAFKA_JAAS_CLAZZ;
    public static String PRIMARY_KAFKA_SECURITY_PROTOCOL;
    public static String PRIMARY_KAFKA_SASLME_CHANISM;
    //移动云kafka
    public static String KAFKA_BOOTSTRAP_SERVERS;
    public static String KAFKA_SPNR_PARQUET_GROUP;
    public static String KAFKA_CONSUME_TOPIC;
    public static String KAFKA_USERNAME;
    public static String KAFKA_PASSWORD;
    public static String KAFKA_JAAS_CLAZZ;
    public static String KAFKA_SECURITY_PROTOCOL;
    public static String KAFKA_SASLME_CHANISM;
    public static String KRB5_CONF_PATH;
    public static String SYS_UCV_TEST_FOR_MAPREDUCE_KEYTAB_PATH;
    public static String SYS_UCV_TEST_FOR_MAPREDUCE_PRINCIPAL;

    public static String CHECKPOINT_STORAGE;

    // 用户分群csv
    public static String USERGROUP_KAFKA_GROUPID;
    public static String USERGROUP_KAFKA_TOPICS;

    //高频事件
    public static String EVENT_BOOK = "Book";
    public static String EVENT_CANCEL = "Cancel";
    public static String EVENT_ACTIONCODECHANGE = "ActionCodeChange";
    public static String EVENT_ASRCHANGE = "AsrChange";
    public static String EVENT_SSRCHANGE = "SSRChange";
    public static String EVENT_OSICHANGE = "OSIChange";
    public static String EVENT_RMKCHANGE = "RMKChange";
    public static String EVENT_SPLIT = "Split";
    public static String EVENT_PASSENGERPROTECTION = "PassengerProtection";
    public static String EVENT_REVALIDATION = "Revalidation";
    public static String SUBEVENT_FLIGHTCHANGE = "FlightChange";
    public static String SUBEVENT_AIRPORTCHANGE = "AirportChange";
    public static String EVENT_ISSUE = "Issue";
    public static String EVENT_VT = "VT";
    public static String EVENT_TRFD = "TRFD";
    public static String EVENT_EXCHANGE = "Exchange";
    public static String SUBEVENT_REISSUE = "ReIssue";
    public static String SUBEVENT_EXCHANGED = "Exchanged";
    public static String EVENT_SUSPEND = "Suspend";
    public static String EVENT_DESUSPEND = "DeSuspend";
    public static String EVENT_TICKETCHECKIN = "TicketCheckIn";
    public static String SUBEVENT_TICKETGOSHOW = "TicketGoShow";
    public static String EVENT_FLOWN = "Flown";
    public static String EVENT_TICKETCHECKOUT = "TicketCheckOut";
    public static String EVENT_PRINTITINERARY = "PrintItinerary";
    public static String EVENT_ETRF = "ETRF";
    public static String EVENT_IRRCHANGE = "IRRChange";
    public static String EVENT_VOID2OPEN = "Void2Open";
    public static String EVENT_EXCHANGE2OPEN = "Exchange2Open";
    public static String EVENT_FIMEXCHANGE2OPEN = "FIMExchange2Open";
    public static String EVENT_USED2OPEN = "Used2Open";
    public static String EVENT_CABINCHANGE = "CabinChange";
    public static String EVENT_CANCELPRINTITINERARY = "CancelPrintItinerary";
    public static String EVENT_TKTSEGDURCHANGE = "TktSegDurChange";
    public static String EVENT_BOARDED2OPEN = "Boarded2Open";
    public static String EVENT_NAMECHANGE = "NameChange";
    public static String EVENT_IDCHANGE = "IDChange";
    public static String EVENT_EMDISSUE = "EMDIssue";
    public static String EVENT_EMDREFUND = "EMDRefund";
    public static String EVENT_EMDCHECKIN = "EMDCheckIn";
    public static String EVENT_EMDBOARD = "EMDBoard";
    public static String EVENT_EMDVOID = "EMDVOID";
    public static String EVENT_EMDFLOWN = "EMDFlown";
    public static String EVENT_CHECKIN = "CheckIn";
    public static String EVENT_CHECKOUT = "CheckOut";
    public static String EVENT_BAGCHANGE = "BagChange";
    public static String EVENT_UPGRADE = "Upgrade";
    public static String EVENT_DOWNGRADE = "Downgrade";
    public static String EVENT_SEATCHANGE = "SeatChange";
    public static String EVENT_STANDBY = "STANDBY";
    public static String EVENT_REPRINTBOARDINGCARD = "ReprintBoardingCard";
    public static String EVENT_ZFLIGHTCHANGE = "ZFlightChange";
    public static String EVENT_FFCHANGE = "FFChange";
    public static String EVENT_PSGIF = "PsgIF";
    public static String EVENT_CKISSRCHANGE = "CKISSRChange";
    public static String EVENT_BOARDED = "Boarded";
    public static String EVENT_NOSHOW = "Noshow";
    public static Set<String> HSD_EVENTS = new HashSet<>(Arrays.asList(
            "Book", "Cancel", "ActionCodeChange", "AsrChange", "SSRChange",
            "OSIChange", "Split", "Issue", "Revalidation", "VT", "TRFD",
            "Exchange", "Suspend", "DeSuspend", "TicketCheckIn", "Flown",
            "TicketCheckOut", "ETRF", "IRRChange", "Void2Open", "Exchange2Open",
            "FIMExchange2Open", "Used2Open", "CabinChange", "TktSegDurChange",
            "Boarded2Open", "NameChange", "IDChange", "EMDIssue", "EMDRefund",
            "EMDCheckIn", "EMDBoard", "EMDVOID", "EMDFlown", "CheckIn",
            "CheckOut", "BagChange", "FFChange", "Boarded", "Noshow"
    ));
    //唯一性证件类型
    public static List<String> UNIQUE_CERTIFICATION = Arrays.asList("NI","RP","TC","OF","CW","CS","VC","NC","SE","WP","WS","PR");

    // 汇率FTP服务器配置值字段（非final，用于存储从配置文件加载的实际值）
    public static String EXCHANGE_RATE_FTP_HOST;
    public static String EXCHANGE_RATE_FTP_PORT;
    public static String EXCHANGE_RATE_FTP_USERNAME;
    public static String EXCHANGE_RATE_FTP_PWD;
    public static String EXCHANGE_RATE_FTP_DOWNLOAD_PATH;

    public static String CRM_MEMBER_INFO_QUERY_NEW_URL;
    public static String CRM_QUERY_CARD_URL;
    public static String CRM_QUERY_MEMBER_EXT_URL;




    // 静态初始化块，用于加载配置文件
    static {
        loadConfiguration();
    }

    /**
     * 加载配置文件
     */
    private static void loadConfiguration() {
        ConfigService configService = null;
        try {
            Properties props = new Properties();
            try (InputStream input = Constants.class.getClassLoader().getResourceAsStream("application.properties")) {
                if (input == null) {
                    System.err.println("Unable to find local application.properties file");
                    return;
                }
                props.load(input);
            } catch (IOException e) {
                System.err.println("load config failed:" + e.getMessage());
                return;
            }
            String configRoad = props.getProperty("config.route","local");
            // 环境变量中读取
            String configRoadEnv = System.getenv("CONFIGROUTE");
            if (configRoadEnv != null && !configRoadEnv.isEmpty()) {
                configRoad = configRoadEnv;
            }
            String configfile = props.getProperty("config.filename");
            // 环境变量中读取
            String configfileEnv = System.getenv("CONFIGFILENAME");
            if (configfileEnv != null && !configfileEnv.isEmpty()) {
                configfile = configfileEnv;
            }
            System.out.println("========Config route:" + configRoad+", file:"+configfile+"========");
            String configInfoEncKey = props.getProperty("config.infoenckey");
            String b64configInfoEncKey = SM4Utils.hexToBase64(configInfoEncKey + "cd5cb8d01c");
            if ("nacos".equalsIgnoreCase(configRoad)) {
                // Nacos配置信息
                // Nacos服务器信息加密密钥
                // Nacos服务器地址
                String serverAddr = props.getProperty("nacos.server-addr");
                // 用户名
                String username = props.getProperty("nacos.username");
                // 密码
                String password = props.getProperty("nacos.password");
                // 命名空间
                String namespace = props.getProperty("nacos.namespace");
                // 配置文件的dataId
                String dataId = configfile;
                // 配置文件的group
                String group = props.getProperty("nacos.group");
                // 超时时间
                String timeoutmsstr = props.getProperty("nacos.timeout-ms", "20000");
                // 连接超时时间
                String connecttimeout = props.getProperty("nacos.connect-timeout","3000");
                // 读取超时时间
                String readtimeout = props.getProperty("nacos.read-timeout","10000");

                long timeoutMs = timeoutmsstr==null?20000:Long.parseLong(timeoutmsstr);
                System.out.println("Nacos configuration: nacosInfoEncKey:"+b64configInfoEncKey+", serverAddr:"+serverAddr+",username:"+username+",password:"+password+",namespace:"+namespace+",dataId:"+dataId+",group:"+group+",timeoutMs:"+timeoutMs);
                String b64password = SM4Utils.hexToBase64(password);
                password=SM4Utils.decrypt(b64password,b64configInfoEncKey);
//            System.out.println("Nacos configuration: nacosInfoEncKey:"+b64configInfoEncKey+", serverAddr:"+serverAddr+",username:"+username+",password:"+password+",namespace:"+namespace+",dataId:"+dataId+",group:"+group+",timeoutMs:"+timeoutMs);

                // 创建Nacos配置服务
                Properties properties = new Properties();
                properties.put("serverAddr", serverAddr);
                properties.put("username", username);
                properties.put("password", password);
                properties.put("connectTimeout", connecttimeout);
                properties.put("readTimeout", readtimeout);

                // 如果命名空间不为空，则设置命名空间
                if (namespace != null && !namespace.isEmpty()) {
                    properties.put("namespace", namespace);
                }

                configService = NacosFactory.createConfigService(properties);

                // 从Nacos获取配置
                String content = configService.getConfig(dataId, group, timeoutMs);
                if (content == null || content.isEmpty()) {
                    System.err.println("Unable to get config from Nacos, dataId: " + dataId);
                    return;
                }
//            System.out.println("content:"+content);
                // 解析配置内容
                props.clear();
                props.load(new StringReader(content));
            } else {
                try (InputStream inputProp = Constants.class.getClassLoader().getResourceAsStream(configfile)) {
                    if (inputProp == null) {
                        System.err.println("Unable to find local "+configfile+" file");
                        return;
                    }
                    props.clear();
                    props.load(inputProp);
                } catch (IOException e) {
                    System.err.println("load config "+configfile+" failed:" + e.getMessage());
                    return;
                }
            }

            // 使用反射方式设置属性值
            Field[] fields = Constants.class.getDeclaredFields();
            for (Field field : fields) {
                // 只处理非final的静态字段
                if (Modifier.isStatic(field.getModifiers()) &&
                    !Modifier.isFinal(field.getModifiers())) {
                    String fieldName = field.getName();
                    String propertyValue = props.getProperty(fieldName);
                    if ((fieldName.endsWith("_PWD") || fieldName.endsWith("_PASSWORD")
                    || fieldName.equals("SM4_KEY") || fieldName.equals("TRP_AES_KEY")
                    || fieldName.equals("CA_CRM_KEY"))
                    && propertyValue != null && propertyValue.length() > 0) {
                        propertyValue = SM4Utils.decrypt(SM4Utils.hexToBase64(propertyValue), b64configInfoEncKey);
                    }

                    if (propertyValue != null) {
                        try {
                            field.setAccessible(true);
                            Class<?> fieldType = field.getType();

                            if (fieldType == String.class) {
                                field.set(null, propertyValue);
                            } else if (fieldType == long.class || fieldType == Long.class) {
                                field.set(null, Long.valueOf(propertyValue));
                            } else if (fieldType == int.class || fieldType == Integer.class) {
                                field.set(null, Integer.valueOf(propertyValue));
                            }
                        } catch (Exception e) {
                            System.err.println("Error setting field " + fieldName + ": " + e.getMessage());
                        }
                    }
                }
            }
            props.clear();
            props = null;

            // 特殊处理需要根据其他属性计算的字段
            FE_URL = DORIS_FE_IP + ":" + DORIS_FE_PORT;
            BE_URL = DORIS_BE_IP + ":" + DORIS_BE_PORT;
            DORIS_JDBC_URL = "jdbc:mysql://" + DORIS_IP + ":" + DORIS_PORT + "?serverTimezone=Asia/Shanghai&useTimezone=true&useServerPrepStmts=true&useLocalSessionState=true&rewriteBatchedStatements=true&cachePrepStmts=true&prepStmtCacheSqlLimit=99999&prepStmtCacheSize=500";
            DM_JDBC_URL = "jdbc:dm://" + DM_IP + ":" + DM_PORT + "/" + DM_SCHEMA + "?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true&serverTimezone=Asia/Shanghai";
        } catch (NacosException e) {
            System.err.println("Nacos error: " + e.getMessage());
            throw new RuntimeException(e);
        } catch (IOException e) {
            System.err.println("Error loading config from Nacos: " + e.getMessage());
            throw new RuntimeException(e);
        } catch (Exception e) {
            System.err.println("Error initializing constants: " + e.getMessage());
            throw new RuntimeException(e);
        } finally {
            // 关闭配置服务
            if (configService != null) {
                try {
                    System.out.println("Nacos configService shutDown");
                    configService.shutDown();
                } catch (NacosException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private static void showConfiguration() {
        try {
            // 使用反射方式获取属性值
            Field[] fields = Constants.class.getDeclaredFields();
            for (Field field : fields) {
                // 只处理非final的静态字段
                if (Modifier.isStatic(field.getModifiers()) &&
                        !Modifier.isFinal(field.getModifiers())) {
                    String fieldName = field.getName();
                    field.setAccessible(true);
                    Object fieldValue = field.get(null);
                    System.out.println(fieldName + " = " + fieldValue);
                }
            }
        } catch (Exception e) {
            System.err.println("Error showConfiguration constants: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        showConfiguration();
    }
}
