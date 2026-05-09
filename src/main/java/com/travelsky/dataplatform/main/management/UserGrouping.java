package com.travelsky.dataplatform.main.management;

import com.google.common.collect.Lists;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.trp.utils.DateUtil;
import com.travelsky.dataplatform.utils.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * 用户分群 csv 文件处理
 *
 * @author rz
 * @since 2025/11/11
 **/
public class UserGrouping {

    static final Logger logger = LoggerFactory.getLogger(UserGrouping.class);
    // 新增或修改标志为0
    private static final String ADD_UPDATE = "0";
    // csv敏感字段加密的路径后缀
    private static final String ENCRYPTED = "encrypted";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "UserGroupingKafkaSource");
        env.setParallelism(1);
        // SASL认证配置（实际应从安全存储获取）
        String jaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                "username=\"" + Constants.KAFKA_USERNAME + "\" " + "password=\"" + Constants.KAFKA_PASSWORD + "\";";

        // 构建 Kafka Source
        KafkaSource<ConsumerRecord<String, String>> kafkaSource = KafkaSource.<ConsumerRecord<String, String>>builder()
                .setBootstrapServers(Constants.KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(Constants.USERGROUP_KAFKA_TOPICS)
                .setGroupId(Constants.USERGROUP_KAFKA_GROUPID)
                .setStartingOffsets(OffsetsInitializer.latest())// 从最新的偏移量开始消费
                // 只获取 value，使用 StringDeserializer，如果需要使用key的话需要自定义反序列化器
                .setDeserializer(new KafkaRecordDeserializationSchema<ConsumerRecord<String, String>>() {
                    @Override
                    public TypeInformation<ConsumerRecord<String, String>> getProducedType() {
                        return TypeInformation.of(new TypeHint<ConsumerRecord<String, String>>() {
                        });
                    }
                    @Override
                    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<ConsumerRecord<String, String>> collector) throws IOException {
                        String key = consumerRecord.key() == null ? null : new String(consumerRecord.key());
                        String value = consumerRecord.value() == null ? null : new String(consumerRecord.value());
                        collector.collect(new ConsumerRecord<>(consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), consumerRecord.timestamp(), consumerRecord.timestampType(), consumerRecord.checksum(), consumerRecord.serializedKeySize(), consumerRecord.serializedValueSize(), key, value));
                    }
                })
                .setProperty("security.protocol", Constants.KAFKA_SECURITY_PROTOCOL)
                .setProperty("sasl.mechanism", "SCRAM-SHA-256")
                .setProperty("sasl.jaas.config", jaasConfig)
                .setProperty("session.timeout.ms", "1200000")
                .setProperty("fetch.wait.max.ms", "100000")
                //自动提交偏移量
                .setProperty("enable.auto.commit", "true")
                .setProperty("auto.commit.interval.ms", "5000")
                .build();

        // 启动流式任务，打印 value
        SingleOutputStreamOperator<String> kafka_source = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "User Group Kafka Source")
                .map(record -> record.value());
        kafka_source.print();

        // 处理Kafka消息，生成csv，更新数据库
        dealGroupCsv(kafka_source);

        env.execute("Usergrouping");
    }

    /**
     * 处理Kafka消息
     *
     * @param kafka_source
     */
    private static void dealGroupCsv(SingleOutputStreamOperator<String> kafka_source) {

        kafka_source.map((MapFunction<String, Object>) s -> {
            // addOrUpdate_groupingId_groupingHistoryId_date_true (最后一个可能没有)
            // 0_1000151_1000000377_2025-11-11_true
            logger.info("处理用户自定义分群csv消息，接收到消息：{}", s);
            System.out.println("处理用户自定义分群csv消息，接收到消息:" + s);
            String[] array = s.split("_");
            if (array.length < 2) {
                return "消息[" + s + "]不正确";
            }
            String addOrUpdate = array[0];
            String groupingId = array[1];
            String groupingHistoryId = array[2];
            String date = array[3];
            // 是否需要全域旅客标签字段
            // TODO 下载全域标签字段csv的接口，发送Kafka消息时要多一个字段，之后完善
            boolean allFields = false;
            if (array.length == 5) {
                allFields = Boolean.parseBoolean(array[4]);
            }

            // 新增或更新分群，需要更新 groups 字段，写 csv 文件
            if (ADD_UPDATE.equals(addOrUpdate)) {
                // 根据groupingHistoryId从Doris库GROUPING_RECORD表查询到tid
                List<String> tids = queryTids(groupingHistoryId);
                if (CollectionUtil.isNullOrEmpty(tids)) {
                    return "tid size = 0";
                }
                // 数量太大就不用生成了，几百万级别的csv下载了也无用
                if (tids.size() > Constants.GROUPING_CSV_SIZE) {
                    return "tid size limited";
                }

                // csv路径前缀：hdfs://cmss/data/test/encrypt/user_grouping/1000000377_2025-11-11
                String csvPathPre = Constants.GROUPING_FILE_PATH + "/" + groupingHistoryId + "_" + date;
                // 敏感字段不加密csv的路径：hdfs://cmss/data/test/encrypt/user_grouping/1000000377_2025-11-11.csv
                String plaintextFileName = csvPathPre + ".csv";
                // 敏感字段加密csv的路径：hdfs://cmss/data/test/encrypt/user_grouping/1000000377_2025-11-11_encrypted.csv
                String encryptedFileName = csvPathPre + "_" + ENCRYPTED + ".csv";
                // csv写到hdfs
                boolean success = writeGroupingCsv(tids, plaintextFileName, encryptedFileName, allFields);

                if (success) {
                    // 成功后，csv下载状态改为可下载1
                    enableCsvDownloadFlag(groupingId, groupingHistoryId, "1");
                    logger.info("csv 文件写入完成：{} 、{} ", plaintextFileName, encryptedFileName);
                    System.out.println(new Date() + "，csv文件写入完成:" + plaintextFileName + ";" + encryptedFileName);
                } else {
                    // 失败后，csv下载状态改为失败4
                    enableCsvDownloadFlag(groupingId, groupingHistoryId, "3");
                    logger.info("csv 文件写入失败：{} 、{} ", plaintextFileName, encryptedFileName);
                    System.out.println(new Date() + "，csv文件写入失败:" + plaintextFileName + ";" + encryptedFileName);
                }

            } else {
                // 删除分群对应的 csv 文件
                deleteCsv(groupingId, "");
                // 删除分群历史记录
                deleteUserGroupingHistory(groupingId);
                // 删除分区记录表GROUPING_RECORD
                deleteGroupingRecord(groupingId);
            }

            return "done";
        });
    }

    /**
     * 新增或更新分群，生成csv文件，生成2份，一份敏感字段加密的，一份敏感字段明文的
     * 字段：
     * 用户T_ID PK_ID,直销系统用户ID CRM_CUSTOMER_ID,鲁雁管家高端旅客用户ID LY_VIP_ID,中文姓名 CN_NAME,英文姓名 EN_NAME
     * 性别 SEX,生日 BIRTHDAY,主手机号 MOBILE_PHONE,主手机号可信度等级 MOBILE_NUMBER_RELIABILITY,邮箱 EMAIL
     * 直销用户状态 DIRECT_USER_STATUS,直销最后一次登录日期 DIRECT_LASTLOGIN_DATE,大客户号 KEY_ACCOUNT_NUMBER
     * 常旅客卡号 FREQUENT_TRAVELER_CARDNO,是否接受会员天地短信营销 ACCEPT_SMS_MARKETING
     * 是否接受会员天地邮件营销 ACCEPT_EMAIL_MARKETING,鲁雁行卡号 LY_CARD_NUMBER
     *
     * 如果allFields为true，还需要加入其他的全域标签字段，allFields为true时，一般为第二次生成csv，已经存在一个没有全域标签字段的csv了
     * @param tids              分群 tid 集合
     * @param plaintextFileName csv中敏感字段不加密的文件写入路径
     * @param encryptedFileName csv中敏感字段加密文件写入路径
     * @param allFields         是否需要全域旅客标签字段
     */
    private static boolean writeGroupingCsv(final List<String> tids, String plaintextFileName, String encryptedFileName,
                                         boolean allFields) {
        try {
            Configuration configuration = buildHdfsConfiguration();

            FileSystem plaintextFs = FileSystem.get(configuration);
            Path plaintextPath = new Path(plaintextFileName);

            FileSystem encryptedFs = FileSystem.get(configuration);
            Path encryptedPath = new Path(encryptedFileName);

            // TODO 如果是二次写入全域标签字段，是否先删除旧的csv
            if (allFields) {
            /*if (plaintextFs.exists(plaintextPath)) {
                plaintextFs.delete(plaintextPath, true);
            }
            if (encryptedFs.exists(encryptedPath)) {
                encryptedFs.delete(encryptedPath, true);
            }*/
            }

            // 内容明文CSV的输出流
            FSDataOutputStream plaintextFsdos = plaintextFs.create(plaintextPath);
            // 内容加密CSV的输出流
            FSDataOutputStream encryptedFsdos = encryptedFs.create(encryptedPath);

            // csv头
            String csvHeader = getCsvHead(allFields);
            encryptedFsdos.write(csvHeader.getBytes(UTF_8));
            plaintextFsdos.write(csvHeader.getBytes(UTF_8));

            // 查询数据库，DIM的用户维表
            Connection connDim = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.DIM_USER, Constants.DIM_PWD);

            Statement stmtDim = DorisUtils.getStatement(connDim);

            ResultSet rs = null;

            // 分批查询，每次1000
            List<List<String>> queryTids = Lists.partition(tids, 1000);
            for (List<String> tid : queryTids) {
                String sql = "SELECT PK_ID,CRM_CUSTOMER_ID,LY_VIP_ID,CN_NAME,EN_NAME,SEX,BIRTHDAY,MOBILE_PHONE," +
                        "MOBILE_NUMBER_RELIABILITY,EMAIL,DIRECT_USER_STATUS,DIRECT_LASTLOGIN_DATE,KEY_ACCOUNT_NUMBER," +
                        "FREQUENT_TRAVELER_CARDNO,ACCEPT_SMS_MARKETING,ACCEPT_EMAIL_MARKETING,LY_CARD_NUMBER," +
                        "PAS_LOCATION,PRICE_DIS_DEV,DEP_CITY,DCS_UP_CLASS,MEMBER_VALUE,PAS_ACTIVITY,PAS_TRAVEL_VALUE," +
                        "PASSENGER_COMSUM_VALUE,PAS_VALUE_UPDATE " +
                        "FROM " + Constants.DWQ_DB + ".T_DIM_USER_DIM WHERE PK_ID in('" + String.join("','", tid) + "')";
                rs = DorisUtils.getDorisResult(stmtDim, sql);
                while (rs != null && rs.next()) {
                    // 数据库已经加密的
                    String pkId = rs.getString("PK_ID");
                    String phone = rs.getString("MOBILE_PHONE");
                    String mail = rs.getString("EMAIL");
                    String ff = rs.getString("FREQUENT_TRAVELER_CARDNO");
                    String lyCardNumber = rs.getString("LY_CARD_NUMBER");

                    // 数据库明文的
                    String crmCustomerId = rs.getString("CRM_CUSTOMER_ID");
                    String lyVipId = rs.getString("LY_VIP_ID");

                    // 写入加密的csv行，敏感字段是明文的要先加密
                    crmCustomerId = SM4Utils.encrypt(crmCustomerId, Constants.SM4_KEY);
                    lyVipId = SM4Utils.encrypt(lyVipId, Constants.SM4_KEY);
                    // 获得csv行数据
                    StringBuilder encryptedRow = getCsvRowStr(rs, pkId, phone, mail, ff, lyCardNumber, crmCustomerId, lyVipId);
                    // 获取到该tid对应的全域标签信息
                    Map<String, String> allFieldMap = getExpandFields(pkId, allFields);
                    // 添加全域旅客标签
                    addExpandFields(allFieldMap, encryptedRow);
                    // 写入文件
                    encryptedFsdos.write(encryptedRow.toString().getBytes(UTF_8));

                    // 写入明文的csv行，加密字段要解密
                    phone = SM4Utils.decrypt(phone, Constants.SM4_KEY);;
                    ff = SM4Utils.decrypt(ff, Constants.SM4_KEY);
                    lyCardNumber = SM4Utils.decrypt(lyCardNumber, Constants.SM4_KEY);
                    mail = SM4Utils.decrypt(mail, Constants.SM4_KEY);
                    crmCustomerId = SM4Utils.decrypt(crmCustomerId, Constants.SM4_KEY);
                    lyVipId = SM4Utils.decrypt(lyVipId, Constants.SM4_KEY);

                    // 获得csv行数据
                    StringBuilder plaintextRow = getCsvRowStr(rs, pkId, phone, mail, ff, lyCardNumber, crmCustomerId, lyVipId);
                    // 添加全域旅客标签
                    addExpandFields(allFieldMap, plaintextRow);
                    // 写入文件
                    plaintextFsdos.write(plaintextRow.toString().getBytes(UTF_8));
                }
            }
            plaintextFsdos.close();
            encryptedFsdos.close();
            plaintextFs.close();
            encryptedFs.close();
            DorisUtils.close(connDim, stmtDim, rs);
        } catch (Exception e) {
            System.out.println(new Date() + ",生成CSV[" + plaintextFileName + "]失败：" + e.getMessage());
            logger.error("生成CSV[{}]失败", plaintextFileName, e);
            return false;
        }
        return true;
    }

    /**
     * 返回全域旅客标签字段映射
     * @param tid 加密的形式，和数据库存的内容一样，无需特殊处理
     * @return 全域旅客标签字段映射
     */
    private static Map<String, String> getExpandFields(String tid, boolean need) {
        if (!need) {
            return new HashMap<>();
        }
        // TODO 后续补充，根据tid查询到所需标签字段
        return new HashMap<>();

    }

    /**
     * 补全全域旅客标签字段
     * @param map 全域旅客标签字段映射
     * @param row  csv的行数据
     */
    private static void addExpandFields(Map<String, String> map, StringBuilder row) {
        if (map.isEmpty()) {
            return;
        }
        // TODO 后续补充，把标签字段按照顺序加到row后面
    }

    /**
     * 写入csv行数据
     */
    private static StringBuilder getCsvRowStr(ResultSet rs, String pkId, String phone, String mail, String ff,
                                       String lyCardNumber, String crmCustomerId, String lyVipId) throws SQLException {
        Date birthDay = rs.getDate("BIRTHDAY");
        Date lastloginDate = rs.getDate("DIRECT_LASTLOGIN_DATE");
        Date pasUpdate = rs.getDate("PAS_VALUE_UPDATE");
        StringBuilder sb = new StringBuilder();
        sb.append(pkId).append(",")
                .append(nullToEmpty(crmCustomerId)).append(",")
                .append(nullToEmpty(lyVipId)).append(",")
                .append(nullToEmpty(rs.getString("CN_NAME"))).append(",")
                .append(nullToEmpty(rs.getString("EN_NAME"))).append(",")
                .append(transferGender(rs.getString("SEX"))).append(",")
                .append(birthDay == null ? "" : DateUtil.formatDate(birthDay, DateUtil.DATE_PATTERN)).append(",")
                .append(nullToEmpty(phone)).append(",")
                .append(transferPhoneReliability(rs.getString("MOBILE_NUMBER_RELIABILITY"))).append(",")
                .append(nullToEmpty(mail)).append(",")
                .append(transferDirUserStatus(rs.getString("DIRECT_USER_STATUS"))).append(",")
                .append(lastloginDate == null ? "" : DateUtil.formatDate(lastloginDate, DateUtil.DATE_PATTERN)).append(",")
                .append(nullToEmpty(rs.getString("KEY_ACCOUNT_NUMBER"))).append(",")
                .append(nullToEmpty(ff)).append(",")
                .append(transferBoolean(rs.getString("ACCEPT_SMS_MARKETING"))).append(",")
                .append(transferBoolean(rs.getString("ACCEPT_EMAIL_MARKETING"))).append(",")
                .append(nullToEmpty(lyCardNumber)).append(",")

                .append(nullToEmpty(rs.getString("PAS_LOCATION"))).append(",")
                .append(nullToEmpty(rs.getString("PRICE_DIS_DEV"))).append(",")
                .append(nullToEmpty(rs.getString("DEP_CITY"))).append(",")
                .append(nullToEmpty(rs.getString("DCS_UP_CLASS"))).append(",")
                .append(nullToEmpty(rs.getString("MEMBER_VALUE"))).append(",")
                .append(transferPasActivity(rs.getString("PAS_ACTIVITY"))).append(",")
                .append(transferPasValue(rs.getString("PAS_TRAVEL_VALUE"))).append(",")
                .append(transferPasValue(rs.getString("PASSENGER_COMSUM_VALUE"))).append(",")
                .append(pasUpdate == null ? "" : DateUtil.formatDate(pasUpdate, DateUtil.DATE_PATTERN)).append(",")

                .append("\n");
        return sb;
    }

    /**
     * null转为空字符串
     * @param str
     */
    private static String nullToEmpty(String str) {
        return StringUtils.isNotBlank(str) ? str : "";
    }

    /**
     * TOP10 （92.00-100.00]
     * TOP20  （83.00-92.00]
     * TOP 30   (74.00-83.00]
     * TOP 40   (65.00-74.00]
     * TOP 50  (56.00-65.00]
     * TOP 60   (47.00-56.00]
     * TOP 70  (38.00-47.00]
     * TOP 80  (29.00-38.00]
     * TOP 90  (20.00-29.00]
     * TOP100  (11.00-20.00]
     * @param str
     */
    private static String transferPasActivity(String str) {
        if (StringUtils.isEmpty(str)) {
            return "";
        }
        try {
            int num = Integer.parseInt(str);
            // 入库数据已处理
            return "TOP" + num;
//            if (num > 92) {
//                return "TOP10";
//            } else if (num > 83) {
//                return "TOP20";
//            } else if (num > 74){
//                return "TOP30";
//            } else if (num > 65){
//                return "TOP40";
//            } else if (num > 56){
//                return "TOP50";
//            } else if (num > 47){
//                return "TOP60";
//            } else if (num > 38){
//                return "TOP70";
//            } else if (num > 29){
//                return "TOP80";
//            } else if (num > 20){
//                return "TOP90";
//            } else if (num > 11){
//                return "TOP100";
//            } else {
//                return "";
//            }
        } catch (Exception e) {
            return "未知";
        }
    }

    /**
     * TOP10 （90-100]
     * TOP 20  （80-90]
     * TOP 30   (70-80]
     * TOP 40   (60-70]
     * TOP 50  (50-60]
     * TOP 60   (40-50]
     * TOP 70  (30-40]
     * TOP 80  (20-30]
     * TOP 90  (10-20]
     * TOP100  [0-10]
     * @param str
     */
    private static String transferPasValue(String str) {
        if (StringUtils.isEmpty(str)) {
            return "";
        }
        try {
            int num = Integer.parseInt(str);
            // 入库数据已处理
            return "TOP" + num;
//            if (num > 90) {
//                return "TOP10";
//            } else if (num > 80) {
//                return "TOP20";
//            } else if (num > 70){
//                return "TOP30";
//            } else if (num > 60){
//                return "TOP40";
//            } else if (num > 50){
//                return "TOP50";
//            } else if (num > 40){
//                return "TOP60";
//            } else if (num > 30){
//                return "TOP70";
//            } else if (num > 20){
//                return "TOP80";
//            } else if (num > 10){
//                return "TOP90";
//            } {
//                return "TOP100";
//            }
        } catch (Exception e) {
            return "未知";
        }
    }

    /**
     * 性别转换 男 M，女 F，未知 U
     * @param gender 数据库查出来的性别
     * @return  转换后的
     */
    private static String transferGender(String gender) {
        if (com.travelsky.dataplatform.constans.trp.utils.Constants.M.equals(gender)) {
            return "男";
        } else if (com.travelsky.dataplatform.constans.trp.utils.Constants.F.equals(gender)) {
            return "女";
        } else {
            return "未知";
        }
    }

    /**
     * 直销用户状态DIRECT_USER_STATUS 冻结2、注销0、正常1
     * @param status 直销用户状态
     * @return  转换后的
     */
    private static String transferDirUserStatus(String status) {
        if (StringUtils.isEmpty(status)) {
            return "";
        }
        if (com.travelsky.dataplatform.constans.trp.utils.Constants.DIRECT_USER_STATUS_2.equals(status)) {
            return "冻结";
        } else if (com.travelsky.dataplatform.constans.trp.utils.Constants.DIRECT_USER_STATUS_1.equals(status)) {
            return "正常";
        } else if (com.travelsky.dataplatform.constans.trp.utils.Constants.DIRECT_USER_STATUS_0.equals(status)) {
            return "注销";
        } else {
            return "未知";
        }
    }

    /**
     * 主手机号可信度等级
     * 高可信 1-2 ，中可信 3-4，低可信 5及之后
     * @param status 主手机号可信度等级
     * @return  转换后的
     */
    private static String transferPhoneReliability(String status) {
        if (StringUtils.isEmpty(status)) {
            return "";
        }

        try {
            int num = Integer.parseInt(status);
            if (num == 1 || num == 2) {
                return "高可信";
            } else if (num == 3 || num == 4) {
                return "中可信";
            } else {
                return "低可信";
            }
        } catch (Exception e) {
            return "未知";
        }

    }

    /**
     * 转换是、否
     * 1-是
     * 0-否
     * 空-null
     * @param str
     * @return  转换后的
     */
    private static String transferBoolean(String str) {
        if (StringUtils.isEmpty(str)) {
            return "";
        }
        try {
            int num = Integer.parseInt(str);
            if (1 == num) {
                return "是";
            } else if (0 == num){
                return "否";
            } else {
                return "未知";
            }
        } catch (Exception e) {
            return "未知";
        }

    }

    /**
     * 根据用户分群 historyId 获得该分群下用户 tid 集合
     * 根据groupingHistoryId从Doris dwd库GROUPING_RECORD表查询到tid
     *
     * @param historyId 用户分群 historyId
     * @return tid 集合
     */
    private static List<String> queryTids(String historyId) {
        List<String> tids = new ArrayList<>();
        Connection connDwd = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.DIM_USER, Constants.DIM_PWD);

        Statement stmtDid = DorisUtils.getStatement(connDwd);

        ResultSet rs = DorisUtils.getDorisResult(stmtDid, "SELECT T_ID FROM " + Constants.DWQ_DB
                + ".GROUPING_RECORD WHERE GROUPING_HISTORY_ID ='" + historyId + "'");
        try {
            if (rs != null) {
                while (rs.next()) {
                    tids.add(rs.getString("T_ID"));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        DorisUtils.close(connDwd, stmtDid, rs);
        System.out.println("查询到tid数量：" + tids.size());
        return tids;
    }

    /**
     * 删除分群的同时删除 csv 文件
     *
     * @param groupingId 分群 id
     * @param notDelHistoryId  分群历史 id，保留这个 id 的 csv 文件
     */
    private static void deleteCsv(String groupingId, String notDelHistoryId) {
        logger.info("开始删除分群信息，groupingId：{}，不删除的historyId：{}", groupingId, notDelHistoryId);
        System.out.println("开始删除分群信息，groupingId：" + groupingId +"，不删除的historyId：" + notDelHistoryId);
        try {
            Connection connection = DmDbUtils.getConnection(Constants.DM_JDBC_URL, Constants.DM_USER, Constants.DM_PWD);
            Statement statement = DmDbUtils.getStatement(connection);
            // 拿到分群 id 对应的分群历史 id
            List<String> historyIds = getDelHistoryIds(groupingId, notDelHistoryId, statement);
            // 删除hdfs上的csv
            deleteHdfsCsv(historyIds);

            // 修改 csv 下载标志状态为0
            String updateSql = "UPDATE USERGROUPINGHISTORY SET ENABLE_CSV_DOWNLOAD='0' WHERE GROUPING_ID='" + groupingId + "'";
            if (StringUtils.isNotBlank(notDelHistoryId)) {
                updateSql+= " AND GROUPING_HISTORY_ID != '" + notDelHistoryId + "'";
            }
            DmDbUtils.executeUpdate(statement, updateSql);
            statement.close();
            connection.close();
        } catch (Exception e) {
            logger.error("删除分群失败：", e);
            System.out.println("删除分群信息失败，groupingId：" + groupingId +"，不删除的historyId：" + notDelHistoryId + "，原因：" + e.getMessage());
        }

    }

    /**
     * 修改 csv 下载标志状态为1可下载
     *
     * @param groupingId
     * @param historyId
     * @param csvState CSV状态：1-可以下载，3-生成失败
     */
    private static void enableCsvDownloadFlag(String groupingId, String historyId, String csvState) {
        logger.info("开始修改csv下载标志为{}，historyId：{}", csvState, historyId);
        System.out.println("开始修改csv下载标志为" + csvState + ",historyId：" + historyId);
        try {
            Connection connection = DmDbUtils.getConnection(Constants.DM_JDBC_URL, Constants.DM_USER, Constants.DM_PWD);
            Statement statement = DmDbUtils.getStatement(connection);
            // 修改 csv 下载标志状态为1可下载
            String updateSql = "UPDATE USERGROUPINGHISTORY SET ENABLE_CSV_DOWNLOAD='" + csvState + "' " +
                    "WHERE GROUPING_ID='" + groupingId + "' AND GROUPING_HISTORY_ID = '" + historyId + "'";
            DmDbUtils.executeUpdate(statement, updateSql);
            statement.close();
            connection.close();
            logger.info("修改csv下载标志为{}成功，historyId：{}", csvState, historyId);
            System.out.println("修改csv下载标志为" + csvState + "成功,historyId：" + historyId);
        } catch (Exception e) {
            logger.error("修改csv下载标志失败", e);
            System.out.println("修改csv下载标志失败，historyId：" + historyId + "，原因：" + e.getMessage());
        }

    }

    /**
     * 删除hdfs上的csv文件
     *
     * @param historyIds  分群历史 id集合
     * @throws IOException
     */
    private static void deleteHdfsCsv(List<String> historyIds) throws IOException {
        Configuration configuration = buildHdfsConfiguration();
        // csv路径目录：hdfs://cmss/data/test/encrypt/user_grouping/
        String csvPathDir = Constants.GROUPING_FILE_PATH + "/";
        Path path = new Path(csvPathDir);
        FileSystem fileSystem = path.getFileSystem(configuration);

        FileStatus[] statuses = fileSystem.listStatus(path);
        // 删除分群对应的 csv 文件
        for (String deleteId : historyIds) {
            for (FileStatus file : statuses) {
                String fileName = file.getPath().getName();
                if (fileName.contains(deleteId)) {
                    fileSystem.delete(file.getPath(), true);
                    logger.info("删除csv文件成功：{}", fileName);
                    System.out.println("删除csv文件成功" + fileName);
                }
            }
        }
    }

    /**
     * 根据groupingId和historyId获取要删除的historyId集合
     * 如果传了historyId，那么这个不用删除
     *
     * @param historyId 用户分群 historyId
     * @return tid 集合
     */
    private static List<String> getDelHistoryIds(String groupingId, String historyId, Statement statement) throws SQLException {
        List<String> historyIds = new ArrayList<>();
        String sql = "SELECT GROUPING_HISTORY_ID FROM USERGROUPINGHISTORY WHERE GROUPING_ID='" + groupingId + "'";
        if (StringUtils.isNotBlank(historyId)) {
            sql += " AND GROUPING_HISTORY_ID != '" + historyId + "'";
        }

        ResultSet rs = DmDbUtils.getDmResult(statement, sql);
        try {
            if (rs != null) {
                while (rs.next()) {
                    historyIds.add(rs.getString("GROUPING_HISTORY_ID"));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        rs.close();
        return historyIds;
    }

    /**
     * 根据分群 id 删除分群历史记录
     *
     * @param groupingId 分群历史 id
     * @return
     */
    private static void deleteUserGroupingHistory(String groupingId) {
        logger.info("开始删除分群记录，groupingId：{}", groupingId);
        System.out.println("开始删除分群记录，groupingId：" + groupingId);

        try {
            Connection connection = DmDbUtils.getConnection(Constants.DM_JDBC_URL, Constants.DM_USER, Constants.DM_PWD);
            Statement statement = DmDbUtils.getStatement(connection);
            String sql = "DELETE FROM USERGROUPINGHISTORY WHERE GROUPING_ID = '" + groupingId + "'";
            DmDbUtils.executeUpdate(statement, sql);
            statement.close();
            connection.close();
            logger.info("删除分群记录成功，groupingId：{}", groupingId);
            System.out.println("删除分群记录成功，groupingId：" + groupingId);
        } catch (Exception e) {
            logger.error("删除分群记录失败：", e);
            System.out.println("删除分群记录失败，groupingId：" + groupingId + "，原因：" + e.getMessage());
        }

    }

    /**
     * 根据分群 id 删除分区记录表GROUPING_RECORD
     *
     * @param groupingId 分群历史 id
     * @return
     */
    private static void deleteGroupingRecord(String groupingId) throws SQLException {
        logger.info("开始删除分区记录表GROUPING_RECORD，groupingId：{}", groupingId);
        System.out.println("开始删除分区记录表GROUPING_RECORD，groupingId：" + groupingId);
        try {
            Connection conn = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.DIM_USER, Constants.DIM_PWD);
            Statement statement = DorisUtils.getStatement(conn);
            String sql = "DELETE FROM " + Constants.DWQ_DB +".GROUPING_RECORD WHERE GROUPING_ID = '" + groupingId + "'";
            DorisUtils.excuteDorisInsert(statement, sql);
            statement.close();
            conn.close();
            logger.info("删除分区记录表GROUPING_RECORD成功，groupingId：{}", groupingId);
            System.out.println("删除分区记录表GROUPING_RECORD成功，groupingId：" + groupingId);
        } catch (Exception e) {
            logger.error("删除分区记录表GROUPING_RECORD：", e);
            System.out.println("删除分区记录表GROUPING_RECORD，groupingId：" + groupingId + "，原因：" + e.getMessage());
        }

    }

    /**
     * csv表头
     *
     * @return csv表头
     */
    public static String getCsvHead(boolean allFields) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("T_id").append(",")
                .append("直销系统用户ID").append(",")
                .append("鲁雁管家高端旅客用户ID").append(",")
                .append("中文姓名").append(",")
                .append("英文姓名").append(",")
                .append("性别").append(",")
                .append("生日").append(",")
                .append("主手机号").append(",")
                .append("主手机号可信度等级").append(",")
                .append("邮箱").append(",")
                .append("直销用户状态").append(",")
                .append("直销最后一次登录日期").append(",")
                .append("大客户号").append(",")
                .append("常旅客卡号").append(",")
                .append("是否接受会员天地短信营销").append(",")
                .append("是否接受会员天地邮件营销").append(",")
                .append("鲁雁行卡号").append(",")
                .append("旅客客源地").append(",")
                .append("国内平均折扣比较").append(",")
                .append("出行城市TOP3").append(",")
                .append("离港升舱需求度").append(",")
                .append("会员价值").append(",")
                .append("旅客飞行活跃度").append(",")
                .append("旅客出行价值").append(",")
                .append("旅客消费价值").append(",")
                .append("旅客价值特征更新日期");
        if (allFields) {
            // TODO 添加全域标签字段
        }
        stringBuilder.append("\n");

        return stringBuilder.toString();
    }

    /**
     * 构建HDFS配置（包含Kerberos认证）
     */
    private static Configuration buildHdfsConfiguration() {
        System.setProperty("java.security.krb5.conf", Constants.KRB5_CONF_PATH);

        Configuration conf = new Configuration();
        try {
            conf.set("fs.defaultFS", "hdfs://cmss");
            conf.set("dfs.nameservices", "cmss");
            conf.set("dfs.ha.namenodes.cmss", "nn1,nn2");
            conf.set("dfs.namenode.rpc-address.cmss.nn1", "emr-master-001.novalocal:8020");
            conf.set("dfs.namenode.rpc-address.cmss.nn2", "emr-master-ha-001.novalocal:8020");
            conf.set("dfs.client.failover.proxy.provider.cmss",
                    "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
            // 本地调试需要，服务器上，本行要注释
//            KerberosAuthUtils.initKerberosAuth(Constants.KRB5_CONF_PATH, Constants.SYS_UCV_TEST_FOR_MAPREDUCE_PRINCIPAL, Constants.SYS_UCV_TEST_FOR_MAPREDUCE_KEYTAB_PATH);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return conf;
    }
}