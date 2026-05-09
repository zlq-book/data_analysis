package com.travelsky.dataplatform.utils;

import com.travelsky.dataplatform.constans.Constants;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.doris.flink.source.DorisSource;

import java.util.Properties;
import java.util.UUID;

/**
 * @author kuangaihua
 * @date 2025/7/9 9:50
 */
public class FlinkDorisUtils {

    /**
     * 创建DorisSink-入ODS层数据
     * @param tableName
     * @return
     * @param <T>
     */
    public static <T> DorisSink<T> creatDorisODSSink(String tableName) {
        return creatDorisSink(
                Constants.ODS_DB,
                tableName,
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.ODS_USER,
                Constants.ODS_PWD);
    }

    /**
     * 创建DorisSink-入ODS层数据
     * @param tableName
     * @return
     * @param <T>
     */
    public static <T> DorisSink<T> creatDorisODSSinkForFTP(String tableName) {
        return creatDorisSinkForSnakeCase(
                Constants.ODS_DB,
                tableName,
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.ODS_USER,
                Constants.ODS_PWD);
    }

    /**
     * 创建高频DorisSink-入DWD层数据
     * @param tableName
     * @param flushIntervalMs 攒批提交时间间隔
     * @return
     * @param <T>
     */
    public static <T> DorisSink<T> creatHsdDorisDWDSink(String tableName, Integer flushIntervalMs) {
        return creatDorisSink(
                Constants.DWD_DB,
                tableName,
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DWD_USER,
                Constants.DWD_PWD,
                flushIntervalMs);
    }
    /**
     * 创建DorisSink-入DWD层数据
     * @param tableName
     * @return
     * @param <T>
     */
    public static <T> DorisSink<T> creatDorisDWDSink(String tableName) {
        return creatDorisSink(
                Constants.DWD_DB,
                tableName,
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DWD_USER,
                Constants.DWD_PWD);
    }

    /**
     * 创建DorisSink-入DIM层数据
     * @param tableName
     * @return
     * @param <T>
     */
    public static <T> DorisSink<T> creatDorisDIMSink(String tableName) {
        return creatDorisSink(
                Constants.DIM_DB,
                tableName,
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DIM_USER,
                Constants.DIM_PWD);
    }

    //doris sink，整行更新
    public static <T> DorisSink<T> creatDorisSink(String dbName, String tableName, String feURL, String beURL, String user, String password) {
        return creatDorisSink(
                dbName,
                tableName,
                feURL,
                beURL,
                user,
                password,
                20);
    }
    public static <T> DorisSink<T> creatDorisSink(String dbName, String tableName, String feURL, String beURL, String user, String password, Integer flushIntervalMs) {

        try {
//  确保表存在，不存在则创建该表
//            DorisTableInfoUtils.createDorisTableUserDimIfNotExists(dbName, tableName);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        DorisSink<String> builder = DorisSink.<String>builder();

            DorisOptions dorisOptions =
                    DorisOptions.builder()
                            .setFenodes(feURL)
                            .setBenodes(beURL)
                            .setTableIdentifier(dbName + "." + tableName)
                            .setUsername(user)
                            .setPassword(password)
                            .build();

            Properties properties = new Properties();
            properties.setProperty("read_json_by_line", "true");
            properties.setProperty("format", "json");
            long timestamp = System.currentTimeMillis();
            UUID uuid = UUID.randomUUID();
            DorisExecutionOptions executionOptions =
                    DorisExecutionOptions.builder()
                            .setLabelPrefix("" + timestamp+uuid)
                            .setDeletable(false)
                            .setBatchMode(true)
                            .setFlushQueueSize(5)
                            .setBufferFlushMaxRows(800000)
                            .setBufferFlushMaxBytes(300 * 1024 * 1024)
                            .setBufferFlushIntervalMs(flushIntervalMs * 1000)
                            .setStreamLoadProp(properties)
                            .setBufferSize(1024 * 1024*1024 * 1024)
                            .build();
            GenericDorisSerializer<T> serializer = GenericDorisSerializer
//                    .createWithDateFormat(dateFormat);
                    .createSafe();
            DorisSink<T> dorisSink = DorisSink.<T>builder().setDorisReadOptions(DorisReadOptions.builder().build())
                    .setDorisExecutionOptions(executionOptions)
                    .setSerializer(serializer)
                    .setDorisOptions(dorisOptions).build();
            return dorisSink;
        } catch (Exception e) {
            System.out.println("创建dorisSink报错");
            e.printStackTrace();
            return null;
        }
    }

    //doris sink，整行更新
    public static <T> DorisSink<T> creatDorisSinkForSnakeCase(String dbName, String tableName, String feURL, String beURL, String user, String password) {

        try {
//  确保表存在，不存在则创建该表
//            DorisTableInfoUtils.createDorisTableUserDimIfNotExists(dbName, tableName);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        DorisSink<String> builder = DorisSink.<String>builder();

            DorisOptions dorisOptions =
                    DorisOptions.builder()
                            .setFenodes(feURL)
                            .setBenodes(beURL)
                            .setTableIdentifier(dbName + "." + tableName)
                            .setUsername(user)
                            .setPassword(password)
                            .build();

            Properties properties = new Properties();
            properties.setProperty("read_json_by_line", "true");
            properties.setProperty("format", "json");
            properties.setProperty("strip_outer_array", "false");  // 非数组格式，必须为false
            properties.setProperty("fuzzy_parse", "true");  // 启用模糊解析，自动匹配驼峰和下划线
            long timestamp = System.currentTimeMillis();
            UUID uuid = UUID.randomUUID();
            DorisExecutionOptions executionOptions =
                    DorisExecutionOptions.builder()
                            .setLabelPrefix("" + timestamp+uuid)
                            .setDeletable(false)
                            .setBatchMode(true)
                            // 优化：设置缓冲区大小和批处理阈值，提升写入性能
                            .setBufferSize(10485760)  // 10MB缓冲区
                            .setBufferCount(3)  // 3个缓冲区
                            .setStreamLoadProp(properties)
                            .build();
            // 使用支持下划线命名的序列化器
            GenericDorisSerializer<T> serializer = GenericDorisSerializer.createForDoris();
            DorisSink<T> dorisSink = DorisSink.<T>builder().setDorisReadOptions(DorisReadOptions.builder().build())
                    .setDorisExecutionOptions(executionOptions)
                    .setSerializer(serializer)
                    .setDorisOptions(dorisOptions).build();
            return dorisSink;
        } catch (Exception e) {
            System.out.println("创建dorisSink报错");
            e.printStackTrace();
            return null;
        }
    }

    //doris sink，部分列更新
    public static <T> DorisSink<T> creatDorisPartialSink(String dbName, String tableName, String feURL, String beURL, String user, String password, String columns) {

        try {
//  确保表存在，不存在则创建该表
//            DorisTableInfoUtils.createDorisTableUserDimIfNotExists(dbName, tableName);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        DorisSink<String> builder = DorisSink.<String>builder();

            DorisOptions dorisOptions =
                    DorisOptions.builder()
                            .setFenodes(feURL)
                            .setBenodes(beURL)
                            .setTableIdentifier(dbName + "." + tableName)
                            .setUsername(user)
                            .setPassword(password)
                            .build();

            Properties properties = new Properties();
            properties.setProperty("read_json_by_line", "true");
            properties.setProperty("format", "json");
            properties.setProperty("partial_columns", "true");
            properties.setProperty("columns", columns);
            long timestamp = System.currentTimeMillis();
            UUID uuid = UUID.randomUUID();
            DorisExecutionOptions executionOptions =
                    DorisExecutionOptions.builder()
                            .setLabelPrefix("" + timestamp+uuid)
                            .setDeletable(false)
                            .setBatchMode(true)
                            .setStreamLoadProp(properties)
                            .build();
            GenericDorisSerializer<T> serializer = GenericDorisSerializer
//                    .createWithDateFormat(dateFormat);
                    .createSafe();
            DorisSink<T> dorisSink = DorisSink.<T>builder().setDorisReadOptions(DorisReadOptions.builder().build())
                    .setDorisExecutionOptions(executionOptions)
                    .setSerializer(serializer)
                    .setDorisOptions(dorisOptions).build();
            return dorisSink;
        } catch (Exception e) {
            System.out.println("创建dorisSink报错");
            e.printStackTrace();
            return null;
        }
    }

    public static <T> DorisSource<T> creatQueryDorisSource(String dbName, String tableName, String[] fieldNames, String FilterQuery, Class<T> clazz, String feURL, String beURL, String user, String password) {
//        T instance = clazz.getDeclaredConstructor().newInstance();
        DorisOptions dorisOptions = DorisOptions.builder()
                .setFenodes(feURL)
                .setBenodes(beURL)
                .setTableIdentifier(dbName + "." + tableName)
                .setUsername(user)
                .setPassword(password)
                .build();
        String fields = String.join(",", fieldNames);
        // 配置读取选项并设置过滤条件
        DorisReadOptions readOptions = DorisReadOptions.builder()
                .setReadFields(fields)
                .setUseFlightSql(true)
                .setFlightSqlPort(Constants.DORIS_FLIGHT_PORT)
                .setRequestBatchSize(500)
                .setFilterQuery(FilterQuery)  // 直接设置过滤条件
                .build();

        GenericJsonDeserializer<T> serializer = new GenericJsonDeserializer<>(clazz, fieldNames);
        // 构建DorisSource
        DorisSource<T> dorisSource = DorisSource.<T>builder()
                .setDorisOptions(dorisOptions)
                .setDorisReadOptions(readOptions)
                .setDeserializer(serializer)  // 自定义反序列化器
                .build();
        return dorisSource;

//        env.fromSource(
//                dorisSource,
//                WatermarkStrategy.noWatermarks(),
//                "Doris Source"
//        ).map(user->{
//            return user.getName();
//        }).print();
////        System.out.println("**********************************"+dd.print());
//
//        env.execute("Doris Predicate Pushdown Example");
//    }
    }

    public static DorisSink<String> creatDorisJsonSink(String dbName, String tableName, String feURL, String beURL, String user, String password) {

        DorisSink.Builder<String> builder = DorisSink.<String>builder();

        DorisOptions dorisOptions =
                DorisOptions.builder()
                        .setFenodes(feURL)
                        .setBenodes(beURL)
                        .setTableIdentifier(dbName + "." + tableName)
                        .setUsername(user)
                        .setPassword(password)
                        .build();

        Properties properties = new Properties();
        UUID uuid = UUID.randomUUID();
        properties.setProperty("read_json_by_line", "true");
        properties.setProperty("format", "json");
        DorisExecutionOptions executionOptions =
                DorisExecutionOptions.builder()
                        .setLabelPrefix("label-doris"+uuid)
                        .setDeletable(false)
                        .setBatchMode(true)
                        .setStreamLoadProp(properties)

                        .build();

        DorisSink<String> build = builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionOptions)
                .setSerializer(new SimpleStringSerializer())

                .setDorisOptions(dorisOptions).build();
        return build;

    }

    //点查doris，并返回JSONObject

    public static DorisSink<String> creatDorisJsonPartialSink(String dbName, String tableName, String feURL, String beURL, String user, String password, String columns) {

        DorisSink.Builder<String> builder = DorisSink.<String>builder();
        UUID uuid = UUID.randomUUID();
        DorisOptions dorisOptions =
                DorisOptions.builder()
                        .setFenodes(feURL)
                        .setBenodes(beURL)
                        .setTableIdentifier(dbName + "." + tableName)
                        .setUsername(user)
                        .setPassword(password)
                        .build();

        Properties properties = new Properties();
        properties.setProperty("read_json_by_line", "true");
        properties.setProperty("format", "json");
        properties.setProperty("partial_columns", "true");
        properties.setProperty("columns", columns);
        DorisExecutionOptions executionOptions =
                DorisExecutionOptions.builder()
                        .setLabelPrefix("label-doris"+uuid)
                        .setDeletable(false)
                        .setBatchMode(true)
                        .setStreamLoadProp(properties)

                        .build();

        DorisSink<String> build = builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionOptions)
                .setSerializer(new SimpleStringSerializer())

                .setDorisOptions(dorisOptions).build();
        return build;

    }

}
