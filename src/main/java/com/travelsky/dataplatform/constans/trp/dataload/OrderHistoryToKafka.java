//package com.travelsky.dataplatform.constans.trp.dataload;
//
//import com.travelsky.dataplatform.constans.trp.utils.Constants;
//import com.travelsky.dataplatform.constans.trp.utils.DateUtil;
//import com.travelsky.dataplatform.constans.trp.utils.HttpClientUtils;
//import com.travelsky.dataplatform.constans.trp.utils.Utils;
//import org.apache.avro.Schema;
//import org.apache.avro.reflect.ReflectData;
//import org.apache.commons.lang3.StringUtils;
//import org.apache.flink.api.common.JobExecutionResult;
//import org.apache.flink.api.common.accumulators.LongCounter;
//import org.apache.flink.api.common.functions.RichMapFunction;
//import org.apache.flink.api.common.typeinfo.TypeHint;
//import org.apache.flink.api.java.DataSet;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.api.java.operators.DataSource;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.typeutils.PojoTypeInfo;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.core.fs.Path;
//import org.apache.flink.formats.parquet.ParquetPojoInputFormat;
//import org.apache.parquet.avro.AvroSchemaConverter;
//import org.apache.parquet.schema.MessageType;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//import java.lang.reflect.Field;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//
///**
// * 将oracle 的seach表写到Kafka
// * 另一部分需要从 ITINERARY抽取字段插入到hbase中
// * 插数据时需要先判断原字段是否有值 是否和要插入的值重复 重复的舍弃  不重复的加分隔符拼接插入
// */
//public class OrderHistoryToKafka {
//    /**
//     * 日志实例
//     */
//    final static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(OrderHistoryToKafka.class);
//
//    /**
//     * 创建批处理实例
//     */
//    private static final ExecutionEnvironment ENV = ExecutionEnvironment.getExecutionEnvironment();
//
//
//    /**
//     * 将数据写入order_search表
//     *
//     * @param date 执行日期
//     * @throws Exception
//     */
//    public static void saveDataToKafka(String date) throws Exception {
//
//        //拼出数据所在目录
//        String itineraryDir = Constants.HDFS_SERVER + Constants.HDFS_FLODER
//                + Constants.SLASH + Constants.AIR_LINE + Constants.SLASH
//                + date + Constants.ITINERARY;
//
//        String searchDir = Constants.HDFS_SERVER + Constants.HDFS_FLODER
//                + Constants.SLASH + Constants.AIR_LINE + Constants.SLASH
//                + date + Constants.SEARCH;
//
//        //如果文件不存在
//        if (!Utils.hdfsFolderExist(itineraryDir)) {
//            LOGGER.error("itinerary文件夹不存在:" + itineraryDir);
//            //告警
//            HttpClientUtils.sentPost(HttpClientUtils.createEventPostJson("JobException",
//                    "文件夹不存在" + itineraryDir), Constants.ARK_MONITOR_EVENT_SERVICE_URL);
//            throw new Exception("itinerary文件夹不存在");
//        }
//        //如果文件不存在
//        if (!Utils.hdfsFolderExist(searchDir)) {
//            LOGGER.error("search文件夹不存在:" + searchDir);
//            //告警
//            HttpClientUtils.sentPost(HttpClientUtils.createEventPostJson("JobException",
//                    "文件夹不存在" + searchDir), Constants.ARK_MONITOR_EVENT_SERVICE_URL);
//            throw new Exception("itinerary文件夹不存在");
//        }
//
//        //定义格式
//        PojoTypeInfo<OracleItineraryModel> itineraryTypeInfo = (PojoTypeInfo<OracleItineraryModel>) PojoTypeInfo.of(OracleItineraryModel.class);
//        Schema itinerarySchema = ReflectData.get().getSchema(OracleItineraryModel.class);
//        MessageType itineraryMessageType = new AvroSchemaConverter().convert(itinerarySchema);
//        //创建路径
//        Path itineraryPath = new Path(itineraryDir);
//        ParquetPojoInputFormat<OracleItineraryModel> itineraryInputFormat = new ParquetPojoInputFormat<>(itineraryPath, itineraryMessageType, itineraryTypeInfo);
//        DataSource<OracleItineraryModel> itineraryDataSource = ENV.createInput(itineraryInputFormat, itineraryTypeInfo);
////        定义格式
//        PojoTypeInfo<OracleSearchModel> searchTypeInfo = (PojoTypeInfo<OracleSearchModel>) PojoTypeInfo.of(OracleSearchModel.class);
//        Schema searchSchema = ReflectData.get().getSchema(OracleSearchModel.class);
//        MessageType searchMessageType = new AvroSchemaConverter().convert(searchSchema);
//        Path searchPath = new Path(searchDir);
//        ParquetPojoInputFormat<OracleSearchModel> searchInputFormat = new ParquetPojoInputFormat<>(searchPath, searchMessageType, searchTypeInfo);
//        DataSet<OracleSearchModel> searchDataSource = ENV.createInput(searchInputFormat, searchTypeInfo);
//        //统计spnrID总条数
//        DataSet<Tuple2<String, Map<String, String>>> itineraryDataSet = itineraryDataSource
//                .map(value -> {
//                    Tuple2<String, Map<String, String>> itineraryTuple = new Tuple2<>();
//                    if (null != value.getRESERVATIONDATE() && value.getRESERVATIONDATE().length() >= 19) {
//                        value.setRESERVATIONDATE(DateUtil.formatDate(DateUtil
//                                .parse(value.getRESERVATIONDATE().replace("T", " "), Constants.DATA_PATTERN), Constants.TIME_PATTERN));
//                    }
//                    if (null != value.getRESERVATIONSTART() && value.getRESERVATIONSTART().length() >= 19) {
//                        value.setRESERVATIONSTART(DateUtil.formatDate(DateUtil
//                                .parse(value.getRESERVATIONSTART().replace("T", " "), Constants.DATA_PATTERN), Constants.TIME_PATTERN));
//                    }
//                    if (null != value.getRESERVATIONEND() && value.getRESERVATIONEND().length() >= 19) {
//                        value.setRESERVATIONEND(DateUtil.formatDate(DateUtil
//                                .parse(value.getRESERVATIONEND().replace("T", " "), Constants.DATA_PATTERN), Constants.TIME_PATTERN));
//                    }
//                    Map<String, String> itineraryMap = objectToMap(value);
//                    itineraryTuple.f0 = value.getSPNRID();
//                    itineraryTuple.f1 = itineraryMap;
//                    return itineraryTuple;
//                }).returns(new TypeHint<Tuple2<String, Map<String, String>>>() {
//                }).groupBy(value -> {
//                    return value.f0;
//                }).reduce((value1, value2) -> {
//                    /**
//                     * 合并相同spnrID的pnr数据
//                     */
//                    Iterator<Map.Entry<String, String>> entries = value2.f1.entrySet().iterator();
//                    while (entries.hasNext()) {
//                        Map.Entry<String, String> entry = entries.next();
//                        value1.f1.merge(entry.getKey(), entry.getValue(), OrderHistoryToHbase::getMergeContent);
//                    }
//                    return value1;
//                }).returns(new TypeHint<Tuple2<String, Map<String, String>>>() {
//                }).map(new RichMapFunction<Tuple2<String, Map<String, String>>, Tuple2<String, Map<String, String>>>() {
//                    LongCounter counterAcc = new LongCounter();
//
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//                        getRuntimeContext().addAccumulator("total_count" + "_" + date, counterAcc);
//                    }
//
//                    @Override
//                    public Tuple2<String, Map<String, String>> map(Tuple2<String, Map<String, String>> value) throws Exception {
//                        counterAcc.add(1L);
//                        return value;
//                    }
//                });
//
//        String startTimeSearch = DateUtil.formatDate(System.currentTimeMillis(), Constants.RUN_LOG_PATTERN);
//
//        //将SEARCH表数据与ITINERARY数据写入ORDER_SEARCH表
//        saveSearchToOederSearch(searchDataSource, itineraryDataSet, date);
//
//        // 首次执行时插入
//        String startTimeFetch = DateUtil.formatDate(System.currentTimeMillis(), Constants.RUN_LOG_PATTERN);
//
//        //将ITINERARY数据中的SPNRID写入FETCH_LOG表
//        saveFetchLog(itineraryDataSet, date);
//
//        // 执行任务
//        JobExecutionResult jobResult = ENV.execute("OrderHistoryToHbase");
//        String endTimeSearch = DateUtil.formatDate(System.currentTimeMillis(), Constants.RUN_LOG_PATTERN);
//
//        //统计spnrID总条数
//        Long totalCount = jobResult.getAccumulatorResult("total_count" + "_" + date);
//        //统计写入order_search表总条数
//        Long orderSearchCount = jobResult.getAccumulatorResult("order_search_count" + "_" + date);
//        //计算未写入order_search表条数
//        Long failureOrderSearch = totalCount - orderSearchCount;
//        LOGGER.info("本次任务写入order_search表数量：" + orderSearchCount + ",failureOrderSearch数量" +
//                failureOrderSearch + ",需要写入的总数：" + totalCount);
//
//        //统计写入fetch_log总条数
//        Long fetchLogCount = jobResult.getAccumulatorResult("order_fetch_log_count" + "_" + date);
//        //统计未写入fetch_log条数
//        Long failureFetchLogCount = totalCount - fetchLogCount;
//
//        LOGGER.info("本次任务需要写入data_log表orderSearchCount数量：" + orderSearchCount + "，fetchLogCount数量：" + fetchLogCount);
//
//    }
//
//    /**
//     * 将HDFS上的文件写入hbase的order_serach表
//     *
//     * @param searchDataSource
//     * @param itineraryDataSource
//     * @param date
//     */
//    private static void saveSearchToOederSearch(DataSet<OracleSearchModel> searchDataSource,
//                                                DataSet<Tuple2<String, Map<String, String>>> itineraryDataSource, String date) throws Exception {
//
//        //将SEARCH表数据进行行转列
//        DataSet<Tuple2<String, Map<String, String>>> searchDataSet = searchDataSource
//                .filter(value -> {
//                    if (null == value.getTYPE() && null == value.getCONTENT()) {
//                        return false;
//                    }
//                    return true;
//                })
//                .map(new SearchDataRowToColumnMap())
//                .groupBy(value -> {
//                    return value.f0;
//                }).reduce((value1, value2) -> {
//                    /**
//                     * 合并相同spnrID的search数据
//                     */
//                    Iterator<Map.Entry<String, String>> entries = value2.f1.entrySet().iterator();
//                    while (entries.hasNext()) {
//                        Map.Entry<String, String> entry = entries.next();
//                        value1.f1.merge(entry.getKey(), entry.getValue(), OrderHistoryToHbase::getMergeContent);
//                    }
//                    return value1;
//                }).returns(new TypeHint<Tuple2<String, Map<String, String>>>() {
//                });
//
//        DataSet<Tuple2<String, Map<String, String>>> itineraryDataSet = itineraryDataSource;
//
//        //将两种数据进行join  相同spnrId会聚合变成一条数据写入hbase表
//        DataSet<Tuple2<String, Map<String, String>>> result = itineraryDataSet.fullOuterJoin(searchDataSet)
//                .where(value -> {
//                    return value.f0;
//                }).equalTo(value -> {
//                    return value.f0;
//                }).with(new SerachJoinItineryResultProcess())
//                .map(new RichMapFunction<Tuple2<String, Map<String, String>>, Tuple2<String, Map<String, String>>>() {
//                    LongCounter counterAcc = new LongCounter();
//
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//                        getRuntimeContext().addAccumulator("order_search_count" + "_" + date, counterAcc);
//                    }
//
//                    @Override
//                    public Tuple2<String, Map<String, String>> map(Tuple2<String, Map<String, String>> value) throws Exception {
//                        counterAcc.add(1L);
//                        return value;
//                    }
//                });
//        TupleHBaseOutputFormat tupleHBaseOutputFormat = new TupleHBaseOutputFormat(Utils.getHbaseTbaleName(Constants.HBASE_TABLE_ORDER_SEARCH, date),
//                Constants.HBASE_TABLE_COLUMN_FAMILY, false);
//
//        result.output(tupleHBaseOutputFormat);
//
//    }
//
//    /**
//     * 合并数据
//     *
//     * @param oldValue 旧值
//     * @param newValue 新值
//     * @return 合并后值
//     */
//    public static String getMergeContent(String oldValue, String newValue) {
//
//        String[] oldValues = oldValue.split(Constants.MULTIPLE_DATA_SEPARATOR);
//        String[] newValues = newValue.split(Constants.MULTIPLE_DATA_SEPARATOR);
//
//        StringBuffer rtnValue = new StringBuffer();
//        //使用Set去重特性  将重复的值保留一个
//        Set<String> set = new HashSet();
//        for (String old : oldValues) {
//            set.add(old);
//        }
//        for (String newv : newValues
//        ) {
//            set.add(newv);
//        }
//
//        Object[] res = set.toArray();
//        for (int i = 0; i < set.size(); i++) {
//            if (StringUtils.isBlank(rtnValue.toString())) {
//                rtnValue.append(res[i]);
//            } else {
//                rtnValue.append(Constants.MULTIPLE_DATA_SEPARATOR).append(res[i]);
//            }
//        }
//        return rtnValue.toString();
//    }
//
//    /**
//     * 将 itinerary的spnrId 写入fetch_log表中
//     *
//     * @param itineraryDataSource
//     * @param date
//     */
//    private static void saveFetchLog(DataSet<Tuple2<String, Map<String, String>>> itineraryDataSource, String date) throws Exception {
//        long current = System.currentTimeMillis();
//        String today = DateUtil.formatDate(current, Constants.PATTERN);
//
//        DataSet<OrderFetchLogModel> result = itineraryDataSource
//                .map(new RichMapFunction<Tuple2<String, Map<String, String>>, OrderFetchLogModel>() {
//                    LongCounter counterAcc = new LongCounter();
//
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//                        getRuntimeContext().addAccumulator("order_fetch_log_count" + "_" + date, counterAcc);
//                    }
//
//                    @Override
//                    public OrderFetchLogModel map(Tuple2<String, Map<String, String>> info) throws Exception {
//                        OrderFetchLogModel spnrListModel = new OrderFetchLogModel();
//                        spnrListModel.setSpnrId(info.f1.get(Constants.PREFIX_MAP_ITINERARY + Constants.MAP_SPNRID));
//                        spnrListModel.setAddTime(today);
//                        spnrListModel.setSummaryFlag(Constants.NEW_SPNRID_MARK);
//                        spnrListModel.setSpnrFlag(Constants.NEW_SPNRID_MARK);
//                        spnrListModel.setOrderCreateDate(date);
//                        counterAcc.add(1L);
//                        return spnrListModel;
//                    }
//                });
//
//        result.output(new HBaseOutputFormat(Constants.HBASE_TABLE_ORDER_FETCH_LOG,
//                Constants.HBASE_TABLE_COLUMN_FAMILY, false, false, null));
//
//    }
//
//
//    public static Map<String, String> objectToMap(Object obj) throws IllegalAccessException {
//        Map<String, String> map = new HashMap<String, String>();
//        Class<?> clazz = obj.getClass();
//        for (Field field : clazz.getDeclaredFields()) {
//            field.setAccessible(true);
//            String column = field.getName();
//            if ("ENCODER".equals(column) || "WRITER$".equals(column) || "DECODER".equals(column)
//                    || "MODEL$".equals(column) || "READER$".equals(column) || "SCHEMA$".equals(column)) {
//                continue;
//            }
//            //ITINERARY表字段需要加一个前缀
//            String fieldName = Constants.PREFIX_MAP_ITINERARY + field.getName();
//
//            String value = "";
//            if (field.get(obj) != null) {
//                value = field.get(obj).toString();
//            }
//
//            String oldValue = map.get(fieldName);
//            if (StringUtils.isNotBlank(oldValue)) {
//                map.put(fieldName, oldValue + "," + value);
//            } else {
//                map.put(fieldName, value);
//            }
//        }
//        return map;
//    }
//}
