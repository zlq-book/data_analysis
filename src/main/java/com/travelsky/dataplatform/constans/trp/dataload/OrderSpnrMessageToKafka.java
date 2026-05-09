//package com.travelsky.dataplatform.constans.trp.dataload;
//
//import com.travelsky.dataplatform.constans.trp.utils.Constants;
//import com.travelsky.dataplatform.constans.trp.utils.DateUtil;
//import com.travelsky.dataplatform.constans.trp.utils.HttpClientUtils;
//import com.travelsky.dataplatform.constans.trp.utils.Utils;
//import org.apache.commons.lang3.StringUtils;
//import org.apache.flink.api.common.JobExecutionResult;
//import org.apache.flink.api.common.accumulators.LongCounter;
//import org.apache.flink.api.common.functions.FilterFunction;
//import org.apache.flink.api.common.functions.RichMapFunction;
//import org.apache.flink.api.java.DataSet;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.tuple.Tuple4;
//import org.apache.flink.configuration.Configuration;
//import org.apache.hadoop.hbase.exceptions.HBaseException;
//import org.apache.hadoop.hbase.filter.CompareFilter;
//import org.apache.hadoop.hbase.filter.FilterList;
//import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
//import org.apache.hadoop.hbase.filter.SubstringComparator;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//import java.util.*;
//
///**
// * 读取HBase的订单报文表获取记录表获取spnrid列表
// * 然后调用接口获取报文
// * 写入到订单报文表：(order_message)
// * 并且记录日志到data_log 以及run_log
// */
//public class OrderSpnrMessageToKafka {
//    /**
//     * 日志实例
//     */
//    public final static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(OrderSpnrMessageToKafka.class);
//
//    public static ExecutionEnvironment env;
//
//    /**
//     * 将指定日期的数据获取报文后写入hbase
//     *
//     * @param date
//     * @throws Exception
//     */
//    public static void getSpnrMessageToHbase(String date) throws Exception {
//        //获取报文情况记录
//        Tuple4<Long, Long, Long, Long> totalGetXmlStatus = new Tuple4<>(0L, 0L, 0L, 0L);
//
//        //任务开始时间
//        String startTimeSpnrService = DateUtil.formatDate(System.currentTimeMillis(), Constants.RUN_LOG_PATTERN);
//
//        //获取指定日期未获取过SPNR报文的snridlist
//        List spnrIdList = getSpnrIdList(date, Constants.SPNR_FLAG);
//        LOGGER.info(date + "需要获取spnr报文的spnrID数为" + spnrIdList.size());
//        if (spnrIdList.size() > 0) {
//            //将spnrID获取报文并存入hbase   记录获取情况
//            Tuple4<Long, Long, Long, Long> getSpnrXmlStatus = saveXmlToHbase(spnrIdList, date, Constants.SPNR_FLAG, env);
//            totalGetXmlStatus.f0 = getSpnrXmlStatus.f0;
//            totalGetXmlStatus.f1 = getSpnrXmlStatus.f1;
//        } else {
//            HBaseUtils.insertDataLog(date, totalGetXmlStatus.f0, totalGetXmlStatus.f1,
//                    totalGetXmlStatus.f2, totalGetXmlStatus.f3);
//            System.exit(0);
//        }
//
//        String endTimeSpnrService = DateUtil.formatDate(System.currentTimeMillis(), Constants.RUN_LOG_PATTERN);
//        HBaseUtils.insertRunLog(startTimeSpnrService, endTimeSpnrService, totalGetXmlStatus.f1,
//                totalGetXmlStatus.f0, Constants.SPNR_SERVICE, date,
//                Utils.getHbaseTbaleName(Constants.HBASE_TABLE_ORDER_SPNR, date));
//
//        String startTimeSummaryService = DateUtil.formatDate(System.currentTimeMillis(), Constants.RUN_LOG_PATTERN);
//
//        //判断报文是否正常获取并写入hbase表  来更新fetch_log表状态
//        ckeckGetXmlWhetherSucceed(spnrIdList, date);
//        //任务结束时间
//        String endTimeSummaryService = DateUtil.formatDate(System.currentTimeMillis(), Constants.RUN_LOG_PATTERN);
//        //记录日志
//        HBaseUtils.insertRunLog(startTimeSummaryService, endTimeSummaryService, totalGetXmlStatus.f3,
//                totalGetXmlStatus.f2, Constants.SUMMARY_SERVICE, date,
//                Utils.getHbaseTbaleName(Constants.HBASE_TABLE_ORDER_SPNR, date));
//
//        // 记录迁移数据统计表
//        HBaseUtils.insertDataLog(date, totalGetXmlStatus.f0, totalGetXmlStatus.f1,
//                totalGetXmlStatus.f2, totalGetXmlStatus.f3);
//
//    }
//
//    /**
//     * 判断spnrID是否正常获取到spnr报文和summary报文
//     * 成功获取时 更新fetch_log表对应状态
//     *
//     * @param spnrIdList
//     */
//    public static void ckeckGetXmlWhetherSucceed(List<String> spnrIdList, String date) throws IOException {
//        LOGGER.info("开始更新fetch_log表spnrID获取报文状态");
//        for (String spnrID : spnrIdList) {
//            Map<String, String> map = HBaseUtils.searchByRowKey(Utils.getHbaseTbaleName(Constants.HBASE_TABLE_ORDER_SPNR, date), spnrID);
//            if (StringUtils.isNotBlank(map.get(Constants.SPNR_XML))) {
//                HBaseUtils.insertIndex(Constants.HBASE_TABLE_ORDER_FETCH_LOG, spnrID, Constants.SPNR_FLAG, Constants.GET_XML_SUCCESS_MARK, false);
//            } else {
//                HBaseUtils.insertIndex(Constants.HBASE_TABLE_ORDER_FETCH_LOG, spnrID, Constants.SPNR_FLAG, Constants.GET_XML_FAIL_MARK, false);
//            }
//        }
//        LOGGER.info("fetch_log表获取报文状态更新完成");
//
//    }
//
//    /**
//     * 获取报文并保存到hbase表
//     *
//     * @param spnrIdList
//     * @param date
//     * @param messageType 失败类型 第一次获取报文时传null即可
//     * @param env
//     * @return 获取报文情况记录
//     * @throws Exception
//     */
//    static Tuple4<Long, Long, Long, Long> saveXmlToHbase(List spnrIdList, String date, String messageType, ExecutionEnvironment env) throws Exception {
//        //记录获取报文情况 f0:spnr报文获取成功条数  f1:spnr报文获取失败条数 f3：summary报文获取成功条数 f4：summary报文获取失败条数
//        Tuple4<Long, Long, Long, Long> totalCount = new Tuple4<>(0L, 0L, 0L, 0L);
//
//        //将spnrIdList转换成DataSource
//        //rebalance：均匀地重新平衡 DataSet 的并行分区以消除数据倾斜。
//        DataSet<String> dataSet = env.fromCollection(spnrIdList).rebalance();
//
//        //创建需要广播的数据
//        List<String> broadList = new ArrayList<>();
//        //第一位是可连续失败次数阀值
//        broadList.add(0, Constants.CONTINUOUS_FAILURE_THRESHOLD);
//        //第二位是总失败次数阀值
//        broadList.add(1, Constants.TOTAL_FAILURE_THRESHOLD);
//        //第三位是日期
//        broadList.add(2, date);
//        //将需要广播的变量转换成DataSet
//        DataSet<String> broadSource = env.fromCollection(broadList);
//
//        //根据传入的失败类型调用不同获取报文的方法   第一次获取报文的spnrID两个方法都要执行
//        if (StringUtils.isBlank(messageType) || Constants.SPNR_FLAG.equals(messageType)) {
//            orderSpnrMessageToHbase(dataSet, broadSource, date);
//        }
//        //获取summary报文写入到order_message表
//        if (StringUtils.isBlank(messageType) || Constants.SUMMARY_FLAG.equals(messageType)) {
//            orderSummaryMessageToHbase(dataSet, broadSource, date);
//        }
//
//        JobExecutionResult orderSpnrSummaryMessageToHbase = env.execute("OrderSpnrSummaryMessageToHbase");
//
//        //计算报文获取情况 成功获取到报文的数量 = 获取到的spnrID总量 - 指定日期指定报文获取成功数量
//        if (StringUtils.isBlank(messageType) || Constants.SPNR_FLAG.equals(messageType)) {
//            //要获取报文的spnrID总数
//            Integer total = spnrIdList.size();
//            //以日期和报文获取类型作为条件检索报文获取成功的spnrID条数
//            Integer success = Integer.valueOf(orderSpnrSummaryMessageToHbase.getAccumulatorResult("get_spnr_xml" + "_" + date) + "");
//            totalCount.f0 = Long.valueOf(success);
//            totalCount.f1 = Long.valueOf(total - success);
//            LOGGER.info("本次获取的SPNR报文数：" + totalCount.f0);
//        }
//
//        if (StringUtils.isBlank(messageType) || Constants.SUMMARY_FLAG.equals(messageType)) {
//            //要获取报文的spnrID总数
//            Integer total = spnrIdList.size();
//            //以日期和报文获取类型作为条件检索报文获取成功的spnrID条数
//            Integer success = Integer.valueOf(orderSpnrSummaryMessageToHbase.getAccumulatorResult("get_summary_xml" + "_" + date) + "");
//            totalCount.f2 = Long.valueOf(success);
//            totalCount.f3 = Long.valueOf(total - success);
//            LOGGER.info("本次获取的SUMMARY报文数：" + totalCount.f2);
//        }
//
//        return totalCount;
//    }
//
//
//    /**
//     * 获取summary报文写入hbase
//     *
//     * @param source
//     * @param date
//     */
//    @SuppressWarnings("unchecked")
//    private static void orderSummaryMessageToHbase(DataSet source, DataSet<String> broadSource, String date) throws Exception {
//
//        DataSet<OrderMessageModel> result = source
//                //将数据进行分组 每一百的spnrID为一组  剩余 不足一百的spnrID为一组
////                .mapPartition(new GroupingSpnrIdMapPartition())
//                //获取报文
////                .flatMap(new GetSummaryXmlFlatMap())
//                .map(new GetSummaryXmlMap())
//                //设置广播变量
//                .withBroadcastSet(broadSource, Constants.BROADCAST_MAP_NAME)
//                .filter(new FilterFunction<OrderMessageModel>() {
//                    @Override
//                    public boolean filter(OrderMessageModel value) throws Exception {
//                        return value != null ;
//                    }
//                })
//                .map(new RichMapFunction<OrderMessageModel, OrderMessageModel>() {
//                    LongCounter counterAcc = new LongCounter();
//
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//                        getRuntimeContext().addAccumulator("get_summary_xml" + "_" + date, counterAcc);
//                    }
//
//                    @Override
//                    public OrderMessageModel map(OrderMessageModel value) throws Exception {
//                        if (StringUtils.isNotBlank(value.getSummaryXml())) {
//                            counterAcc.add(1L);
//                        }
//                        return value;
//                    }
//                });
//        //输出到hbase表
//        result.output(new HBaseOutputFormat(Utils.getHbaseTbaleName(Constants.HBASE_TABLE_ORDER_SPNR, date),
//                Constants.HBASE_TABLE_COLUMN_FAMILY, Constants.HBASE_TABLE_ORDER_FETCH_LOG,
//                Constants.HBASE_TABLE_COLUMN_FAMILY, Constants.SUMMARY_FLAG, false,
//                Constants.HBASE_FLUSH_NUM,date));
//    }
//
//    /**
//     * 获取summary报文写入hbase
//     *
//     * @param source
//     */
//    @SuppressWarnings("unchecked")
//    private static void orderSpnrMessageToHbase(DataSet<String> source, DataSet<String> broadSet, String date) throws Exception {
//
//        DataSet<OrderMessageModel> result = source
//                //获取spnr报文
//                .map(new getSpnrXmlMap())
//                //设置广播变量
//                .withBroadcastSet(broadSet, Constants.BROADCAST_MAP_NAME)
//                .filter(new FilterFunction<OrderMessageModel>() {
//                    @Override
//                    public boolean filter(OrderMessageModel value) throws Exception {
//                        return value != null ;
//                    }
//                })
//                .map(new RichMapFunction<OrderMessageModel, OrderMessageModel>() {
//                    LongCounter counterAcc = new LongCounter();
//
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//                        getRuntimeContext().addAccumulator("get_spnr_xml" + "_" + date, counterAcc);
//                    }
//
//
//                    @Override
//                    public OrderMessageModel map(OrderMessageModel value) throws Exception {
//                        if (StringUtils.isNotBlank(value.getSpnrXml())) {
//                            counterAcc.add(1L);
//                        }
//                        return value;
//                    }
//                });
//
//        //输出到hbase表
//        result.output(new HBaseOutputFormat(Utils.getHbaseTbaleName(Constants.HBASE_TABLE_ORDER_SPNR, date),
//                Constants.HBASE_TABLE_COLUMN_FAMILY, Constants.HBASE_TABLE_ORDER_FETCH_LOG
//                , Constants.HBASE_TABLE_COLUMN_FAMILY, Constants.SPNR_FLAG, false,
//                Constants.HBASE_FLUSH_NUM,date));
//
//    }
//
//
//    /**
//     * 获取指定日期指定报文类型的spnrId集合
//     *
//     * @param date
//     * @return
//     * @throws HBaseException
//     */
//    static List getSpnrIdList(String date, String xmlType) throws HBaseException {
//
//        SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes(Constants.HBASE_TABLE_COLUMN_FAMILY),
//                Bytes.toBytes(xmlType), CompareFilter.CompareOp.NOT_EQUAL, new SubstringComparator(Constants.GET_XML_SUCCESS_MARK));
//        filter2.setFilterIfMissing(true);
//        FilterList filterLst = new FilterList(FilterList.Operator.MUST_PASS_ALL);
//        filterLst.addFilter(filter2);
//        String tableName = Constants.HBASE_TABLE_ORDER_FETCH_LOG;
//        Tuple2<Integer, List<Map<String, String>>> rst = HBaseUtils.selectAllByPageFilters(
//                tableName, HBaseUtils.ROWKEY_FIRST, filterLst, date, null);
//        List spnrList = new ArrayList();
//
//        for (Map<String, String> map : rst.f1) {
//            String mark = map.get(xmlType);
//            //等于1代表成功获取到报文  过滤掉
//            if (!Constants.GET_XML_SUCCESS_MARK.equals(mark)) {
//                spnrList.add(map.get(Constants.HBASE_TABLE_COLUMN_ROWKEY));
//            }
//        }
//
//        return spnrList;
//    }
//
//
//    /**
//     * 获取需要删除的spnrId list集合
//     *
//     * @return
//     * @throws HBaseException
//     */
//    public static void deleteSpnrIdList(String date) throws HBaseException, IOException {
//
//        LOGGER.info("开始根据报文获取状态删除报文获取成功的fetch_log表数据");
//        //按照年份将spnrID获取出来
//        SingleColumnValueFilter SummaryFilter = new SingleColumnValueFilter(Bytes.toBytes(Constants.HBASE_TABLE_COLUMN_FAMILY),
//                Bytes.toBytes(Constants.SUMMARY_FLAG), CompareFilter.CompareOp.EQUAL, new SubstringComparator(Constants.GET_XML_SUCCESS_MARK));
//        SingleColumnValueFilter spnrFilter = new SingleColumnValueFilter(Bytes.toBytes(Constants.HBASE_TABLE_COLUMN_FAMILY),
//                Bytes.toBytes(Constants.SPNR_FLAG), CompareFilter.CompareOp.EQUAL, new SubstringComparator(Constants.GET_XML_SUCCESS_MARK));
//        FilterList filterLst = new FilterList(FilterList.Operator.MUST_PASS_ALL);
//
//        spnrFilter.setFilterIfMissing(true);
//        SummaryFilter.setFilterIfMissing(true);
//
//        filterLst.addFilter(spnrFilter);
//        filterLst.addFilter(SummaryFilter);
//        //获取表名
//        String tableName = Constants.HBASE_TABLE_ORDER_FETCH_LOG;
//        Tuple2<Integer, List<Map<String, String>>> rst = HBaseUtils.selectAllByPageFilters(
//                tableName, HBaseUtils.ROWKEY_FIRST, filterLst, date, null);
//        List spnrList = new ArrayList();
//
//        for (Map<String, String> map : rst.f1
//        ) {
//            //获取rowkey
//            spnrList.add(map.get(Constants.HBASE_TABLE_COLUMN_ROWKEY));
//        }
//        //根据rowkey删除行数据
//        HBaseUtils.deleteSpecifiedLine(tableName, spnrList);
//        HBaseUtils.deleteSpecifiedLine(Constants.HBASE_TABLE_ORDER_SPNRID_MISSING, spnrList);
//
//        LOGGER.info("fetch_log表数据删除完成");
//    }
//
//}
