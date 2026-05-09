package com.travelsky.trp.usercenter.data.analysis.transform.zsf;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import com.travelsky.dataplatform.utils.FlinkDorisUtils;
import com.travelsky.trp.usercenter.data.analysis.model.dim.DateDimModel;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class DateDimTrans {
    public static Logger log = LoggerFactory.getLogger(DateDimTrans.class);

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, " DateDimTrans");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 1. 生成日期范围（例如 2020-01-01 到 2030-12-31）
        List<String> dateList = generateDateRange("20610101", "20631231");

        // 2. 创建 DataStream<String>
        DataStream<String> dateStream = env.fromCollection(dateList);

        DataStream<DateDimModel> dateDimStream = dateStream.map(record -> {
            String dateValue = record; // e.g., "20251225"
            // 解析为 LocalDate
            LocalDate date = LocalDate.parse(dateValue.substring(0, 4) + "-" +
                    dateValue.substring(4, 6) + "-" +
                    dateValue.substring(6, 8));

            int year = date.getYear();
            int month = date.getMonthValue();
            int day = date.getDayOfMonth();

            // 获取星期
            String dateWeek = date.getDayOfWeek()
                    .getDisplayName(java.time.format.TextStyle.SHORT, Locale.ENGLISH);
            // 获取月份
            String dateMonth = date.getMonth()
                    .getDisplayName(java.time.format.TextStyle.SHORT, Locale.ENGLISH);
            // 季度
            String dateQuarter;
            if (month <= 3) {
                dateQuarter = "Q1";
            } else if (month <= 6) {
                dateQuarter = "Q2";
            } else if (month <= 9) {
                dateQuarter = "Q3";
            } else {
                dateQuarter = "Q4";
            }

            // 年份
            String dateYear = String.valueOf(year);
            String dateKey = dateValue;
            // 节假日判断
            List<String> holidays = new ArrayList<>();
            // 暑假：7月、8月
            if (month == 7 || month == 8) {
                holidays.add("Summer");
            }
            // 春运（近似）：1月10日 ~ 2月25日
            if ((month == 1 && day >= 10) || (month == 2 && day <= 25)) {
                holidays.add("SpringFestivalTravel");
            }
            // 五一前后：4月28日 ~ 5月7日
            if ((month == 4 && day >= 28) || (month == 5 && day <= 7)) {
                holidays.add("LabourDay");
            }
            // 十一前后：9月28日 ~ 10月10日
            if ((month == 9 && day >= 28) || (month == 10 && day <= 10)) {
                holidays.add("NationalDay");
            }
            // 合并结果，用逗号分隔（如 "Summer,LabourDay"）
            String holidayStr = String.join(",", holidays);


            // 创建新对象
            DateDimModel dateDimModel = new DateDimModel();
            dateDimModel.setDateKey(dateKey);
            dateDimModel.setDatevalue(dateValue);
            dateDimModel.setDateWeek(dateWeek);
            dateDimModel.setDateMonth(dateMonth);
            dateDimModel.setDateQuarter(dateQuarter);
            dateDimModel.setDateYear(dateYear);
            dateDimModel.setHolidays(holidayStr);

            return dateDimModel;
        });


        log.info("连接 Doris 参数: DB={}, Table={}, FE={}, BE={}, User={}",
                Constants.DIM_DB, "T_DIM_DATE_DIM", Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT, Constants.DIM_USER);
        DorisSink<DateDimModel> dorisSink = FlinkDorisUtils.creatDorisSink(
                Constants.DIM_DB,
                "T_DIM_DATE_DIM",
                Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT,
                Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT,
                Constants.DIM_USER,
                Constants.DIM_PWD);
        // 数据写入 Doris
        dateDimStream.sinkTo(dorisSink);

        env.execute("Date Dimension Generation Job");

    }





    public static List<String> generateDateRange(String start, String end) {
        LocalDate startDate = LocalDate.parse(start, DateTimeFormatter.ofPattern("yyyyMMdd"));
        LocalDate endDate = LocalDate.parse(end, DateTimeFormatter.ofPattern("yyyyMMdd"));

        List<String> dates = new ArrayList<>();
        LocalDate current = startDate;
        while (!current.isAfter(endDate)) {
            dates.add(current.format(DateTimeFormatter.ofPattern("yyyyMMdd")));
            current = current.plusDays(1);
        }
        return dates;
    }
}
