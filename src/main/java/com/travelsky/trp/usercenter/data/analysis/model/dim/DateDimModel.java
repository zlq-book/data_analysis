package com.travelsky.trp.usercenter.data.analysis.model.dim;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 日期维表
 */
public class DateDimModel {

    /** 主键 */
    @JsonProperty("DATE_KEY")
    private String dateKey;

    /** 日期值 */
    @JsonProperty("DATEVALUE")
    private String datevalue;

    /** 周几 */
    @JsonProperty("DATE_WEEK")
    private String dateWeek;

    /** 月份 */
    @JsonProperty("DATE_MONTH")
    private String dateMonth;

    /** 季度 */
    @JsonProperty("DATE_QUARTER")
    private String dateQuarter;

    /** 年份 */
    @JsonProperty("DATE_YEAR")
    private String dateYear;

    /** 节假日标识 */
    @JsonProperty("HOLIDAYS")
    private String holidays;

    public String getDateKey() {
        return dateKey;
    }

    public void setDateKey(String dateKey) {
        this.dateKey = dateKey;
    }

    public String getDatevalue() {
        return datevalue;
    }

    public void setDatevalue(String datevalue) {
        this.datevalue = datevalue;
    }

    public String getDateWeek() {
        return dateWeek;
    }

    public void setDateWeek(String dateWeek) {
        this.dateWeek = dateWeek;
    }

    public String getDateMonth() {
        return dateMonth;
    }

    public void setDateMonth(String dateMonth) {
        this.dateMonth = dateMonth;
    }

    public String getDateQuarter() {
        return dateQuarter;
    }

    public void setDateQuarter(String dateQuarter) {
        this.dateQuarter = dateQuarter;
    }

    public String getDateYear() {
        return dateYear;
    }

    public void setDateYear(String dateYear) {
        this.dateYear = dateYear;
    }

    public String getHolidays() {
        return holidays;
    }

    public void setHolidays(String holidays) {
        this.holidays = holidays;
    }

    @Override
    public String toString() {
        return "DateDimModel{" +
                "dateKey='" + dateKey + '\'' +
                ", datevalue='" + datevalue + '\'' +
                ", dateWeek='" + dateWeek + '\'' +
                ", dateMonth='" + dateMonth + '\'' +
                ", dateQuarter='" + dateQuarter + '\'' +
                ", dateYear='" + dateYear + '\'' +
                ", holidays='" + holidays + '\'' +
                '}';
    }
}