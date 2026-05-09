package com.travelsky.trp.usercenter.data.analysis.model.dim;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 时间维表
 */
public class TimeDimModel {

    /** 主键 */
    @JsonProperty("TIME_KEY")
    private String timeKey;

    /** 时间值 */
    @JsonProperty("TIMEVALUE")
    private String timevalue;

    /** 小时数 */
    @JsonProperty("TIME_HOUR")
    private Integer timeHour;

    /** 分钟数 */
    @JsonProperty("TIME_MINUTE")
    private Integer timeMinute;

    /** 时间段 */
    @JsonProperty("TIME_PERIOD")
    private String timePeriod;

    public String getTimeKey() {
        return timeKey;
    }

    public void setTimeKey(String timeKey) {
        this.timeKey = timeKey;
    }

    public String getTimevalue() {
        return timevalue;
    }

    public void setTimevalue(String timevalue) {
        this.timevalue = timevalue;
    }

    public Integer getTimeHour() {
        return timeHour;
    }

    public void setTimeHour(Integer timeHour) {
        this.timeHour = timeHour;
    }

    public Integer getTimeMinute() {
        return timeMinute;
    }

    public void setTimeMinute(Integer timeMinute) {
        this.timeMinute = timeMinute;
    }

    public String getTimePeriod() {
        return timePeriod;
    }

    public void setTimePeriod(String timePeriod) {
        this.timePeriod = timePeriod;
    }

    @Override
    public String toString() {
        return "TimeDimModel{" +
                "timeKey='" + timeKey + '\'' +
                ", timevalue='" + timevalue + '\'' +
                ", timeHour=" + timeHour +
                ", timeMinute=" + timeMinute +
                ", timePeriod='" + timePeriod + '\'' +
                '}';
    }
}