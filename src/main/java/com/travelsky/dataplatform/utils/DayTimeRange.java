package com.travelsky.dataplatform.utils;

/**
 * 用于封装一天的开始时间和结束时间
 */
public class DayTimeRange {
    private final String startTime;
    private final String endTime;

    public DayTimeRange(String startTime, String endTime) {
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public String getStartTime() {
        return startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    @Override
    public String toString() {
        return "DayTimeRange{" +
                "startTime='" + startTime + '\'' +
                ", endTime='" + endTime + '\'' +
                '}';
    }
}
