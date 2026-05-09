package com.travelsky.trp.usercenter.data.analysis.model.dim;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 航程维表
 */
public class SegDimModel {

    /** 主键 */
    @JsonProperty("SEGMENT_KEY")
    private String segmentKey;

    /** 航线 */
    @JsonProperty("AIRLINE")
    private String airline;

    /** 承运方航司 */
    @JsonProperty("OPERAT_AIRLINE")
    private String operatAirline;

    /** 承运方航班号 */
    @JsonProperty("OPERAT_FLIGHT_NUM")
    private String operatFlightNum;

    /** 起飞日期 */
    @JsonProperty("DEPARTURE_DATE")
    private String departureDate;

    /** 起飞时间 */
    @JsonProperty("DEPARTURE_TIME")
    private String departureTime;

    /** 到达日期 */
    @JsonProperty("ARRIVAL_DATE")
    private String arrivalDate;

    /** 到达时间 */
    @JsonProperty("ARRIVAL_TIME")
    private String arrivalTime;

    /** 飞行时长 */
    @JsonProperty("DURATION")
    private String duration;

    /** 机型 */
    @JsonProperty("AIRTYPE")
    private String airtype;

    /** 市场方航司 */
    @JsonProperty("MARKET_AIRLINE")
    private String marketAirline;

    /** 市场方航班号 */
    @JsonProperty("MARKET_FLIGHT_NUM")
    private String marketFlightNum;

    /** 航程距离 */
    @JsonProperty("DISTANCE_TPM")
    private String distanceTpm;

    /** 航班延误标识（Y/N） */
    @JsonProperty("IS_DELAYED")
    private String isDelayed;

    /** 航班是否取消（Y/N） */
    @JsonProperty("IS_CANCELLED")
    private String isCancelled;

    /** 是否代码共享航班（Y/N） */
    @JsonProperty("IS_CODE_SHARE")
    private String isCodeShare;

    /** 创建时间 */
    @JsonProperty("CREATE_TIME")
    private String createTime;

    /** 更新时间 */
    @JsonProperty("UPDATE_TIME")
    private String updateTime;

    // --- Getters and Setters ---

    public String getSegmentKey() {
        return segmentKey;
    }

    public void setSegmentKey(String segmentKey) {
        this.segmentKey = segmentKey;
    }

    public String getAirline() {
        return airline;
    }

    public void setAirline(String airline) {
        this.airline = airline;
    }

    public String getOperatAirline() {
        return operatAirline;
    }

    public void setOperatAirline(String operatAirline) {
        this.operatAirline = operatAirline;
    }

    public String getOperatFlightNum() {
        return operatFlightNum;
    }

    public void setOperatFlightNum(String operatFlightNum) {
        this.operatFlightNum = operatFlightNum;
    }

    public String getDepartureDate() {
        return departureDate;
    }

    public void setDepartureDate(String departureDate) {
        this.departureDate = departureDate;
    }

    public String getDepartureTime() {
        return departureTime;
    }

    public void setDepartureTime(String departureTime) {
        this.departureTime = departureTime;
    }

    public String getArrivalDate() {
        return arrivalDate;
    }

    public void setArrivalDate(String arrivalDate) {
        this.arrivalDate = arrivalDate;
    }

    public String getArrivalTime() {
        return arrivalTime;
    }

    public void setArrivalTime(String arrivalTime) {
        this.arrivalTime = arrivalTime;
    }

    public String getDuration() {
        return duration;
    }

    public void setDuration(String duration) {
        this.duration = duration;
    }

    public String getAirtype() {
        return airtype;
    }

    public void setAirtype(String airtype) {
        this.airtype = airtype;
    }

    public String getMarketAirline() {
        return marketAirline;
    }

    public void setMarketAirline(String marketAirline) {
        this.marketAirline = marketAirline;
    }

    public String getMarketFlightNum() {
        return marketFlightNum;
    }

    public void setMarketFlightNum(String marketFlightNum) {
        this.marketFlightNum = marketFlightNum;
    }

    public String getDistanceTpm() {
        return distanceTpm;
    }

    public void setDistanceTpm(String distanceTpm) {
        this.distanceTpm = distanceTpm;
    }

    public String getIsDelayed() {
        return isDelayed;
    }

    public void setIsDelayed(String isDelayed) {
        this.isDelayed = isDelayed;
    }

    public String getIsCancelled() {
        return isCancelled;
    }

    public void setIsCancelled(String isCancelled) {
        this.isCancelled = isCancelled;
    }

    public String getIsCodeShare() {
        return isCodeShare;
    }

    public void setIsCodeShare(String isCodeShare) {
        this.isCodeShare = isCodeShare;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }

    @Override
    public String toString() {
        return "SegDimModel{" +
                "segmentKey='" + segmentKey + '\'' +
                ", airline='" + airline + '\'' +
                ", operatAirline='" + operatAirline + '\'' +
                ", operatFlightNum='" + operatFlightNum + '\'' +
                ", departureDate='" + departureDate + '\'' +
                ", departureTime='" + departureTime + '\'' +
                ", arrivalDate='" + arrivalDate + '\'' +
                ", arrivalTime='" + arrivalTime + '\'' +
                ", duration='" + duration + '\'' +
                ", airtype='" + airtype + '\'' +
                ", marketAirline='" + marketAirline + '\'' +
                ", marketFlightNum='" + marketFlightNum + '\'' +
                ", distanceTpm='" + distanceTpm + '\'' +
                ", isDelayed='" + isDelayed + '\'' +
                ", isCancelled='" + isCancelled + '\'' +
                ", isCodeShare='" + isCodeShare + '\'' +
                ", createTime='" + createTime + '\'' +
                ", updateTime='" + updateTime + '\'' +
                '}';
    }
}