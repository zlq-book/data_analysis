package com.travelsky.trp.usercenter.data.analysis.model.dwd.fact;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;

public class VariflightFlightModel {

    @JsonProperty("FlightNo")
    private String flightNo;

    @JsonProperty("FlightDep")
    private String flightDep;

    @JsonProperty("FlightArr")
    private String flightArr;

    @JsonProperty("FlightDepcode")
    private String flightDepcode;

    @JsonProperty("FlightArrcode")
    private String flightArrcode;

    @JsonProperty("FlightDepAirport")
    private String flightDepAirport;

    @JsonProperty("FlightArrAirport")
    private String flightArrAirport;

    @JsonProperty("FlightCompany")
    private String flightCompany;

    @JsonProperty("FlightDeptimePlanDate")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime flightDeptimePlanDate;

    @JsonProperty("FlightArrtimePlanDate")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime flightArrtimePlanDate;

    @JsonProperty("FlightDeptimeDate")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime flightDeptimeDate;

    @JsonProperty("FlightArrtimeDate")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime flightArrtimeDate;

    @JsonProperty("FlightState")
    private String flightState;

    @JsonProperty("org_timezone")
    private Integer orgTimezone;

    @JsonProperty("dst_timezone")
    private Integer dstTimezone;

    @JsonProperty("FlightDuration")
    private Integer flightDuration;

    @JsonProperty("fcategory")
    private String fcategory;

    @JsonProperty("fservice")
    private String fservice;

    @JsonProperty("ftype")
    private String ftype;

    // 新增字段1：generic（机型相关，JSON中为字符串类型）
    @JsonProperty("generic")
    private String generic;

    // 新增字段2：FlightIngateTime（进港时间，可能为空，格式与其他时间字段一致）
    @JsonProperty("FlightIngateTime")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime flightIngateTime;

    // 新增字段3：FlightOutgateTime（出港时间，可能为空，格式与其他时间字段一致）
    @JsonProperty("FlightOutgateTime")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime flightOutgateTime;

    // Getters and Setters（包含新增字段）
    public String getFlightNo() { return flightNo; }
    public void setFlightNo(String flightNo) { this.flightNo = flightNo; }

    public String getFlightDep() { return flightDep; }
    public void setFlightDep(String flightDep) { this.flightDep = flightDep; }

    public String getFlightArr() { return flightArr; }
    public void setFlightArr(String flightArr) { this.flightArr = flightArr; }

    public String getFlightDepcode() { return flightDepcode; }
    public void setFlightDepcode(String flightDepcode) { this.flightDepcode = flightDepcode; }

    public String getFlightArrcode() { return flightArrcode; }
    public void setFlightArrcode(String flightArrcode) { this.flightArrcode = flightArrcode; }

    public String getFlightDepAirport() { return flightDepAirport; }
    public void setFlightDepAirport(String flightDepAirport) { this.flightDepAirport = flightDepAirport; }

    public String getFlightArrAirport() { return flightArrAirport; }
    public void setFlightArrAirport(String flightArrAirport) { this.flightArrAirport = flightArrAirport; }

    public String getFlightCompany() { return flightCompany; }
    public void setFlightCompany(String flightCompany) { this.flightCompany = flightCompany; }

    public LocalDateTime getFlightDeptimePlanDate() { return flightDeptimePlanDate; }
    public void setFlightDeptimePlanDate(LocalDateTime flightDeptimePlanDate) { this.flightDeptimePlanDate = flightDeptimePlanDate; }

    public LocalDateTime getFlightArrtimePlanDate() { return flightArrtimePlanDate; }
    public void setFlightArrtimePlanDate(LocalDateTime flightArrtimePlanDate) { this.flightArrtimePlanDate = flightArrtimePlanDate; }

    public LocalDateTime getFlightDeptimeDate() { return flightDeptimeDate; }
    public void setFlightDeptimeDate(LocalDateTime flightDeptimeDate) { this.flightDeptimeDate = flightDeptimeDate; }

    public LocalDateTime getFlightArrtimeDate() { return flightArrtimeDate; }
    public void setFlightArrtimeDate(LocalDateTime flightArrtimeDate) { this.flightArrtimeDate = flightArrtimeDate; }

    public String getFlightState() { return flightState; }
    public void setFlightState(String flightState) { this.flightState = flightState; }

    public Integer getOrgTimezone() { return orgTimezone; }
    public void setOrgTimezone(Integer orgTimezone) { this.orgTimezone = orgTimezone; }

    public Integer getDstTimezone() { return dstTimezone; }
    public void setDstTimezone(Integer dstTimezone) { this.dstTimezone = dstTimezone; }

    public Integer getFlightDuration() { return flightDuration; }
    public void setFlightDuration(Integer flightDuration) { this.flightDuration = flightDuration; }

    public String getFcategory() { return fcategory; }
    public void setFcategory(String fcategory) { this.fcategory = fcategory; }

    public String getFservice() { return fservice; }
    public void setFservice(String fservice) { this.fservice = fservice; }

    public String getFtype() { return ftype; }
    public void setFtype(String ftype) { this.ftype = ftype; }

    // 新增字段的 Getter 和 Setter
    public String getGeneric() { return generic; }
    public void setGeneric(String generic) { this.generic = generic; }

    public LocalDateTime getFlightIngateTime() { return flightIngateTime; }
    public void setFlightIngateTime(LocalDateTime flightIngateTime) { this.flightIngateTime = flightIngateTime; }

    public LocalDateTime getFlightOutgateTime() { return flightOutgateTime; }
    public void setFlightOutgateTime(LocalDateTime flightOutgateTime) { this.flightOutgateTime = flightOutgateTime; }

    @Override
    public String toString() {
        return "VariflightFlightInfo{" +
                "flightNo='" + flightNo + '\'' +
                ", flightDep='" + flightDep + '\'' +
                ", flightArr='" + flightArr + '\'' +
                ", flightDepcode='" + flightDepcode + '\'' +
                ", flightArrcode='" + flightArrcode + '\'' +
                ", flightDepAirport='" + flightDepAirport + '\'' +
                ", flightArrAirport='" + flightArrAirport + '\'' +
                ", flightCompany='" + flightCompany + '\'' +
                ", flightDeptimePlanDate=" + flightDeptimePlanDate +
                ", flightArrtimePlanDate=" + flightArrtimePlanDate +
                ", flightDeptimeDate=" + flightDeptimeDate +
                ", flightArrtimeDate=" + flightArrtimeDate +
                ", flightState='" + flightState + '\'' +
                ", orgTimezone=" + orgTimezone +
                ", dstTimezone=" + dstTimezone +
                ", flightDuration=" + flightDuration +
                ", fcategory='" + fcategory + '\'' +
                ", fservice='" + fservice + '\'' +
                ", ftype='" + ftype + '\'' +
                ", generic='" + generic + '\'' +  // 新增字段
                ", flightIngateTime=" + flightIngateTime +  // 新增字段
                ", flightOutgateTime=" + flightOutgateTime +  // 新增字段
                '}';
    }
}