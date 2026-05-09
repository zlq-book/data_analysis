package com.travelsky.trp.usercenter.data.analysis.model.dwd.fact;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;

/**
 * 附加服务_改期收费出EMD票_航段事实表
 */
public class ChargefeeTikFactModal {

    /**
     * 主键：EMD出票日期+EMD票号+起飞机场+到达机场
     */
    @JsonProperty("PK_ID")
    private String pkId;

    /**
     * 串联编号
     */
    @JsonProperty("FK_SERIES_NUMBER")
    private String fkSeriesNumber;

    /**
     * EMD出票日期
     */
    @JsonProperty("FK_CHANGEFEETIK_DATE")
    private String fkChangefeetikDate;

    /**
     * EMD出票时间
     */
    @JsonProperty("FK_CHANGEFEETIK_TIME")
    private String fkChangefeetikTime;

    /**
     * 起飞机场
     */
    @JsonProperty("DEPAIRPORT")
    private String depairport;

    /**
     * 到达机场
     */
    @JsonProperty("ARRIAIRPORT")
    private String arriairport;

    /**
     * 航班日期
     */
    @JsonProperty("FK_CHANGEFEESTART_DATE")
    private String fkChangefeestartDate;

    /**
     * 航班时间
     */
    @JsonProperty("FK_CHANGEFEESTART_TIME")
    private String fkChangefeestartTime;

    /**
     * 市场航司（二字码）
     */
    @JsonProperty("AIRLINE_CODE")
    private String airlineCode;

    /**
     * 市场航班号（数字）
     */
    @JsonProperty("FLIGHT_NUMBER")
    private String flightNumber;

    /**
     * 乘机人英文姓
     */
    @JsonProperty("EN_LAST_NAME")
    private String enLastName;

    /**
     * 乘机人英文名
     */
    @JsonProperty("EN_FIRST_NAME")
    private String enFirstName;

    /**
     * 乘机人中文姓名
     */
    @JsonProperty("CN_NAME")
    private String cnName;

    /**
     * 乘机人类型
     */
    @JsonProperty("PASSENGER_TYPE")
    private String passengerType;

    /**
     * 乘机人证件类型
     */
    @JsonProperty("CERT_TYPE")
    private String certType;

    /**
     * 乘机人证件号
     */
    @JsonProperty("CERT_NUMBER")
    private String certNumber;

    /**
     * 乘机人年龄
     */
    @JsonProperty("PASSENGER_AGE")
    private Integer passengerAge;

    /**
     * 乘机人
     */
    @JsonProperty("FK_PASSENGER_USER_TID")
    private String fkPassengerUserTid;

    /**
     * 航程
     */
    @JsonProperty("FK_CHANGEFEE_SEG")
    private String fkChangefeeSeg;

    /**
     * 国内国际标识
     */
    @JsonProperty("AK_DIMARK")
    private String akDimark;

    /**
     * 关联票号
     */
    @JsonProperty("AK_TIKNUM")
    private String akTiknum;

    /**
     * EMD票号
     */
    @JsonProperty("AK_EMDNUM")
    private String akEmdnum;

    /**
     * EMD票状态
     */
    @JsonProperty("AK_EMD_STATUS")
    private String akEmdStatus;

    /**
     * 变更前舱位
     */
    @JsonProperty("AK_CHARGETIK_CABIN")
    private String akChargetikCabin;

    /**
     * 变更后舱位
     */
    @JsonProperty("AK_CHARGETIKNEW_CABIN")
    private String akChargetiknewCabin;

    /**
     * 原币种
     */
    @JsonProperty("AK_CURRENCY")
    private String akCurrency;

    /**
     * 金额原币种
     */
    @JsonProperty("PAY_AMOUNT")
    private Double payAmount;

    /**
     * EMD出票office
     */
    @JsonProperty("AK_EMDTIKCHANNEL")
    private String akEmdtikchannel;

    /**
     * EMD出票Office对应的IataCode
     */
    @JsonProperty("AK_EMDIATACODE")
    private String akEmdiatacode;

    /**
     * EMD类型
     */
    @JsonProperty("AK_EMDTYPE")
    private String akEmdtype;

    /**
     * 收费改期EMD出票计数
     */
    @JsonProperty("CHARGE_RESCHEDULE_EMD_COUNT")
    private Integer chargeRescheduleEmdCount = 1;

    /**
     * 数据是否启用
     */
    @JsonProperty("DATA_ACTIVE")
    private Boolean dataActive;

    /**
     * 数据启用时间
     */
    @JsonProperty("DATA_ACTIVE_TIME")
    private String dataActiveTime;
    /**
     * 数据有效性
     */
    @JsonProperty("IS_VALID")
    private String isValid;

    /**
     * 本系统创建日期时间
     */
    @JsonProperty("SYSTEM_CREATETIME")
    private String systemCreatetime;

    /**
     * 本系统最后更新日期时间
     */
    @JsonProperty("SYSTEM_LAST_UPDATETIME")
    private String systemLastUpdatetime = LocalDateTime.now().toString();


    public String getPkId() {
        return pkId;
    }

    public void setPkId(String pkId) {
        this.pkId = pkId;
    }

    public String getFkSeriesNumber() {
        return fkSeriesNumber;
    }

    public void setFkSeriesNumber(String fkSeriesNumber) {
        this.fkSeriesNumber = fkSeriesNumber;
    }

    public String getDepairport() {
        return depairport;
    }

    public void setDepairport(String depairport) {
        this.depairport = depairport;
    }

    public String getArriairport() {
        return arriairport;
    }

    public void setArriairport(String arriairport) {
        this.arriairport = arriairport;
    }

    public String getAirlineCode() {
        return airlineCode;
    }

    public void setAirlineCode(String airlineCode) {
        this.airlineCode = airlineCode;
    }

    public String getFlightNumber() {
        return flightNumber;
    }

    public void setFlightNumber(String flightNumber) {
        this.flightNumber = flightNumber;
    }

    public String getEnLastName() {
        return enLastName;
    }

    public void setEnLastName(String enLastName) {
        this.enLastName = enLastName;
    }

    public String getEnFirstName() {
        return enFirstName;
    }

    public void setEnFirstName(String enFirstName) {
        this.enFirstName = enFirstName;
    }

    public String getCnName() {
        return cnName;
    }

    public void setCnName(String cnName) {
        this.cnName = cnName;
    }

    public String getPassengerType() {
        return passengerType;
    }

    public void setPassengerType(String passengerType) {
        this.passengerType = passengerType;
    }

    public String getCertType() {
        return certType;
    }

    public void setCertType(String certType) {
        this.certType = certType;
    }

    public String getCertNumber() {
        return certNumber;
    }

    public void setCertNumber(String certNumber) {
        this.certNumber = certNumber;
    }

    public Integer getPassengerAge() {
        return passengerAge;
    }

    public void setPassengerAge(Integer passengerAge) {
        this.passengerAge = passengerAge;
    }

    public String getFkPassengerUserTid() {
        return fkPassengerUserTid;
    }

    public void setFkPassengerUserTid(String fkPassengerUserTid) {
        this.fkPassengerUserTid = fkPassengerUserTid;
    }

    public String getAkDimark() {
        return akDimark;
    }

    public void setAkDimark(String akDimark) {
        this.akDimark = akDimark;
    }

    public String getAkTiknum() {
        return akTiknum;
    }

    public void setAkTiknum(String akTiknum) {
        this.akTiknum = akTiknum;
    }

    public String getAkEmdnum() {
        return akEmdnum;
    }

    public void setAkEmdnum(String akEmdnum) {
        this.akEmdnum = akEmdnum;
    }

    public String getAkEmdStatus() {
        return akEmdStatus;
    }

    public void setAkEmdStatus(String akEmdStatus) {
        this.akEmdStatus = akEmdStatus;
    }

    public String getAkChargetikCabin() {
        return akChargetikCabin;
    }

    public void setAkChargetikCabin(String akChargetikCabin) {
        this.akChargetikCabin = akChargetikCabin;
    }

    public String getAkChargetiknewCabin() {
        return akChargetiknewCabin;
    }

    public void setAkChargetiknewCabin(String akChargetiknewCabin) {
        this.akChargetiknewCabin = akChargetiknewCabin;
    }

    public String getAkCurrency() {
        return akCurrency;
    }

    public void setAkCurrency(String akCurrency) {
        this.akCurrency = akCurrency;
    }

    public Double getPayAmount() {
        return payAmount;
    }

    public void setPayAmount(Double payAmount) {
        this.payAmount = payAmount;
    }

    public String getAkEmdtikchannel() {
        return akEmdtikchannel;
    }

    public void setAkEmdtikchannel(String akEmdtikchannel) {
        this.akEmdtikchannel = akEmdtikchannel;
    }

    public String getAkEmdiatacode() {
        return akEmdiatacode;
    }

    public void setAkEmdiatacode(String akEmdiatacode) {
        this.akEmdiatacode = akEmdiatacode;
    }

    public String getAkEmdtype() {
        return akEmdtype;
    }

    public void setAkEmdtype(String akEmdtype) {
        this.akEmdtype = akEmdtype;
    }

    public Integer getChargeRescheduleEmdCount() {
        return chargeRescheduleEmdCount;
    }

    public void setChargeRescheduleEmdCount(Integer chargeRescheduleEmdCount) {
        this.chargeRescheduleEmdCount = chargeRescheduleEmdCount;
    }


    public String getSystemLastUpdatetime() {
        return systemLastUpdatetime;
    }

    public void setSystemLastUpdatetime(String systemLastUpdatetime) {
        this.systemLastUpdatetime = systemLastUpdatetime;
    }

    public String getFkChangefeetikDate() {
        return fkChangefeetikDate;
    }

    public void setFkChangefeetikDate(String fkChangefeetikDate) {
        this.fkChangefeetikDate = fkChangefeetikDate;
    }

    public String getFkChangefeetikTime() {
        return fkChangefeetikTime;
    }

    public void setFkChangefeetikTime(String fkChangefeetikTime) {
        this.fkChangefeetikTime = fkChangefeetikTime;
    }

    public String getFkChangefeestartDate() {
        return fkChangefeestartDate;
    }

    public void setFkChangefeestartDate(String fkChangefeestartDate) {
        this.fkChangefeestartDate = fkChangefeestartDate;
    }

    public String getFkChangefeestartTime() {
        return fkChangefeestartTime;
    }

    public void setFkChangefeestartTime(String fkChangefeestartTime) {
        this.fkChangefeestartTime = fkChangefeestartTime;
    }

    public String getFkChangefeeSeg() {
        return fkChangefeeSeg;
    }

    public void setFkChangefeeSeg(String fkChangefeeSeg) {
        this.fkChangefeeSeg = fkChangefeeSeg;
    }

    public String getSystemCreatetime() {
        return systemCreatetime;
    }

    public void setSystemCreatetime(String systemCreatetime) {
        this.systemCreatetime = systemCreatetime;
    }

    public Boolean getDataActive() {
        return dataActive;
    }

    public void setDataActive(Boolean dataActive) {
        this.dataActive = dataActive;
    }

    public String getDataActiveTime() {
        return dataActiveTime;
    }

    public void setDataActiveTime(String dataActiveTime) {
        this.dataActiveTime = dataActiveTime;
    }

    public String getIsValid() {
        return isValid;
    }

    public void setIsValid(String isValid) {
        this.isValid = isValid;
    }

    @Override
    public String toString() {
        return "ChargefeeTikFactModal{" +
                "pkId='" + pkId + '\'' +
                ", fkSeriesNumber='" + fkSeriesNumber + '\'' +
                ", fkChangefeetikDate=" + fkChangefeetikDate +
                ", fkChangefeetikTime='" + fkChangefeetikTime + '\'' +
                ", depairport='" + depairport + '\'' +
                ", arriairport='" + arriairport + '\'' +
                ", fkChangefeestartDate=" + fkChangefeestartDate +
                ", fkChangefeestartTime='" + fkChangefeestartTime + '\'' +
                ", airlineCode='" + airlineCode + '\'' +
                ", flightNumber='" + flightNumber + '\'' +
                ", enLastName='" + enLastName + '\'' +
                ", enFirstName='" + enFirstName + '\'' +
                ", cnName='" + cnName + '\'' +
                ", passengerType='" + passengerType + '\'' +
                ", certType='" + certType + '\'' +
                ", certNumber='" + certNumber + '\'' +
                ", passengerAge=" + passengerAge +
                ", fkPassengerUserTid='" + fkPassengerUserTid + '\'' +
                ", fkChangefeeSeg='" + fkChangefeeSeg + '\'' +
                ", akDimark='" + akDimark + '\'' +
                ", akTiknum='" + akTiknum + '\'' +
                ", akEmdnum='" + akEmdnum + '\'' +
                ", akEmdStatus='" + akEmdStatus + '\'' +
                ", akChargetikCabin='" + akChargetikCabin + '\'' +
                ", akChargetiknewCabin='" + akChargetiknewCabin + '\'' +
                ", akCurrency='" + akCurrency + '\'' +
                ", payAmount=" + payAmount +
                ", akEmdtikchannel='" + akEmdtikchannel + '\'' +
                ", akEmdiatacode='" + akEmdiatacode + '\'' +
                ", akEmdtype='" + akEmdtype + '\'' +
                ", chargeRescheduleEmdCount=" + chargeRescheduleEmdCount +
                ", dataActive=" + dataActive +
                ", dataActiveTime='" + dataActiveTime + '\'' +
                ", isValid='" + isValid + '\'' +
                ", systemCreatetime='" + systemCreatetime + '\'' +
                ", systemLastUpdatetime='" + systemLastUpdatetime + '\'' +
                '}';
    }
}
