package com.travelsky.trp.usercenter.data.analysis.model.dwd.fact;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;

/**
 * 附加服务选餐退票航段事实表实体类-T_DWD_MEAL_REFUND_FACT
 * @author yuzc
 * @date 2025/7/30
 */
public class MealRefundFactModel {
    /**
     * pk_id
     */
    @JsonProperty("PK_ID")
    private String pkId;

    /**
     * EMD退票日期
     */
    @JsonProperty("FK_REFUND_DATE")
    private String fkRefundDate;

    /**
     * EMD退票时间
     */
    @JsonProperty("FK_REFUND_TIME")
    private String fkRefundTime;

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
    @JsonProperty("FK_MEALSTART_DATE")
    private String fkMealstartDate;

    /**
     * 航班时间
     */
    @JsonProperty("FK_MEALSTART_TIME")
    private String fkMealstartTime;

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
    @JsonProperty("FK_MEAL_SEG")
    private String fkMealSeg;

    /**
     * 国内国际标识
     */
    @JsonProperty("AK_DIMARK")
    private String akDimark;

    /**
     * 关联机票票号
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
     * 舱等
     */
    @JsonProperty("AK_CABIN")
    private String akCabin;

    /**
     * 舱位
     */
    @JsonProperty("AK_SEGCABIN")
    private String akSegcabin;

    /**
     * 币种
     */
    @JsonProperty("AK_CURRENCY")
    private String akCurrency;

    /**
     * 选餐金额
     */
    @JsonProperty("MEAL_AMOUNT")
    private Double mealAmount;

    /**
     * 选餐详情
     */
    @JsonProperty("MEAL_DETTAIL")
    private String mealDettail;

    /**
     * 餐食类型
     */
    @JsonProperty("MEAL_TYPE")
    private String mealType;

    /**
     * 选餐退票次数计数
     */
    @JsonProperty("PAY_COUNT")
    private Integer payCount;

    /**
     * EMD出票office
     */
    @JsonProperty("AK_EMDTIKCHANNEL")
    private String akEmdtikchannel;

    /**
     * EMD出票Office对应的IataCode
     */
    @JsonProperty("AK_EMDIATACODE")
    private String akEmdiataCode;

    /**
     * EMD类型
     */
    @JsonProperty("AK_EMDTYPE")
    private String akEmdtype;

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
     * 本系统创建日期时间
     */
    @JsonProperty("SYSTEM_CREATETIME")
    private String systemCreatetime;

    /**
     * 本系统最后更新日期时间
     */
    @JsonProperty("SYSTEM_LAST_UPDATETIME")
    private String systemLastUpdatetime;

    // Getter and Setter methods

    public String getPkId() {
        return pkId;
    }

    public void setPkId(String pkId) {
        this.pkId = pkId;
    }


    public String getFkRefundDate() {
        return fkRefundDate;
    }

    public void setFkRefundDate(String fkRefundDate) {
        this.fkRefundDate = fkRefundDate;
    }

    public String getFkRefundTime() {
        return fkRefundTime;
    }

    public void setFkRefundTime(String fkRefundTime) {
        this.fkRefundTime = fkRefundTime;
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

    public String getFkMealstartDate() {
        return fkMealstartDate;
    }

    public void setFkMealstartDate(String fkMealstartDate) {
        this.fkMealstartDate = fkMealstartDate;
    }

    public String getFkMealstartTime() {
        return fkMealstartTime;
    }

    public void setFkMealstartTime(String fkMealstartTime) {
        this.fkMealstartTime = fkMealstartTime;
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

    public String getFkMealSeg() {
        return fkMealSeg;
    }

    public void setFkMealSeg(String fkMealSeg) {
        this.fkMealSeg = fkMealSeg;
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

    public String getAkCabin() {
        return akCabin;
    }

    public void setAkCabin(String akCabin) {
        this.akCabin = akCabin;
    }

    public String getAkSegcabin() {
        return akSegcabin;
    }

    public void setAkSegcabin(String akSegcabin) {
        this.akSegcabin = akSegcabin;
    }

    public String getAkCurrency() {
        return akCurrency;
    }

    public void setAkCurrency(String akCurrency) {
        this.akCurrency = akCurrency;
    }

    public Double getMealAmount() {
        return mealAmount;
    }

    public void setMealAmount(Double mealAmount) {
        this.mealAmount = mealAmount;
    }

    public String getMealDettail() {
        return mealDettail;
    }

    public void setMealDettail(String mealDettail) {
        this.mealDettail = mealDettail;
    }

    public String getMealType() {
        return mealType;
    }

    public void setMealType(String mealType) {
        this.mealType = mealType;
    }

    public Integer getPayCount() {
        return payCount;
    }

    public void setPayCount(Integer payCount) {
        this.payCount = payCount;
    }

    public String getAkEmdtikchannel() {
        return akEmdtikchannel;
    }

    public void setAkEmdtikchannel(String akEmdtikchannel) {
        this.akEmdtikchannel = akEmdtikchannel;
    }

    public String getAkEmdiataCode() {
        return akEmdiataCode;
    }

    public void setAkEmdiataCode(String akEmdiataCode) {
        this.akEmdiataCode = akEmdiataCode;
    }

    public String getAkEmdtype() {
        return akEmdtype;
    }

    public void setAkEmdtype(String akEmdtype) {
        this.akEmdtype = akEmdtype;
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

    public String getSystemCreatetime() {
        return systemCreatetime;
    }

    public void setSystemCreatetime(String systemCreatetime) {
        this.systemCreatetime = systemCreatetime;
    }

    public String getSystemLastUpdatetime() {
        return systemLastUpdatetime;
    }

    public void setSystemLastUpdatetime(String systemLastUpdatetime) {
        this.systemLastUpdatetime = systemLastUpdatetime;
    }

    @Override
    public String toString() {
        return "MealRefundFactModel{" +
                "pkId='" + pkId + '\'' +
                ", fkRefundDate='" + fkRefundDate + '\'' +
                ", fkRefundTime='" + fkRefundTime + '\'' +
                ", depairport='" + depairport + '\'' +
                ", arriairport='" + arriairport + '\'' +
                ", fkMealstartDate='" + fkMealstartDate + '\'' +
                ", fkMealstartTime='" + fkMealstartTime + '\'' +
                ", enLastName='" + enLastName + '\'' +
                ", enFirstName='" + enFirstName + '\'' +
                ", cnName='" + cnName + '\'' +
                ", passengerType='" + passengerType + '\'' +
                ", certType='" + certType + '\'' +
                ", certNumber='" + certNumber + '\'' +
                ", passengerAge=" + passengerAge +
                ", fkPassengerUserTid='" + fkPassengerUserTid + '\'' +
                ", fkMealSeg='" + fkMealSeg + '\'' +
                ", akDimark='" + akDimark + '\'' +
                ", akTiknum='" + akTiknum + '\'' +
                ", akEmdnum='" + akEmdnum + '\'' +
                ", akEmdStatus='" + akEmdStatus + '\'' +
                ", akCabin='" + akCabin + '\'' +
                ", akSegcabin='" + akSegcabin + '\'' +
                ", akCurrency='" + akCurrency + '\'' +
                ", mealAmount=" + mealAmount +
                ", mealDettail='" + mealDettail + '\'' +
                ", mealType='" + mealType + '\'' +
                ", payCount=" + payCount +
                ", akEmdtikchannel='" + akEmdtikchannel + '\'' +
                ", akEmdiataCode='" + akEmdiataCode + '\'' +
                ", akEmdtype='" + akEmdtype + '\'' +
                ", dataActive=" + dataActive +
                ", dataActiveTime='" + dataActiveTime + '\'' +
                ", systemCreatetime='" + systemCreatetime + '\'' +
                ", systemLastUpdatetime='" + systemLastUpdatetime + '\'' +
                '}';
    }
}