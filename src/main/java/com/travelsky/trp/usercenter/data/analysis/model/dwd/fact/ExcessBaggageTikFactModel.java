package com.travelsky.trp.usercenter.data.analysis.model.dwd.fact;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 附加服务逾重行李出票航段事实表实体类-T_DWD_EXCESS_BAGGAGE_TIK_FACT
 * @author yuzc
 * @date 2025/7/30
 */
public class ExcessBaggageTikFactModel {
    /**
     * pk_id
     */
    @JsonProperty("PK_ID")
    private String pkId;

    /**
     * EMD出票日期
     */
    @JsonProperty("FK_BAGGAGETIK_DATE")
    private String fkBaggagetikDate;

    /**
     * EMD出票时间
     */
    @JsonProperty("FK_BAGGAGETIK_TIME")
    private String fkBaggagetikTime;

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
    @JsonProperty("FK_BAGGAGESTART_DATE")
    private String fkBaggagestartDate;

    /**
     * 航班时间
     */
    @JsonProperty("FK_BAGGAGESTART_TIME")
    private String fkBaggagestartTime;

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
     * 乘机人TID
     */
    @JsonProperty("FK_PASSENGER_USER_TID")
    private String fkPassengerUserTid;

    /**
     * 航程
     */
    @JsonProperty("FK_BAGGAGE_SEG")
    private String fkBaggageSeg;

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
     * 行李数量及单位
     */
    @JsonProperty("AK_BAGGAGE_ATT")
    private String akBaggageAtt;

    /**
     * 行李详情
     */
    @JsonProperty("AK_EXCESSBAGGAGE_DESC")
    private String akExcessBaggageDesc;

    /**
     * 逾重行李属性
     */
    @JsonProperty("AK_EXCESSBAGGAGE_ATTR")
    private String akExcessBaggageAttr;

    /**
     * 逾重行李件数
     */
    @JsonProperty("AK_EXCESSBAGGAGE_NUMBER")
    private Integer akExcessBaggageNumber;


    /**
     * 逾重行李重量
     */
    @JsonProperty("AK_EXCESSBAGGAGE_WEIGHT")
    private Integer akExcessBaggageWeight;

    /**
     * 原币种
     */
    @JsonProperty("AK_EXCESSBAGGAGE_OLDCURRENCYCODE")
    private String akExcessBaggageOldCurrencyCode;

    /**
     * 原币种逾重行李金额
     */
    @JsonProperty("AK_EXCESSBAGGAGE_OLDAMOUNT")
    private Double akExcessBaggageOldAmount;

    /**
     * 逾重行李金额CNY
     */
    @JsonProperty("AK_EXCESSBAGGAGE_OLDAMOUNT_CNY")
    private Double akExcessBaggageOldAmountCny;

    /**
     * 逾重币种
     */
    @JsonProperty("AK_CURRENCY")
    private String akCurrency;

    /**
     * 逾重行李金额
     */
    @JsonProperty("AK_EXCESSBAGGAGE_AMOUNT")
    private BigDecimal akExcessbaggageAmount;

    /**
     * 逾重行李单价
     */
    @JsonProperty("AK_EXCESSBAGGAGE_SINGLEAMOUNT")
    private BigDecimal akExcessbaggageSingleamount;

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
     * 逾重行李出票计数
     */
    @JsonProperty("AK_EXCESSBAGGAGE_COUNT")
    private Integer akExcessBaggageCount;

    /**
     * 逾重行李计量单位数
     */
    @JsonProperty("EXCESSBAGGAGE_QUANTITY")
    private Integer excessbaggageQuantity;

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

    public String getPkId() {
        return pkId;
    }

    public void setPkId(String pkId) {
        this.pkId = pkId;
    }

    public String getFkBaggagetikDate() {
        return fkBaggagetikDate;
    }

    public void setFkBaggagetikDate(String fkBaggagetikDate) {
        this.fkBaggagetikDate = fkBaggagetikDate;
    }

    public String getFkBaggagetikTime() {
        return fkBaggagetikTime;
    }

    public void setFkBaggagetikTime(String fkBaggagetikTime) {
        this.fkBaggagetikTime = fkBaggagetikTime;
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

    public String getFkBaggagestartDate() {
        return fkBaggagestartDate;
    }

    public void setFkBaggagestartDate(String fkBaggagestartDate) {
        this.fkBaggagestartDate = fkBaggagestartDate;
    }

    public String getFkBaggagestartTime() {
        return fkBaggagestartTime;
    }

    public void setFkBaggagestartTime(String fkBaggagestartTime) {
        this.fkBaggagestartTime = fkBaggagestartTime;
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

    public String getFkBaggageSeg() {
        return fkBaggageSeg;
    }

    public void setFkBaggageSeg(String fkBaggageSeg) {
        this.fkBaggageSeg = fkBaggageSeg;
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

    public String getAkBaggageAtt() {
        return akBaggageAtt;
    }

    public void setAkBaggageAtt(String akBaggageAtt) {
        this.akBaggageAtt = akBaggageAtt;
    }

    public String getAkExcessBaggageDesc() {
        return akExcessBaggageDesc;
    }

    public void setAkExcessBaggageDesc(String akExcessBaggageDesc) {
        this.akExcessBaggageDesc = akExcessBaggageDesc;
    }

    public String getAkExcessBaggageAttr() {
        return akExcessBaggageAttr;
    }

    public void setAkExcessBaggageAttr(String akExcessBaggageAttr) {
        this.akExcessBaggageAttr = akExcessBaggageAttr;
    }

    public Integer getAkExcessBaggageNumber() {
        return akExcessBaggageNumber;
    }

    public void setAkExcessBaggageNumber(Integer akExcessBaggageNumber) {
        this.akExcessBaggageNumber = akExcessBaggageNumber;
    }

    public Integer getAkExcessBaggageWeight() {
        return akExcessBaggageWeight;
    }

    public void setAkExcessBaggageWeight(Integer akExcessBaggageWeight) {
        this.akExcessBaggageWeight = akExcessBaggageWeight;
    }

    public String getAkExcessBaggageOldCurrencyCode() {
        return akExcessBaggageOldCurrencyCode;
    }

    public void setAkExcessBaggageOldCurrencyCode(String akExcessBaggageOldCurrencyCode) {
        this.akExcessBaggageOldCurrencyCode = akExcessBaggageOldCurrencyCode;
    }

    public Double getAkExcessBaggageOldAmount() {
        return akExcessBaggageOldAmount;
    }

    public void setAkExcessBaggageOldAmount(Double akExcessBaggageOldAmount) {
        this.akExcessBaggageOldAmount = akExcessBaggageOldAmount;
    }

    public String getAkCurrency() {
        return akCurrency;
    }

    public void setAkCurrency(String akCurrency) {
        this.akCurrency = akCurrency;
    }

    public BigDecimal getAkExcessbaggageAmount() {
        return akExcessbaggageAmount;
    }

    public void setAkExcessbaggageAmount(BigDecimal akExcessbaggageAmount) {
        this.akExcessbaggageAmount = akExcessbaggageAmount;
    }

    public BigDecimal getAkExcessbaggageSingleamount() {
        return akExcessbaggageSingleamount;
    }

    public void setAkExcessbaggageSingleamount(BigDecimal akExcessbaggageSingleamount) {
        this.akExcessbaggageSingleamount = akExcessbaggageSingleamount;
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

    public Integer getExcessbaggageQuantity() {
        return excessbaggageQuantity;
    }

    public void setExcessbaggageQuantity(Integer excessbaggageQuantity) {
        this.excessbaggageQuantity = excessbaggageQuantity;
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

    public Double getAkExcessBaggageOldAmountCny() {
        return akExcessBaggageOldAmountCny;
    }

    public void setAkExcessBaggageOldAmountCny(Double akExcessBaggageOldAmountCny) {
        this.akExcessBaggageOldAmountCny = akExcessBaggageOldAmountCny;
    }

    public Integer getAkExcessBaggageCount() {
        return akExcessBaggageCount;
    }

    public void setAkExcessBaggageCount(Integer akExcessBaggageCount) {
        this.akExcessBaggageCount = akExcessBaggageCount;
    }

    @Override
    public String toString() {
        return "ExcessBaggageTikFactModel{" +
                "pkId='" + pkId + '\'' +
                ", fkBaggagetikDate='" + fkBaggagetikDate + '\'' +
                ", fkBaggagetikTime='" + fkBaggagetikTime + '\'' +
                ", depairport='" + depairport + '\'' +
                ", arriairport='" + arriairport + '\'' +
                ", fkBaggagestartDate='" + fkBaggagestartDate + '\'' +
                ", fkBaggagestartTime='" + fkBaggagestartTime + '\'' +
                ", enLastName='" + enLastName + '\'' +
                ", enFirstName='" + enFirstName + '\'' +
                ", cnName='" + cnName + '\'' +
                ", passengerType='" + passengerType + '\'' +
                ", certType='" + certType + '\'' +
                ", certNumber='" + certNumber + '\'' +
                ", passengerAge=" + passengerAge +
                ", fkPassengerUserTid='" + fkPassengerUserTid + '\'' +
                ", fkBaggageSeg='" + fkBaggageSeg + '\'' +
                ", akDimark='" + akDimark + '\'' +
                ", akTiknum='" + akTiknum + '\'' +
                ", akEmdnum='" + akEmdnum + '\'' +
                ", akEmdStatus='" + akEmdStatus + '\'' +
                ", akBaggageAtt='" + akBaggageAtt + '\'' +
                ", akExcessBaggageDesc='" + akExcessBaggageDesc + '\'' +
                ", akExcessBaggageAttr='" + akExcessBaggageAttr + '\'' +
                ", akExcessBaggageNumber=" + akExcessBaggageNumber +
                ", akExcessBaggageWeight=" + akExcessBaggageWeight +
                ", akExcessBaggageOldCurrencyCode='" + akExcessBaggageOldCurrencyCode + '\'' +
                ", akExcessBaggageOldAmount=" + akExcessBaggageOldAmount +
                ", akExcessBaggageOldAmountCny=" + akExcessBaggageOldAmountCny +
                ", akCurrency='" + akCurrency + '\'' +
                ", akExcessbaggageAmount=" + akExcessbaggageAmount +
                ", akExcessbaggageSingleamount=" + akExcessbaggageSingleamount +
                ", akEmdtikchannel='" + akEmdtikchannel + '\'' +
                ", akEmdiataCode='" + akEmdiataCode + '\'' +
                ", akEmdtype='" + akEmdtype + '\'' +
                ", akExcessBaggageCount=" + akExcessBaggageCount +
                ", excessbaggageQuantity=" + excessbaggageQuantity +
                ", dataActive=" + dataActive +
                ", dataActiveTime='" + dataActiveTime + '\'' +
                ", systemCreatetime='" + systemCreatetime + '\'' +
                ", systemLastUpdatetime='" + systemLastUpdatetime + '\'' +
                '}';
    }
}