package com.travelsky.trp.usercenter.data.analysis.model.dwd.fact;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;

/**
 * 机票退票航段级事实表实体类-T_DWD_REFUND_SEG_FACT
 * @author TS.SHA.fuhuazhang
 * @date 2025/7/20  14:34
 */
public class RefundSegFactModel {

    /**
     * 主键ID
     */
    @JsonProperty("PK_ID")
    private String pkId;

    /**
     * 是否为大客户订单
     */
    @JsonProperty("IS_KEY_ACCOUNT")
    private Boolean isKeyAccount;

    /**
     * 出票日期
     */
    @JsonProperty("TICKETING_DATE")
    private String ticketingDate;

    /**
     * 出票时间
     */
    @JsonProperty("TICKETING_TIME")
    private String ticketingTime;

    /**
     * 退票日期
     */
    @JsonProperty("REFUND_DATE")
    private String refundDate;

    /**
     * 退票时间
     */
    @JsonProperty("REFUND_TIME")
    private String refundTime;

    /**
     * 退票票号
     */
    @JsonProperty("AK_TIK_NUMBER")
    private String akTikNumber;

    /**
     * 起飞机场
     */
    @JsonProperty("FK_DEPAIRPORT")
    private String fkDepairport;

    /**
     * 到达机场
     */
    @JsonProperty("FK_ARRIAIRPORT")
    private String fkArriairport;

    /**
     * 航程
     */
    @JsonProperty("FK_SEG_SEGMENT")
    private String fkSegSegment;

    /**
     * 航班起飞日期
     */
    @JsonProperty("FK_SEG_DATE")
    private String fkSegDate;

    /**
     * 航班起飞时间
     */
    @JsonProperty("FK_SEG_TIME")
    private String fkSegTime;

    /**
     * 乘机人类型（成人等）
     */
    @JsonProperty("AK_PASSTYPE")
    private String akPasstype;

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
     * 乘机人证件类型
     */
    @JsonProperty("CERT_TYPE")
    private String certType;

    /**
     * 乘机人证件号码
     */
    @JsonProperty("CERT_NUMBER")
    private String certNumber;

    /**
     * 修正后乘机人证件类型
     */
    //@JsonProperty("MODIFIED_CERT_TYPE")
    //private String modifiedCertType;

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
     * 国内国际标识
     */
    @JsonProperty("AK_DIMARK")
    private String akDimark;

    /**
     * 常客卡号
     */
    @JsonProperty("FFRF")
    private String ffrf;

    /**
     * 常客等级
     */
    @JsonProperty("FF_LEVEL")
    private String ffLevel;

    /**
     * 常客卡航司
     */
    @JsonProperty("FF_AIRLINE")
    private String ffAirline;

    /**
     * 常客联盟卡级别
     */
    @JsonProperty("FF_ALLIANCE_LEVEL")
    private String ffAllianceLevel;

    /**
     * 航段序号
     */
    @JsonProperty("SEG_NO")
    private Integer segNo;

    /**
     * 退票航段计数
     */
    @JsonProperty("SEG_REFUNDCOUNT")
    private Integer segRefundcount = 1;

    /**
     * 数据有效性
     */
    @JsonProperty("IS_VALID")
    private String isValid;

    /**
     * 退票渠道
     */
    @JsonProperty("AK_REFUND_CHANNEL")
    private String akRefundChannel;

    /**
     * 退票类型
     */
    @JsonProperty("AK_REFUND_TYPE")
    private String akRefundType;

    /**
     * 非自愿退票原因
     */
    @JsonProperty("REFUND_REASON")
    private String refundReason;

    /**
     * 退订人TID
     */
    @JsonProperty("FK_REFUND_USER_TID")
    private String fkRefundUserTid;

    /**
     * 退订人源ID
     */
    @JsonProperty("FK_REFUND_USER_ORIGIN_ID")
    private String fkRefundUserOriginId;

    /**
     * 退票审核完成日期
     */
    @JsonProperty("FK_COMPLETE_DATE")
    private String fkCompleteDate;

    /**
     * 退票审核完成时间
     */
    @JsonProperty("FK_COMPLETE_TIME")
    private String fkCompleteTime;

    /**
     * 币种
     */
    @JsonProperty("AK_CURRENCY")
    private String akCurrency;

    /**
     * 应退金额原币种
     */
    @JsonProperty("SEG_CURREFUNDAMOUNT")
    private Double segCurrefundamount;

    /**
     * 退票手续费原币种
     */
    @JsonProperty("SEG_CURREFUNDCHARGES")
    private Double segCurrefundcharges;

    /**
     * 应退金额CNY
     */
    @JsonProperty("SEG_REFUNDAMOUNT")
    private Double segRefundamount;

    /**
     * 退票手续费CNY
     */
    @JsonProperty("SEG_REFUNDCHARGES")
    private Double segRefundcharges;

    /**
     * 提前出票天数
     */
    @JsonProperty("AK_ADVBOOK_DAY")
    private Integer akAdvbookDay;

    /**
     * 是否与儿童同行
     */
    @JsonProperty("AK_PEER_CHD")
    private Boolean akPeerChd;

    /**
     * 是否与老人同行
     */
    @JsonProperty("PEER_SENIOR")
    private Boolean peerSenior;

    /**
     * 是否与婴儿同行
     */
    //@JsonProperty("PEER_INFANT")
    //private Boolean peerInfant;

    /**
     * 是否为自己订票
     */
    @JsonProperty("SELF_BOOKING")
    private Boolean selfBooking;

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
     * 源系统最后更新时间（时间戳）
     */
    @JsonProperty("SOURCE_LAST_UPDATETIME")
    private String sourceLastUpdatetime;

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

    public String getTicketingDate() {
        return ticketingDate;
    }

    public void setTicketingDate(String ticketingDate) {
        this.ticketingDate = ticketingDate;
    }

    public String getTicketingTime() {
        return ticketingTime;
    }

    public void setTicketingTime(String ticketingTime) {
        this.ticketingTime = ticketingTime;
    }

    public String getRefundDate() {
        return refundDate;
    }

    public void setRefundDate(String refundDate) {
        this.refundDate = refundDate;
    }

    public String getRefundTime() {
        return refundTime;
    }

    public void setRefundTime(String refundTime) {
        this.refundTime = refundTime;
    }

    public String getAkTikNumber() {
        return akTikNumber;
    }

    public void setAkTikNumber(String akTikNumber) {
        this.akTikNumber = akTikNumber;
    }

    public String getFkDepairport() {
        return fkDepairport;
    }

    public void setFkDepairport(String fkDepairport) {
        this.fkDepairport = fkDepairport;
    }

    public String getFkArriairport() {
        return fkArriairport;
    }

    public void setFkArriairport(String fkArriairport) {
        this.fkArriairport = fkArriairport;
    }

    public String getFkSegSegment() {
        return fkSegSegment;
    }

    public void setFkSegSegment(String fkSegSegment) {
        this.fkSegSegment = fkSegSegment;
    }

    public String getFkSegDate() {
        return fkSegDate;
    }

    public void setFkSegDate(String fkSegDate) {
        this.fkSegDate = fkSegDate;
    }

    public String getFkSegTime() {
        return fkSegTime;
    }

    public void setFkSegTime(String fkSegTime) {
        this.fkSegTime = fkSegTime;
    }

    public String getAkPasstype() {
        return akPasstype;
    }

    public void setAkPasstype(String akPasstype) {
        this.akPasstype = akPasstype;
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

    public String getAkDimark() {
        return akDimark;
    }

    public void setAkDimark(String akDimark) {
        this.akDimark = akDimark;
    }

    public String getFfrf() {
        return ffrf;
    }

    public void setFfrf(String ffrf) {
        this.ffrf = ffrf;
    }

    public String getFfLevel() {
        return ffLevel;
    }

    public void setFfLevel(String ffLevel) {
        this.ffLevel = ffLevel;
    }

    public String getFfAirline() {
        return ffAirline;
    }

    public void setFfAirline(String ffAirline) {
        this.ffAirline = ffAirline;
    }

    public String getFfAllianceLevel() {
        return ffAllianceLevel;
    }

    public void setFfAllianceLevel(String ffAllianceLevel) {
        this.ffAllianceLevel = ffAllianceLevel;
    }

    public Integer getSegRefundcount() {
        return segRefundcount;
    }

    public void setSegRefundcount(Integer segRefundcount) {
        this.segRefundcount = segRefundcount;
    }

    public String getIsValid() {
        return isValid;
    }

    public void setIsValid(String isValid) {
        this.isValid = isValid;
    }

    public String getAkRefundChannel() {
        return akRefundChannel;
    }

    public void setAkRefundChannel(String akRefundChannel) {
        this.akRefundChannel = akRefundChannel;
    }

    public String getAkRefundType() {
        return akRefundType;
    }

    public void setAkRefundType(String akRefundType) {
        this.akRefundType = akRefundType;
    }

    public String getRefundReason() {
        return refundReason;
    }

    public void setRefundReason(String refundReason) {
        this.refundReason = refundReason;
    }

    public String getFkRefundUserTid() {
        return fkRefundUserTid;
    }

    public void setFkRefundUserTid(String fkRefundUserTid) {
        this.fkRefundUserTid = fkRefundUserTid;
    }

    public String getFkRefundUserOriginId() {
        return fkRefundUserOriginId;
    }

    public void setFkRefundUserOriginId(String fkRefundUserOriginId) {
        this.fkRefundUserOriginId = fkRefundUserOriginId;
    }

    public String getFkCompleteDate() {
        return fkCompleteDate;
    }

    public void setFkCompleteDate(String fkCompleteDate) {
        this.fkCompleteDate = fkCompleteDate;
    }

    public String getFkCompleteTime() {
        return fkCompleteTime;
    }

    public void setFkCompleteTime(String fkCompleteTime) {
        this.fkCompleteTime = fkCompleteTime;
    }

    public String getAkCurrency() {
        return akCurrency;
    }

    public void setAkCurrency(String akCurrency) {
        this.akCurrency = akCurrency;
    }

    public Double getSegCurrefundamount() {
        return segCurrefundamount;
    }

    public void setSegCurrefundamount(Double segCurrefundamount) {
        this.segCurrefundamount = segCurrefundamount;
    }

    public Double getSegCurrefundcharges() {
        return segCurrefundcharges;
    }

    public void setSegCurrefundcharges(Double segCurrefundcharges) {
        this.segCurrefundcharges = segCurrefundcharges;
    }

    public Double getSegRefundamount() {
        return segRefundamount;
    }

    public void setSegRefundamount(Double segRefundamount) {
        this.segRefundamount = segRefundamount;
    }

    public Double getSegRefundcharges() {
        return segRefundcharges;
    }

    public void setSegRefundcharges(Double segRefundcharges) {
        this.segRefundcharges = segRefundcharges;
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

    public String getSourceLastUpdatetime() {
        return sourceLastUpdatetime;
    }

    public void setSourceLastUpdatetime(String sourceLastUpdatetime) {
        this.sourceLastUpdatetime = sourceLastUpdatetime;
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

    public Boolean getKeyAccount() {
        return isKeyAccount;
    }

    public void setKeyAccount(Boolean keyAccount) {
        isKeyAccount = keyAccount;
    }

    //public String getModifiedCertType() {
    //    return modifiedCertType;
    //}
    //
    //public void setModifiedCertType(String modifiedCertType) {
    //    this.modifiedCertType = modifiedCertType;
    //}

    public Integer getSegNo() {
        return segNo;
    }

    public void setSegNo(Integer segNo) {
        this.segNo = segNo;
    }

    public Integer getAkAdvbookDay() {
        return akAdvbookDay;
    }

    public void setAkAdvbookDay(Integer akAdvbookDay) {
        this.akAdvbookDay = akAdvbookDay;
    }

    public Boolean getAkPeerChd() {
        return akPeerChd;
    }

    public void setAkPeerChd(Boolean akPeerChd) {
        this.akPeerChd = akPeerChd;
    }

    public Boolean getPeerSenior() {
        return peerSenior;
    }

    public void setPeerSenior(Boolean peerSenior) {
        this.peerSenior = peerSenior;
    }

    //public Boolean getPeerInfant() {
    //    return peerInfant;
    //}
    //
    //public void setPeerInfant(Boolean peerInfant) {
    //    this.peerInfant = peerInfant;
    //}

    public Boolean getSelfBooking() {
        return selfBooking;
    }

    public void setSelfBooking(Boolean selfBooking) {
        this.selfBooking = selfBooking;
    }

    @Override
    public String toString() {
        return "{"
                + "RefundSegFactModel: {"
                + "pkId: " + pkId
                + ", " + "isKeyAccount: " + isKeyAccount
                + ", " + "ticketingDate: " + ticketingDate
                + ", " + "ticketingTime: " + ticketingTime
                + ", " + "refundDate: " + refundDate
                + ", " + "refundTime: " + refundTime
                + ", " + "akTikNumber: " + akTikNumber
                + ", " + "fkDepairport: " + fkDepairport
                + ", " + "fkArriairport: " + fkArriairport
                + ", " + "fkSegSegment: " + fkSegSegment
                + ", " + "fkSegDate: " + fkSegDate
                + ", " + "fkSegTime: " + fkSegTime
                + ", " + "akPasstype: " + akPasstype
                + ", " + "enLastName: " + enLastName
                + ", " + "enFirstName: " + enFirstName
                + ", " + "cnName: " + cnName
                + ", " + "certType: " + certType
                + ", " + "certNumber: " + certNumber
                //+ ", " + "modifiedCertType: " + modifiedCertType
                + ", " + "passengerAge: " + passengerAge
                + ", " + "fkPassengerUserTid: " + fkPassengerUserTid
                + ", " + "akCabin: " + akCabin
                + ", " + "akSegcabin: " + akSegcabin
                + ", " + "akDimark: " + akDimark
                + ", " + "ffrf: " + ffrf
                + ", " + "ffLevel: " + ffLevel
                + ", " + "ffAirline: " + ffAirline
                + ", " + "ffAllianceLevel: " + ffAllianceLevel
                + ", " + "segNo: " + segNo
                + ", " + "segRefundcount: " + segRefundcount
                + ", " + "isValid: " + isValid
                + ", " + "akRefundChannel: " + akRefundChannel
                + ", " + "akRefundType: " + akRefundType
                + ", " + "refundReason: " + refundReason
                + ", " + "fkRefundUserTid: " + fkRefundUserTid
                + ", " + "fkRefundUserOriginId: " + fkRefundUserOriginId
                + ", " + "fkCompleteDate: " + fkCompleteDate
                + ", " + "fkCompleteTime: " + fkCompleteTime
                + ", " + "akCurrency: " + akCurrency
                + ", " + "segCurrefundamount: " + segCurrefundamount
                + ", " + "segCurrefundcharges: " + segCurrefundcharges
                + ", " + "segRefundamount: " + segRefundamount
                + ", " + "segRefundcharges: " + segRefundcharges
                + ", " + "akAdvbookDay: " + akAdvbookDay
                + ", " + "akPeerChd: " + akPeerChd
                + ", " + "peerSenior: " + peerSenior
                //+ ", " + "peerInfant: " + peerInfant
                + ", " + "selfBooking: " + selfBooking
                + ", " + "dataActive: " + dataActive
                + ", " + "dataActiveTime: " + dataActiveTime
                + ", " + "sourceLastUpdatetime: " + sourceLastUpdatetime
                + ", " + "systemCreatetime: " + systemCreatetime
                + ", " + "systemLastUpdatetime: " + systemLastUpdatetime
                + "}"
                + "}";
    }
}
