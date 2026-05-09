package com.travelsky.trp.usercenter.data.analysis.model.dwd.fact;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 机票改升换开出票航段级事实表实体类-T_DWD_DATECHANGE_SEG_FACT
 * @author TS.SHA.fuhuazhang
 * @date 2025/7/20  14:31
 */
public class DatechangeSegFactModel {
    /**
     * 主键ID
     */
    @JsonProperty("PK_ID")
    private String pkId;

    /**
     * PNR编号
     */
    @JsonProperty("AK_PNR_NUMBER")
    private String akPnrNumber;

    /**
     * 换开后票号
     */
    @JsonProperty("NEW_TIK_NUM")
    private String newTikNum;

    /**
     * 原票号
     */
    @JsonProperty("ORI_TIK_NUM")
    private String oriTikNum;

    /**
     * 出票office号
     */
    @JsonProperty("AK_BOOKING_OFFICE_NUMBER")
    private String akBookingOfficeNumber;

    /**
     * 换开出票日期
     */
    @JsonProperty("FK_EXCHANGE_DATE")
    private String fkExchangeDate;

    /**
     * 换开出票时间
     */
    @JsonProperty("FK_EXCHANGE_TIME")
    private String fkExchangeTime;

    /**
     * 改升后航班起飞日期
     */
    @JsonProperty("FK_DEPARTURES_DATE")
    private String fkDeparturesDate;

    /**
     * 改升后航班起飞时间
     */
    @JsonProperty("FK_DEPARTURES_TIME")
    private String fkDeparturesTime;

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
     * 改升后航程
     */
    @JsonProperty("FK_DC_SEG")
    private String fkDcSeg;

    /**
     * 乘机人类型（成人等）
     */
    @JsonProperty("PASSENGER_TYPE")
    private String passengerType;

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
     * 行程类型
     */
    //@JsonProperty("AK_BOOKING_DIRECTIONIND")
    //private String akBookingDirectionind;

    /**
     * 换开后舱位
     */
    @JsonProperty("AK_SEGCABIN")
    private String akSegcabin;

    /**
     * 换开后舱等
     */
    @JsonProperty("AK_CABIN")
    private String akCabin;

    /**
     * 换开前舱位
     */
    @JsonProperty("OLD_AK_SEGCABIN")
    private String oldAkSegcabin;

    /**
     * 换开前舱等
     */
    @JsonProperty("OLD_AK_CABIN")
    private String oldAkCabin;

    /**
     * 换开前起飞日期
     */
    @JsonProperty("OLD_DEPARTURES_DATE")
    private String oldDeparturesDate;

    /**
     * 换开前起飞时间
     */
    @JsonProperty("OLD_DEPARTURES_TIME")
    private String oldDeparturesTime;

    /**
     * 换开类型
     */
    @JsonProperty("DATE_CHANGE_TYPE")
    private String dateChangeType;

    /**
     * 国内国际标识
     */
    @JsonProperty("AK_DIMARK")
    private String akDimark;

    /**
     * 航段状态
     */
    @JsonProperty("SEGMENT_STATUS")
    private String segmentStatus;

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
     * 销售币种
     */
    @JsonProperty("AK_CURRENCY")
    private String akCurrency;

    /**
     * 销售币种改升支付金额
     */
    @JsonProperty("DC_CURPAYAMOUNT")
    private BigDecimal dcCurpayamount;

    /**
     * 销售币种变更手续费
     */
    @JsonProperty("DC_CURFEE")
    private BigDecimal dcCurfee;

    /**
     * 销售币种舱位差价
     */
    @JsonProperty("CABINBALANCE_CUR")
    private BigDecimal cabinbalanceCur;

    /**
     * 改升支付金额CNY
     */
    @JsonProperty("DC_PAYAMOUNT")
    private BigDecimal dcPayamount;

    /**
     * 改升手续费CNY
     */
    @JsonProperty("DATECHANGE_FEE")
    private BigDecimal datechangeFee;

    /**
     * 舱位差价CNY
     */
    @JsonProperty("CABINBALANCE")
    private BigDecimal cabinbalance;

    /**
     * 改升航段计数
     */
    @JsonProperty("DATECHANGE_COUNT")
    private Integer datechangeCount = 1;

    /**
     * 大客户号
     */
    @JsonProperty("AK_KEY_ACCOUNT_CODE")
    private String akKeyAccountCode;

    /**
     * 是否为大客户订单
     */
    @JsonProperty("IS_KEY_ACCOUNT")
    private Boolean isKeyAccount;

    /**
     * 渠道改期订单号
     */
    @JsonProperty("CHANGE_ORDERNO")
    private String changeOrderno;

    /**
     * 改升预订渠道
     */
    @JsonProperty("AK_CHANNEL")
    private String akChannel;

    /**
     * 联系人手机号
     */
    @JsonProperty("CONTACT_MOBILE_NUMBER")
    private String contactMobileNumber;

    /**
     * 联系人姓名
     */
    @JsonProperty("CONTACT_NAME")
    private String contactName;

    /**
     * 联系人固定电话
     */
    @JsonProperty("CONTACT_LANDLINE_MOBILE_NUMBER")
    private String contactLandlineMobileNumber;


    /**
     * 预订人TID
     */
    @JsonProperty("FK_BOOKING_USER_TID")
    private String fkBookingUserTid;

    /**
     * 预订人源ID
     */
    @JsonProperty("FK_BOOKING_USER_ORIGIN_ID")
    private String fkBookingUserOriginId;

    /**
     * 渠道记录的改期类型
     */
    @JsonProperty("CHANGE_TYPE")
    private String changeType;

    /**
     * 数据是否启用
     */
    @JsonProperty("DATA_ACTIVE")
    private Boolean dataActive ;

    /**
     * 数据启用时间
     */
    @JsonProperty("DATA_ACTIVE_TIME")
    private String dataActiveTime ;

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

    public String getAkPnrNumber() {
        return akPnrNumber;
    }

    public void setAkPnrNumber(String akPnrNumber) {
        this.akPnrNumber = akPnrNumber;
    }

    public String getNewTikNum() {
        return newTikNum;
    }

    public void setNewTikNum(String newTikNum) {
        this.newTikNum = newTikNum;
    }

    public String getOriTikNum() {
        return oriTikNum;
    }

    public void setOriTikNum(String oriTikNum) {
        this.oriTikNum = oriTikNum;
    }

    public String getAkBookingOfficeNumber() {
        return akBookingOfficeNumber;
    }

    public void setAkBookingOfficeNumber(String akBookingOfficeNumber) {
        this.akBookingOfficeNumber = akBookingOfficeNumber;
    }

    public String getFkExchangeDate() {
        return fkExchangeDate;
    }

    public void setFkExchangeDate(String fkExchangeDate) {
        this.fkExchangeDate = fkExchangeDate;
    }

    public String getFkExchangeTime() {
        return fkExchangeTime;
    }

    public void setFkExchangeTime(String fkExchangeTime) {
        this.fkExchangeTime = fkExchangeTime;
    }

    public String getFkDeparturesDate() {
        return fkDeparturesDate;
    }

    public void setFkDeparturesDate(String fkDeparturesDate) {
        this.fkDeparturesDate = fkDeparturesDate;
    }

    public String getFkDeparturesTime() {
        return fkDeparturesTime;
    }

    public void setFkDeparturesTime(String fkDeparturesTime) {
        this.fkDeparturesTime = fkDeparturesTime;
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

    public String getFkDcSeg() {
        return fkDcSeg;
    }

    public void setFkDcSeg(String fkDcSeg) {
        this.fkDcSeg = fkDcSeg;
    }

    public String getPassengerType() {
        return passengerType;
    }

    public void setPassengerType(String passengerType) {
        this.passengerType = passengerType;
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

    //public String getModifiedCertType() {
    //    return modifiedCertType;
    //}
    //
    //public void setModifiedCertType(String modifiedCertType) {
    //    this.modifiedCertType = modifiedCertType;
    //}

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

    //public String getAkBookingDirectionind() {
    //    return akBookingDirectionind;
    //}
    //
    //public void setAkBookingDirectionind(String akBookingDirectionind) {
    //    this.akBookingDirectionind = akBookingDirectionind;
    //}

    public String getAkSegcabin() {
        return akSegcabin;
    }

    public void setAkSegcabin(String akSegcabin) {
        this.akSegcabin = akSegcabin;
    }

    public String getAkCabin() {
        return akCabin;
    }

    public void setAkCabin(String akCabin) {
        this.akCabin = akCabin;
    }

    public String getOldAkSegcabin() {
        return oldAkSegcabin;
    }

    public void setOldAkSegcabin(String oldAkSegcabin) {
        this.oldAkSegcabin = oldAkSegcabin;
    }

    public String getOldAkCabin() {
        return oldAkCabin;
    }

    public void setOldAkCabin(String oldAkCabin) {
        this.oldAkCabin = oldAkCabin;
    }

    public String getOldDeparturesDate() {
        return oldDeparturesDate;
    }

    public void setOldDeparturesDate(String oldDeparturesDate) {
        this.oldDeparturesDate = oldDeparturesDate;
    }

    public String getOldDeparturesTime() {
        return oldDeparturesTime;
    }

    public void setOldDeparturesTime(String oldDeparturesTime) {
        this.oldDeparturesTime = oldDeparturesTime;
    }

    public String getDateChangeType() {
        return dateChangeType;
    }

    public void setDateChangeType(String dateChangeType) {
        this.dateChangeType = dateChangeType;
    }

    public String getAkDimark() {
        return akDimark;
    }

    public void setAkDimark(String akDimark) {
        this.akDimark = akDimark;
    }

    public String getSegmentStatus() {
        return segmentStatus;
    }

    public void setSegmentStatus(String segmentStatus) {
        this.segmentStatus = segmentStatus;
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

    public String getAkCurrency() {
        return akCurrency;
    }

    public void setAkCurrency(String akCurrency) {
        this.akCurrency = akCurrency;
    }

    public BigDecimal getDcCurpayamount() {
        return dcCurpayamount;
    }

    public void setDcCurpayamount(BigDecimal dcCurpayamount) {
        this.dcCurpayamount = dcCurpayamount;
    }

    public BigDecimal getDcCurfee() {
        return dcCurfee;
    }

    public void setDcCurfee(BigDecimal dcCurfee) {
        this.dcCurfee = dcCurfee;
    }

    public BigDecimal getCabinbalanceCur() {
        return cabinbalanceCur;
    }

    public void setCabinbalanceCur(BigDecimal cabinbalanceCur) {
        this.cabinbalanceCur = cabinbalanceCur;
    }

    public BigDecimal getDcPayamount() {
        return dcPayamount;
    }

    public void setDcPayamount(BigDecimal dcPayamount) {
        this.dcPayamount = dcPayamount;
    }

    public BigDecimal getDatechangeFee() {
        return datechangeFee;
    }

    public void setDatechangeFee(BigDecimal datechangeFee) {
        this.datechangeFee = datechangeFee;
    }

    public BigDecimal getCabinbalance() {
        return cabinbalance;
    }

    public void setCabinbalance(BigDecimal cabinbalance) {
        this.cabinbalance = cabinbalance;
    }

    public Integer getDatechangeCount() {
        return datechangeCount;
    }

    public void setDatechangeCount(Integer datechangeCount) {
        this.datechangeCount = datechangeCount;
    }

    public String getAkKeyAccountCode() {
        return akKeyAccountCode;
    }

    public void setAkKeyAccountCode(String akKeyAccountCode) {
        this.akKeyAccountCode = akKeyAccountCode;
    }

    public Boolean getKeyAccount() {
        return isKeyAccount;
    }

    public void setKeyAccount(Boolean keyAccount) {
        isKeyAccount = keyAccount;
    }

    public String getChangeOrderno() {
        return changeOrderno;
    }

    public void setChangeOrderno(String changeOrderno) {
        this.changeOrderno = changeOrderno;
    }

    public String getAkChannel() {
        return akChannel;
    }

    public void setAkChannel(String akChannel) {
        this.akChannel = akChannel;
    }

    public String getContactMobileNumber() {
        return contactMobileNumber;
    }

    public void setContactMobileNumber(String contactMobileNumber) {
        this.contactMobileNumber = contactMobileNumber;
    }

    public String getContactName() {
        return contactName;
    }

    public void setContactName(String contactName) {
        this.contactName = contactName;
    }

    public String getContactLandlineMobileNumber() {
        return contactLandlineMobileNumber;
    }

    public void setContactLandlineMobileNumber(String contactLandlineMobileNumber) {
        this.contactLandlineMobileNumber = contactLandlineMobileNumber;
    }

    public String getFkBookingUserTid() {
        return fkBookingUserTid;
    }

    public void setFkBookingUserTid(String fkBookingUserTid) {
        this.fkBookingUserTid = fkBookingUserTid;
    }

    public String getFkBookingUserOriginId() {
        return fkBookingUserOriginId;
    }

    public void setFkBookingUserOriginId(String fkBookingUserOriginId) {
        this.fkBookingUserOriginId = fkBookingUserOriginId;
    }

    public String getChangeType() {
        return changeType;
    }

    public void setChangeType(String changeType) {
        this.changeType = changeType;
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

    @Override
    public String toString() {
        return "DatechangeSegFactModel{" +
                "pkId='" + pkId + '\'' +
                ", akPnrNumber='" + akPnrNumber + '\'' +
                ", newTikNum='" + newTikNum + '\'' +
                ", oriTikNum='" + oriTikNum + '\'' +
                ", akBookingOfficeNumber='" + akBookingOfficeNumber + '\'' +
                ", fkExchangeDate='" + fkExchangeDate + '\'' +
                ", fkExchangeTime='" + fkExchangeTime + '\'' +
                ", fkDeparturesDate='" + fkDeparturesDate + '\'' +
                ", fkDeparturesTime='" + fkDeparturesTime + '\'' +
                ", fkDepairport='" + fkDepairport + '\'' +
                ", fkArriairport='" + fkArriairport + '\'' +
                ", fkDcSeg='" + fkDcSeg + '\'' +
                ", passengerType='" + passengerType + '\'' +
                ", enLastName='" + enLastName + '\'' +
                ", enFirstName='" + enFirstName + '\'' +
                ", cnName='" + cnName + '\'' +
                ", certType='" + certType + '\'' +
                ", certNumber='" + certNumber + '\'' +
                //", modifiedCertType='" + modifiedCertType + '\'' +
                ", passengerAge=" + passengerAge +
                ", fkPassengerUserTid='" + fkPassengerUserTid + '\'' +
                //", akBookingDirectionind='" + akBookingDirectionind + '\'' +
                ", akSegcabin='" + akSegcabin + '\'' +
                ", akCabin='" + akCabin + '\'' +
                ", oldAkSegcabin='" + oldAkSegcabin + '\'' +
                ", oldAkCabin='" + oldAkCabin + '\'' +
                ", oldDeparturesDate='" + oldDeparturesDate + '\'' +
                ", oldDeparturesTime='" + oldDeparturesTime + '\'' +
                ", dateChangeType='" + dateChangeType + '\'' +
                ", akDimark='" + akDimark + '\'' +
                ", segmentStatus='" + segmentStatus + '\'' +
                ", ffrf='" + ffrf + '\'' +
                ", ffLevel='" + ffLevel + '\'' +
                ", ffAirline='" + ffAirline + '\'' +
                ", ffAllianceLevel='" + ffAllianceLevel + '\'' +
                ", akCurrency='" + akCurrency + '\'' +
                ", dcCurpayamount=" + dcCurpayamount +
                ", dcCurfee=" + dcCurfee +
                ", cabinbalanceCur=" + cabinbalanceCur +
                ", dcPayamount=" + dcPayamount +
                ", datechangeFee=" + datechangeFee +
                ", cabinbalance=" + cabinbalance +
                ", datechangeCount=" + datechangeCount +
                ", akKeyAccountCode='" + akKeyAccountCode + '\'' +
                ", isKeyAccount=" + isKeyAccount +
                ", changeOrderno='" + changeOrderno + '\'' +
                ", akChannel='" + akChannel + '\'' +
                ", contactMobileNumber='" + contactMobileNumber + '\'' +
                ", contactName='" + contactName + '\'' +
                ", contactLandlineMobileNumber='" + contactLandlineMobileNumber + '\'' +
                ", fkBookingUserTid='" + fkBookingUserTid + '\'' +
                ", fkBookingUserOriginId='" + fkBookingUserOriginId + '\'' +
                ", changeType='" + changeType + '\'' +
                ", dataActive=" + dataActive +
                ", dataActiveTime='" + dataActiveTime + '\'' +
                ", sourceLastUpdatetime='" + sourceLastUpdatetime + '\'' +
                ", systemCreatetime='" + systemCreatetime + '\'' +
                ", systemLastUpdatetime='" + systemLastUpdatetime + '\'' +
                '}';
    }
}
