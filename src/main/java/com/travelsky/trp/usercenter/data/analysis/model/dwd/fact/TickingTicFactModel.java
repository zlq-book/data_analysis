package com.travelsky.trp.usercenter.data.analysis.model.dwd.fact;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 机票出票客票级事实表实体类-T_DWD_TICKING_TIC_FACT
 * @author TS.SHA.fuhuazhang
 * @date 2025/7/20  14:21
 */
public class TickingTicFactModel {

    /**
     * 主键ID
     */
    @JsonProperty("PK_ID")
    private String pkId;

    /**
     * 出票类型
     */
    @JsonProperty("AK_TICKET_TYPE")
    private String akTicketType;

    /**
     * PNR编号
     */
    @JsonProperty("PNR_NUMBER")
    private String pnrNumber;

    /**
     * 票号
     */
    @JsonProperty("TICKET_NUMBER")
    private String ticketNumber;

    /**
     * 出票office号
     */
    @JsonProperty("AK_BOOKING_OFFICE_NUMBER")
    private String akBookingOfficeNumber;

    /**
     * 出票日期
     */
    @JsonProperty("FK_TICKETING_DATE")
    private String fkTicketingDate;

    /**
     * 出票时间
     */
    @JsonProperty("FK_TICKETING_TIME")
    private String fkTicketingTime;

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
     * 国内国际标识
     */
    @JsonProperty("AK_DIMARK")
    private String akDimark;

    /**
     * 是否联票
     */
    @JsonProperty("AK_CONJUCTION")
    private Boolean akConjuction;

    /**
     * 运价基础代码FB
     */
    @JsonProperty("AK_FAREBASIS")
    private String akFarebasis;

    /**
     * 是否为无陪儿童票
     */
    @JsonProperty("UNACCOMPANIED_MINOR")
    private Boolean unaccompaniedMinor;

    /**
     * 是否政府采购票
     */
    @JsonProperty("IS_GOVERNMENT_PURCHASE")
    private Boolean isGovernmentPurchase;

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
     * 是否是团队票
     */
    @JsonProperty("AK_TEAMMARK")
    private Boolean akTeammark;

    /**
     * 同行人数
     */
    @JsonProperty("AK_PEER_NUMBER")
    private Integer akPeerNumber;

    /**
     * 是否单人出行
     */
    @JsonProperty("SINGLE_TRAVELER")
    private Boolean singleTraveler;

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
     * 是否首次出票
     */
    @JsonProperty("AK_FIRST_ISSUE")
    private Boolean akFirstIssue;

    /**
     * 预订人是否在乘机人列表
     */
    @JsonProperty("AK_BOOKING_PASSENGER")
    private Boolean akBookingPassenger;

    /**
     * 提前出票天数
     */
    @JsonProperty("AK_ADVBOOK_DAY")
    private Integer akAdvbookDay;

    /**
     * 销售币种
     */
    @JsonProperty("AK_CURRENCY")
    private String akCurrency;

    /**
     * 客票销售总票面价格
     */
    @JsonProperty("FARE_CURAMOUNT")
    private BigDecimal fareCuramount;

    /**
     * 票款金额不含税CNY
     */
    @JsonProperty("FARE_AMOUNT")
    private BigDecimal fareAmount;

    /**
     * 出票计数
     */
    @JsonProperty("TICKET_COUNT")
    private Integer ticketCount = 1;

    /**
     * 渠道订单号
     */
    @JsonProperty("AK_ORDERNUM")
    private String akOrdernum;

    /**
     * 预订渠道
     */
    @JsonProperty("AK_CHANNEL")
    private String akChannel;

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

    public String getAkTicketType() {
        return akTicketType;
    }

    public void setAkTicketType(String akTicketType) {
        this.akTicketType = akTicketType;
    }

    public String getPnrNumber() {
        return pnrNumber;
    }

    public void setPnrNumber(String pnrNumber) {
        this.pnrNumber = pnrNumber;
    }

    public String getTicketNumber() {
        return ticketNumber;
    }

    public void setTicketNumber(String ticketNumber) {
        this.ticketNumber = ticketNumber;
    }

    public String getAkBookingOfficeNumber() {
        return akBookingOfficeNumber;
    }

    public void setAkBookingOfficeNumber(String akBookingOfficeNumber) {
        this.akBookingOfficeNumber = akBookingOfficeNumber;
    }

    public String getFkTicketingDate() {
        return fkTicketingDate;
    }

    public void setFkTicketingDate(String fkTicketingDate) {
        this.fkTicketingDate = fkTicketingDate;
    }

    public String getFkTicketingTime() {
        return fkTicketingTime;
    }

    public void setFkTicketingTime(String fkTicketingTime) {
        this.fkTicketingTime = fkTicketingTime;
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

    public String getAkDimark() {
        return akDimark;
    }

    public void setAkDimark(String akDimark) {
        this.akDimark = akDimark;
    }

    public Boolean getAkConjuction() {
        return akConjuction;
    }

    public void setAkConjuction(Boolean akConjuction) {
        this.akConjuction = akConjuction;
    }

    public String getAkFarebasis() {
        return akFarebasis;
    }

    public void setAkFarebasis(String akFarebasis) {
        this.akFarebasis = akFarebasis;
    }

    public Boolean getUnaccompaniedMinor() {
        return unaccompaniedMinor;
    }

    public void setUnaccompaniedMinor(Boolean unaccompaniedMinor) {
        this.unaccompaniedMinor = unaccompaniedMinor;
    }

    public Boolean getGovernmentPurchase() {
        return isGovernmentPurchase;
    }

    public void setGovernmentPurchase(Boolean governmentPurchase) {
        isGovernmentPurchase = governmentPurchase;
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

    public Boolean getAkTeammark() {
        return akTeammark;
    }

    public void setAkTeammark(Boolean akTeammark) {
        this.akTeammark = akTeammark;
    }

    public Integer getAkPeerNumber() {
        return akPeerNumber;
    }

    public void setAkPeerNumber(Integer akPeerNumber) {
        this.akPeerNumber = akPeerNumber;
    }

    public Boolean getSingleTraveler() {
        return singleTraveler;
    }

    public void setSingleTraveler(Boolean singleTraveler) {
        this.singleTraveler = singleTraveler;
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

    public Boolean getAkFirstIssue() {
        return akFirstIssue;
    }

    public void setAkFirstIssue(Boolean akFirstIssue) {
        this.akFirstIssue = akFirstIssue;
    }

    public Boolean getAkBookingPassenger() {
        return akBookingPassenger;
    }

    public void setAkBookingPassenger(Boolean akBookingPassenger) {
        this.akBookingPassenger = akBookingPassenger;
    }

    public Integer getAkAdvbookDay() {
        return akAdvbookDay;
    }

    public void setAkAdvbookDay(Integer akAdvbookDay) {
        this.akAdvbookDay = akAdvbookDay;
    }

    public String getAkCurrency() {
        return akCurrency;
    }

    public void setAkCurrency(String akCurrency) {
        this.akCurrency = akCurrency;
    }

    public BigDecimal getFareCuramount() {
        return fareCuramount;
    }

    public void setFareCuramount(BigDecimal fareCuramount) {
        this.fareCuramount = fareCuramount;
    }

    public BigDecimal getFareAmount() {
        return fareAmount;
    }

    public void setFareAmount(BigDecimal fareAmount) {
        this.fareAmount = fareAmount;
    }

    public Integer getTicketCount() {
        return ticketCount;
    }

    public void setTicketCount(Integer ticketCount) {
        this.ticketCount = ticketCount;
    }

    public String getAkOrdernum() {
        return akOrdernum;
    }

    public void setAkOrdernum(String akOrdernum) {
        this.akOrdernum = akOrdernum;
    }

    public String getAkChannel() {
        return akChannel;
    }

    public void setAkChannel(String akChannel) {
        this.akChannel = akChannel;
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
        return "TickingTicFactModel{" +
                "pkId='" + pkId + '\'' +
                ", akTicketType='" + akTicketType + '\'' +
                ", pnrNumber='" + pnrNumber + '\'' +
                ", ticketNumber='" + ticketNumber + '\'' +
                ", akBookingOfficeNumber='" + akBookingOfficeNumber + '\'' +
                ", fkTicketingDate='" + fkTicketingDate + '\'' +
                ", fkTicketingTime='" + fkTicketingTime + '\'' +
                ", passengerType='" + passengerType + '\'' +
                ", enLastName='" + enLastName + '\'' +
                ", enFirstName='" + enFirstName + '\'' +
                ", cnName='" + cnName + '\'' +
                ", certType='" + certType + '\'' +
                ", certNumber='" + certNumber + '\'' +
                ", passengerAge=" + passengerAge +
                ", fkPassengerUserTid='" + fkPassengerUserTid + '\'' +
                ", ffrf='" + ffrf + '\'' +
                ", ffLevel='" + ffLevel + '\'' +
                ", ffAirline='" + ffAirline + '\'' +
                ", ffAllianceLevel='" + ffAllianceLevel + '\'' +
                ", akDimark='" + akDimark + '\'' +
                ", akConjuction=" + akConjuction +
                ", akFarebasis='" + akFarebasis + '\'' +
                ", unaccompaniedMinor=" + unaccompaniedMinor +
                ", isGovernmentPurchase=" + isGovernmentPurchase +
                ", akKeyAccountCode='" + akKeyAccountCode + '\'' +
                ", isKeyAccount=" + isKeyAccount +
                ", akTeammark=" + akTeammark +
                ", akPeerNumber=" + akPeerNumber +
                ", singleTraveler=" + singleTraveler +
                ", akPeerChd=" + akPeerChd +
                ", peerSenior=" + peerSenior +
                ", selfBooking=" + selfBooking +
                ", akFirstIssue=" + akFirstIssue +
                ", akBookingPassenger=" + akBookingPassenger +
                ", akAdvbookDay=" + akAdvbookDay +
                ", akCurrency='" + akCurrency + '\'' +
                ", fareCuramount=" + fareCuramount +
                ", fareAmount=" + fareAmount +
                ", ticketCount=" + ticketCount +
                ", akOrdernum='" + akOrdernum + '\'' +
                ", akChannel='" + akChannel + '\'' +
                ", fkBookingUserTid='" + fkBookingUserTid + '\'' +
                ", fkBookingUserOriginId='" + fkBookingUserOriginId + '\'' +
                ", contactMobileNumber='" + contactMobileNumber + '\'' +
                ", contactName='" + contactName + '\'' +
                ", contactLandlineMobileNumber='" + contactLandlineMobileNumber + '\'' +
                ", dataActive=" + dataActive +
                ", dataActiveTime='" + dataActiveTime + '\'' +
                ", sourceLastUpdatetime='" + sourceLastUpdatetime + '\'' +
                ", systemCreatetime='" + systemCreatetime + '\'' +
                ", systemLastUpdatetime='" + systemLastUpdatetime + '\'' +
                '}';
    }
}
