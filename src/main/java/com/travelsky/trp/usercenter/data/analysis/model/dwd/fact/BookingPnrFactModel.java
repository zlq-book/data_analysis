package com.travelsky.trp.usercenter.data.analysis.model.dwd.fact;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;

/**
 * 机票预订PNR级事实表实体类-T_DWD_BOOKING_PNR_FACT
 * @author TS.SHA.fuhuazhang
 * @date 2025/7/20  14:08
 */
public class BookingPnrFactModel {
    /**
     * 主键ID
     */
    @JsonProperty("PK_ID")
    private String pkId;

    /**
     * PNR编号
     */
    @JsonProperty("PNR_NUMBER")
    private String pnrNumber;

    /**
     * PNR创建日期
     */
    @JsonProperty("FK_PNRCREATE_DATE")
    private String fkPnrcreateDate;

    /**
     * PNR创建时间
     */
    @JsonProperty("FK_PNRCREATE_TIME")
    private String fkPnrcreateTime;

    /**
     * 预订本航段使用的GDS系统
     */
    @JsonProperty("GDS_CODE")
    private String gdsCode;

    /**
     * 订座office
     */
    @JsonProperty("AK_BOOKING_OFFICE")
    private String akBookingOffice;

    /**
     * 是否是团队票
     */
    @JsonProperty("AK_TEAMMARK")
    private Boolean akTeammark;

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
     * 同行人数
     */
    @JsonProperty("AK_PEER_NUMBER")
    private Integer akPeerNumber;

    /**
     * 是否与儿童同行
     */
    @JsonProperty("PEER_CHD")
    private Boolean peerChd;

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
     * 是否单人出行
     */
    @JsonProperty("SINGLE_TRAVEL")
    private Boolean singleTravel;

    /**
     * PNR计数
     */
    @JsonProperty("PNR_COUNT")
    private Integer pnrCount = 1;

    /**
     * 渠道订单号
     */
    @JsonProperty("CHANNEL_ORDER_ID")
    private String channelOrderId;

    /**
     * 预订渠道
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

    ///**
    // * 联系人国际电话代码
    // */
    //@JsonProperty("CONTACT_INTER_MOBILE_CODE")
    //private String contactInterMobileCode;

    /**
     * 联系人固定电话
     */
    @JsonProperty("CONTACT_LANDLINE_MOBILE_NUMBER")
    private String contactLandlineMobileNumber;

    /**
     * 联系人邮箱
     */
    //@JsonProperty("CONTACT_EMAIL")
    //private String contactEmail;

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
     * 预订人是否在乘机人列表
     */
    @JsonProperty("AK_BOOKER_IS_PASSENGER")
    private Boolean akBookerIsPassenger;

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

    public String getPnrNumber() {
        return pnrNumber;
    }

    public void setPnrNumber(String pnrNumber) {
        this.pnrNumber = pnrNumber;
    }

    public String getFkPnrcreateDate() {
        return fkPnrcreateDate;
    }

    public void setFkPnrcreateDate(String fkPnrcreateDate) {
        this.fkPnrcreateDate = fkPnrcreateDate;
    }

    public String getFkPnrcreateTime() {
        return fkPnrcreateTime;
    }

    public void setFkPnrcreateTime(String fkPnrcreateTime) {
        this.fkPnrcreateTime = fkPnrcreateTime;
    }

    public String getGdsCode() {
        return gdsCode;
    }

    public void setGdsCode(String gdsCode) {
        this.gdsCode = gdsCode;
    }

    public String getAkBookingOffice() {
        return akBookingOffice;
    }

    public void setAkBookingOffice(String akBookingOffice) {
        this.akBookingOffice = akBookingOffice;
    }

    public Boolean getAkTeammark() {
        return akTeammark;
    }

    public void setAkTeammark(Boolean akTeammark) {
        this.akTeammark = akTeammark;
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

    public Integer getAkPeerNumber() {
        return akPeerNumber;
    }

    public void setAkPeerNumber(Integer akPeerNumber) {
        this.akPeerNumber = akPeerNumber;
    }

    public Boolean getPeerChd() {
        return peerChd;
    }

    public void setPeerChd(Boolean peerChd) {
        this.peerChd = peerChd;
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

    public Boolean getSingleTravel() {
        return singleTravel;
    }

    public void setSingleTravel(Boolean singleTravel) {
        this.singleTravel = singleTravel;
    }

    public Integer getPnrCount() {
        return pnrCount;
    }

    public void setPnrCount(Integer pnrCount) {
        this.pnrCount = pnrCount;
    }

    public String getChannelOrderId() {
        return channelOrderId;
    }

    public void setChannelOrderId(String channelOrderId) {
        this.channelOrderId = channelOrderId;
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

    //public String getContactInterMobileCode() {
    //    return contactInterMobileCode;
    //}
    //
    //public void setContactInterMobileCode(String contactInterMobileCode) {
    //    this.contactInterMobileCode = contactInterMobileCode;
    //}

    public String getContactLandlineMobileNumber() {
        return contactLandlineMobileNumber;
    }

    public void setContactLandlineMobileNumber(String contactLandlineMobileNumber) {
        this.contactLandlineMobileNumber = contactLandlineMobileNumber;
    }

    //public String getContactEmail() {
    //    return contactEmail;
    //}
    //
    //public void setContactEmail(String contactEmail) {
    //    this.contactEmail = contactEmail;
    //}

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

    public Boolean getAkBookerIsPassenger() {
        return akBookerIsPassenger;
    }

    public void setAkBookerIsPassenger(Boolean akBookerIsPassenger) {
        this.akBookerIsPassenger = akBookerIsPassenger;
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
        return "{"
                + "BookingPnrFactModel: {"
                + "pkId: " + pkId
                + ", " + "pnrNumber: " + pnrNumber
                + ", " + "fkPnrcreateDate: " + fkPnrcreateDate
                + ", " + "fkPnrcreateTime: " + fkPnrcreateTime
                + ", " + "gdsCode: " + gdsCode
                + ", " + "akBookingOffice: " + akBookingOffice
                + ", " + "akTeammark: " + akTeammark
                + ", " + "akKeyAccountCode: " + akKeyAccountCode
                + ", " + "isKeyAccount: " + isKeyAccount
                + ", " + "akPeerNumber: " + akPeerNumber
                + ", " + "peerChd: " + peerChd
                + ", " + "peerSenior: " + peerSenior
                //+ ", " + "peerInfant: " + peerInfant
                + ", " + "singleTravel: " + singleTravel
                + ", " + "pnrCount: " + pnrCount
                + ", " + "channelOrderId: " + channelOrderId
                + ", " + "akChannel: " + akChannel
                + ", " + "contactMobileNumber: " + contactMobileNumber
                + ", " + "contactName: " + contactName
                //+ ", " + "contactInterMobileCode: " + contactInterMobileCode
                + ", " + "contactLandlineMobileNumber: " + contactLandlineMobileNumber
                //+ ", " + "contactEmail: " + contactEmail
                + ", " + "fkBookingUserTid: " + fkBookingUserTid
                + ", " + "fkBookingUserOriginId: " + fkBookingUserOriginId
                + ", " + "akBookerIsPassenger: " + akBookerIsPassenger
                + ", " + "dataActive: " + dataActive
                + ", " + "dataActiveTime: " + dataActiveTime
                + ", " + "sourceLastUpdatetime: " + sourceLastUpdatetime
                + ", " + "systemCreatetime: " + systemCreatetime
                + ", " + "systemLastUpdatetime: " + systemLastUpdatetime
                + "}"
                + "}";
    }
}
