package com.travelsky.trp.usercenter.data.analysis.model.dwd.fact;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;

/**
 * 机票预订航段级事实表实体类-T_DWD_BOOKING_SEG_FACT
 * @author TS.SHA.fuhuazhang
 * @date 2025/7/20  14:13
 */
public class BookingSegFactModel {
    /**
     * 主键ID
     */
    @JsonProperty("PK_ID")
    private String pkId;

    /**
     * 旧主键ID 只做数据记录用 不和表进行关系映射
     */
    private String oldPkId;

    /**
     * PNR编号
     */
    @JsonProperty("PNR_NUMBER")
    private String pnrNumber;

    /**
     * 预订日期
     */
    @JsonProperty("FK_BOOKING_DATE")
    private String fkBookingDate;

    /**
     * 预订时间
     */
    @JsonProperty("FK_BOOKING_TIME")
    private String fkBookingTime;

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
    @JsonProperty("FK_BOOKING_SEG")
    private String fkBookingSeg;

    /**
     * 航班日期
     */
    @JsonProperty("FK_SEG_DATE")
    private String fkSegDate;

    /**
     * 航班时间
     */
    @JsonProperty("FK_SEG_TIME")
    private String fkSegTime;

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
     * VVIP标识
     */
    @JsonProperty("VVIP")
    private String vvip;

    /**
     * 订座office
     */
    @JsonProperty("AK_BOOKING_OFFICE_NUMBER")
    private String akBookingOfficeNumber;

    /**
     * 是否是团队票
     */
    @JsonProperty("AK_TEAMMARK")
    private Boolean akTeammark;

    /**
     * 该航段是否取消预订
     */
    @JsonProperty("IS_CANCLED")
    private Boolean isCancled;

    /**
     * PNR航段行动代码（HK等）
     */
    @JsonProperty("AK_SEG_STATUS")
    private String akSegStatus;

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
     * 飞行时是否使用常客卡
     * */
    @JsonProperty("IS_USED_EFR")
    private Boolean isUsedFfr;

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
     * 提前预订天数
     */
    @JsonProperty("AK_ADVBOOK_DAY")
    private Integer akAdvbookDay;

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
     * 是否为返乡段
     */
    @JsonProperty("HOMECOMING_SEG")
    private Boolean homecomingSeg;

    /**
     * 是否单人出行
     */
    @JsonProperty("SINGLE_TRAVEL")
    private Boolean singleTravel;

    /**
     * 航段计数
     */
    @JsonProperty("SEG_COUNT")
    private Integer segCount = 1;

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
     * 预订人是否在乘机人列表
     */
    @JsonProperty("AK_BOOKER_IS_PASSENGER")
    private Boolean akBookerIsPassenger;


    /**
     * 票价折扣
     */
    @JsonProperty("DISCOUNT")
    private Double discount;

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

    public String getOldPkId() {
        return oldPkId;
    }

    public void setOldPkId(String oldPkId) {
        this.oldPkId = oldPkId;
    }

    public String getPnrNumber() {
        return pnrNumber;
    }

    public void setPnrNumber(String pnrNumber) {
        this.pnrNumber = pnrNumber;
    }

    public String getFkBookingDate() {
        return fkBookingDate;
    }

    public void setFkBookingDate(String fkBookingDate) {
        this.fkBookingDate = fkBookingDate;
    }

    public String getFkBookingTime() {
        return fkBookingTime;
    }

    public void setFkBookingTime(String fkBookingTime) {
        this.fkBookingTime = fkBookingTime;
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

    public String getFkBookingSeg() {
        return fkBookingSeg;
    }

    public void setFkBookingSeg(String fkBookingSeg) {
        this.fkBookingSeg = fkBookingSeg;
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

    public String getVvip() {
        return vvip;
    }

    public void setVvip(String vvip) {
        this.vvip = vvip;
    }

    public String getAkBookingOfficeNumber() {
        return akBookingOfficeNumber;
    }

    public void setAkBookingOfficeNumber(String akBookingOfficeNumber) {
        this.akBookingOfficeNumber = akBookingOfficeNumber;
    }

    public Boolean getAkTeammark() {
        return akTeammark;
    }

    public void setAkTeammark(Boolean akTeammark) {
        this.akTeammark = akTeammark;
    }

    public Boolean getCancled() {
        return isCancled;
    }

    public void setCancled(Boolean cancled) {
        isCancled = cancled;
    }

    public String getAkSegStatus() {
        return akSegStatus;
    }

    public void setAkSegStatus(String akSegStatus) {
        this.akSegStatus = akSegStatus;
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

    public Boolean getUsedFfr() {
        return isUsedFfr;
    }

    public void setUsedFfr(Boolean usedFfr) {
        isUsedFfr = usedFfr;
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

    public Integer getAkAdvbookDay() {
        return akAdvbookDay;
    }

    public void setAkAdvbookDay(Integer akAdvbookDay) {
        this.akAdvbookDay = akAdvbookDay;
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

    public Boolean getHomecomingSeg() {
        return homecomingSeg;
    }

    public void setHomecomingSeg(Boolean homecomingSeg) {
        this.homecomingSeg = homecomingSeg;
    }

    public Boolean getSingleTravel() {
        return singleTravel;
    }

    public void setSingleTravel(Boolean singleTravel) {
        this.singleTravel = singleTravel;
    }

    public Integer getSegCount() {
        return segCount;
    }

    public void setSegCount(Integer segCount) {
        this.segCount = segCount;
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

    public Boolean getAkBookerIsPassenger() {
        return akBookerIsPassenger;
    }

    public void setAkBookerIsPassenger(Boolean akBookerIsPassenger) {
        this.akBookerIsPassenger = akBookerIsPassenger;
    }

    public Double getDiscount() {
        return discount;
    }

    public void setDiscount(Double discount) {
        this.discount = discount;
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
        return "BookingSegFactModel{" +
                "pkId='" + pkId + '\'' +
                ", oldPkId='" + oldPkId + '\'' +
                ", pnrNumber='" + pnrNumber + '\'' +
                ", fkBookingDate='" + fkBookingDate + '\'' +
                ", fkBookingTime='" + fkBookingTime + '\'' +
                ", fkDepairport='" + fkDepairport + '\'' +
                ", fkArriairport='" + fkArriairport + '\'' +
                ", fkBookingSeg='" + fkBookingSeg + '\'' +
                ", fkSegDate='" + fkSegDate + '\'' +
                ", fkSegTime='" + fkSegTime + '\'' +
                ", passengerType='" + passengerType + '\'' +
                ", enLastName='" + enLastName + '\'' +
                ", enFirstName='" + enFirstName + '\'' +
                ", cnName='" + cnName + '\'' +
                ", certType='" + certType + '\'' +
                ", certNumber='" + certNumber + '\'' +
                ", passengerAge=" + passengerAge +
                ", fkPassengerUserTid='" + fkPassengerUserTid + '\'' +
                ", vvip='" + vvip + '\'' +
                ", akBookingOfficeNumber='" + akBookingOfficeNumber + '\'' +
                ", akTeammark=" + akTeammark +
                ", isCancled=" + isCancled +
                ", akSegStatus='" + akSegStatus + '\'' +
                ", akCabin='" + akCabin + '\'' +
                ", akSegcabin='" + akSegcabin + '\'' +
                ", isUsedFfr=" + isUsedFfr +
                ", ffrf='" + ffrf + '\'' +
                ", ffLevel='" + ffLevel + '\'' +
                ", ffAirline='" + ffAirline + '\'' +
                ", ffAllianceLevel='" + ffAllianceLevel + '\'' +
                ", akAdvbookDay=" + akAdvbookDay +
                ", akKeyAccountCode='" + akKeyAccountCode + '\'' +
                ", isKeyAccount=" + isKeyAccount +
                ", akPeerNumber=" + akPeerNumber +
                ", akPeerChd=" + akPeerChd +
                ", peerSenior=" + peerSenior +
                //", peerInfant=" + peerInfant +
                ", selfBooking=" + selfBooking +
                ", homecomingSeg=" + homecomingSeg +
                ", singleTravel=" + singleTravel +
                ", segCount=" + segCount +
                ", channelOrderId='" + channelOrderId + '\'' +
                ", akChannel='" + akChannel + '\'' +
                ", contactMobileNumber='" + contactMobileNumber + '\'' +
                ", contactName='" + contactName + '\'' +
                ", contactLandlineMobileNumber='" + contactLandlineMobileNumber + '\'' +
                ", fkBookingUserTid='" + fkBookingUserTid + '\'' +
                ", fkBookingUserOriginId='" + fkBookingUserOriginId + '\'' +
                ", akBookerIsPassenger=" + akBookerIsPassenger +
                ", discount=" + discount +
                ", dataActive=" + dataActive +
                ", dataActiveTime='" + dataActiveTime + '\'' +
                ", sourceLastUpdatetime='" + sourceLastUpdatetime + '\'' +
                ", systemCreatetime='" + systemCreatetime + '\'' +
                ", systemLastUpdatetime='" + systemLastUpdatetime + '\'' +
                '}';
    }
}
