package com.travelsky.trp.usercenter.data.analysis.model.dwd.fact;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 升舱-机上升舱-业务事实表实体类
 * upgrade_inflight_business_fact
 */
public class UpgradeInflightBusinessFact {


    @JsonProperty("PK_ID")
    private String pkId; // pk_id

    @JsonProperty("ORDER_NO")
    private String orderNo; // 升舱订单号

    @JsonProperty("UPGRADE_DATE")
    private String upgradeDate; // 升舱申请日期

    @JsonProperty("UPGRADE_TIME")
    private String upgradeTime; // 升舱申请时间

    @JsonProperty("ORIG")
    private String orig; // 起飞机场

    @JsonProperty("DEST")
    private String dest; // 到达机场

    @JsonProperty("FLT_DATE")
    private String fltDate; // 航班日期

    @JsonProperty("TAKE_OFF")
    private String takeOff; // 航班时间

    @JsonProperty("PSGNAME")
    private String psgname; // 乘机人姓名

    @JsonProperty("PSG_CARDID")
    private String psgCardid; // 乘机人证件号

    @JsonProperty("FK_PASSENGER_USER_TID")
    private String fkPassengerUserTid; // 乘机人TID

    @JsonProperty("FLY_TYPE")
    private String flyType; // 国内国际标识

    @JsonProperty("TICKET_NUM")
    private String ticketNum; // 关联票号

    @JsonProperty("BEFORE_CABIN")
    private String beforeCabin; // 升舱前舱位

    @JsonProperty("BEFORE_FARE")
    private String beforeFare; // 升舱前舱等

    @JsonProperty("AFTER_CABIN")
    private String afterCabin; // 升舱后舱位

    @JsonProperty("AFTER_FARE")
    private String afterFare; // 升舱后舱等

    @JsonProperty("PAY_TYPE")
    private String payType; // 支付方式

    @JsonProperty("PAY_STATUS")
    private String payStatus;

    @JsonProperty("UPGRADE_PRICE")
    private String upgradePrice; // 升舱金额

    @JsonProperty("MAKEUPFLAG")
    private String makeupflag; // 升舱服务出票数量

    @JsonProperty("IS_UPGRADE_COUPON_USED")
    private Boolean isUpgradeCouponUsed; // 是否使用升舱券

    @JsonProperty("FK_BOOKING_USER_TID")
    private String fkBookingUserTid; // 预订人TID

    @JsonProperty("SYSTEM_CREATETIME")
    private String systemCreatetime; // 本系统创建日期时间

    @JsonProperty("SYSTEM_LAST_UPDATETIME")
    private String systemLastUpdatetime; // 本系统最后更新日期时间


    public String getPkId() {
        return pkId;
    }

    public void setPkId(String pkId) {
        this.pkId = pkId;
    }

    public String getOrderNo() {
        return orderNo;
    }

    public void setOrderNo(String orderNo) {
        this.orderNo = orderNo;
    }

    public String getUpgradeDate() {
        return upgradeDate;
    }

    public void setUpgradeDate(String upgradeDate) {
        this.upgradeDate = upgradeDate;
    }

    public String getUpgradeTime() {
        return upgradeTime;
    }

    public void setUpgradeTime(String upgradeTime) {
        this.upgradeTime = upgradeTime;
    }

    public String getOrig() {
        return orig;
    }

    public void setOrig(String orig) {
        this.orig = orig;
    }

    public String getDest() {
        return dest;
    }

    public void setDest(String dest) {
        this.dest = dest;
    }

    public String getFltDate() {
        return fltDate;
    }

    public void setFltDate(String fltDate) {
        this.fltDate = fltDate;
    }

    public String getTakeOff() {
        return takeOff;
    }

    public void setTakeOff(String takeOff) {
        this.takeOff = takeOff;
    }

    public String getPsgname() {
        return psgname;
    }

    public void setPsgname(String psgname) {
        this.psgname = psgname;
    }

    public String getPsgCardid() {
        return psgCardid;
    }

    public void setPsgCardid(String psgCardid) {
        this.psgCardid = psgCardid;
    }

    public String getFkPassengerUserTid() {
        return fkPassengerUserTid;
    }

    public void setFkPassengerUserTid(String fkPassengerUserTid) {
        this.fkPassengerUserTid = fkPassengerUserTid;
    }

    public String getFlyType() {
        return flyType;
    }

    public void setFlyType(String flyType) {
        this.flyType = flyType;
    }

    public String getTicketNum() {
        return ticketNum;
    }

    public void setTicketNum(String ticketNum) {
        this.ticketNum = ticketNum;
    }

    public String getBeforeCabin() {
        return beforeCabin;
    }

    public void setBeforeCabin(String beforeCabin) {
        this.beforeCabin = beforeCabin;
    }

    public String getBeforeFare() {
        return beforeFare;
    }

    public void setBeforeFare(String beforeFare) {
        this.beforeFare = beforeFare;
    }

    public String getAfterCabin() {
        return afterCabin;
    }

    public void setAfterCabin(String afterCabin) {
        this.afterCabin = afterCabin;
    }

    public String getAfterFare() {
        return afterFare;
    }

    public void setAfterFare(String afterFare) {
        this.afterFare = afterFare;
    }

    public String getPayType() {
        return payType;
    }

    public void setPayType(String payType) {
        this.payType = payType;
    }

    public String getUpgradePrice() {
        return upgradePrice;
    }

    public void setUpgradePrice(String upgradePrice) {
        this.upgradePrice = upgradePrice;
    }

    public String getMakeupflag() {
        return makeupflag;
    }

    public void setMakeupflag(String makeupflag) {
        this.makeupflag = makeupflag;
    }

    public Boolean getUpgradeCouponUsed() {
        return isUpgradeCouponUsed;
    }

    public void setUpgradeCouponUsed(Boolean upgradeCouponUsed) {
        isUpgradeCouponUsed = upgradeCouponUsed;
    }

    public String getFkBookingUserTid() {
        return fkBookingUserTid;
    }

    public void setFkBookingUserTid(String fkBookingUserTid) {
        this.fkBookingUserTid = fkBookingUserTid;
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

    public String getPayStatus() {
        return payStatus;
    }

    public void setPayStatus(String payStatus) {
        this.payStatus = payStatus;
    }


    @Override
    public String toString() {
        return "UpgradeInflightBusinessFact{" +
                "pkId='" + pkId + '\'' +
                ", orderNo='" + orderNo + '\'' +
                ", upgradeDate='" + upgradeDate + '\'' +
                ", upgradeTime='" + upgradeTime + '\'' +
                ", orig='" + orig + '\'' +
                ", dest='" + dest + '\'' +
                ", fltDate='" + fltDate + '\'' +
                ", takeOff='" + takeOff + '\'' +
                ", psgname='" + psgname + '\'' +
                ", psgCardid='" + psgCardid + '\'' +
                ", fkPassengerUserTid='" + fkPassengerUserTid + '\'' +
                ", flyType='" + flyType + '\'' +
                ", ticketNum='" + ticketNum + '\'' +
                ", beforeCabin='" + beforeCabin + '\'' +
                ", beforeFare='" + beforeFare + '\'' +
                ", afterCabin='" + afterCabin + '\'' +
                ", afterFare='" + afterFare + '\'' +
                ", payType='" + payType + '\'' +
                ", payStatus='" + payStatus + '\'' +
                ", upgradePrice='" + upgradePrice + '\'' +
                ", makeupflag='" + makeupflag + '\'' +
                ", isUpgradeCouponUsed=" + isUpgradeCouponUsed +
                ", fkBookingUserTid='" + fkBookingUserTid + '\'' +
                ", systemCreatetime='" + systemCreatetime + '\'' +
                ", systemLastUpdatetime='" + systemLastUpdatetime + '\'' +
                '}';
    }
}
