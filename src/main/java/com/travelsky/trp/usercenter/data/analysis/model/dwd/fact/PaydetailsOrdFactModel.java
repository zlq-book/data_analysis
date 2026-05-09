package com.travelsky.trp.usercenter.data.analysis.model.dwd.fact;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;

/**
 * 支付流水号事实表 - T_DWD_PAYDETAILS_ORD_FACT
 *
 * @author TS.SHA.fuhuazhang
 * @date 2025/7/25  11:26
 */
public class PaydetailsOrdFactModel {
    //主键：数据源代号+订单编号+支付流水号
    @JsonProperty("PK_ID")
    private String pkId;
    //业务大订单编号
    @JsonProperty("BUSINESS_ORDER_ID")
    private String businessOrderId;
    //支付流水号
    @JsonProperty("PAY_NO")
    private String payNo;
    //预订人TID
    @JsonProperty("FK_BOOKING_USER_TID")
    private String fkBookingUserTid;
    //预订人源ID
    @JsonProperty("FK_BOOKING_USER_ORIGIN_ID")
    private String fkBookingUserOriginId;
    //预订日期
    @JsonProperty("FK_BOOKING_DATE")
    private String fkBookingDate;
    //预订时间
    @JsonProperty("FK_BOOKING_TIME")
    private String fkBookingTime;
    //支付日期
    @JsonProperty("FK_PAY_DATE")
    private String fkPaymentDate;
    //支付时间
    @JsonProperty("FK_PAY_TIME")
    private String fkPaymentTime;
    //支付渠道
    @JsonProperty("AK_PAY_CHANNEL")
    private String akPayChannel;
    //支付方式
    @JsonProperty("AK_PAY_METHOD")
    private String akPaymentMethod;
    //现金支付平台
    @JsonProperty("AK_CASH_PAY_PLATFORM")
    private String akCashPlatform;
    //支付状态
    @JsonProperty("AK_PAY_STATUS")
    private String akPaymentStatus;
    //现金支付币种
    @JsonProperty("AK_CASH_CURRENCY")
    private String akCashCurrency;
    //现金支付金额
    @JsonProperty("CASH_AMOUNT")
    private BigDecimal cashAmount;
    //积分支付额
    @JsonProperty("POINT_AMOUNT")
    private BigDecimal pointAmount;
    //鲁雁值支付额
    @JsonProperty("LYVALUE_AMOUNT")
    private BigDecimal lyvalueAmount;
    //是否使用优惠券
    @JsonProperty("AK_COUPON")
    private Boolean akCoupon;
    //优惠券张数
    @JsonProperty("COUPON_COUNT")
    private Integer couponCount;
    //优惠券优惠金额
    @JsonProperty("COUPON_DISCOUNT")
    private Double couponDiscount;
    //优惠券活动名称编码
    @JsonProperty("COUPON_CODE")
    private String couponCode;

    //V2版本删除
    //@JsonProperty("COUPON_TYPE")
    //private String couponType;
    //优惠券券码
    @JsonProperty("COUPON_NO")
    private String couponNo;
    //优惠券名称
    @JsonProperty("COUPON_NAME")
    private String couponName;
    //源系统最后更新时间
    @JsonProperty("SOURCE_LAST_UPDATETIME")
    private String sourceLastUpdatetime;
    //本系统创建日期时间
    @JsonProperty("SYSTEM_CREATETIME")
    private String systemCreatetime;
    //本系统最后更新日期时间
    @JsonProperty("SYSTEM_LAST_UPDATETIME")
    private String systemLastUpdatetime;
    //支付单计数
    @JsonProperty("ORD_COUNT")
    private Integer ordCount = 1;
    //券品类
    @JsonProperty("COUPON_CATEGORY")
    private String couponCategory;

    // Getters and Setters
    public String getPkId() {
        return pkId;
    }

    public void setPkId(String pkId) {
        this.pkId = pkId;
    }

    public String getPayNo() {
        return payNo;
    }

    public void setPayNo(String payNo) {
        this.payNo = payNo;
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

    public String getFkPaymentDate() {
        return fkPaymentDate;
    }

    public void setFkPaymentDate(String fkPaymentDate) {
        this.fkPaymentDate = fkPaymentDate;
    }

    public String getFkPaymentTime() {
        return fkPaymentTime;
    }

    public void setFkPaymentTime(String fkPaymentTime) {
        this.fkPaymentTime = fkPaymentTime;
    }

    public String getAkPayChannel() {
        return akPayChannel;
    }

    public void setAkPayChannel(String akPayChannel) {
        this.akPayChannel = akPayChannel;
    }

    public String getAkPaymentMethod() {
        return akPaymentMethod;
    }

    public void setAkPaymentMethod(String akPaymentMethod) {
        this.akPaymentMethod = akPaymentMethod;
    }

    public String getAkCashPlatform() {
        return akCashPlatform;
    }

    public void setAkCashPlatform(String akCashPlatform) {
        this.akCashPlatform = akCashPlatform;
    }

    public String getAkPaymentStatus() {
        return akPaymentStatus;
    }

    public void setAkPaymentStatus(String akPaymentStatus) {
        this.akPaymentStatus = akPaymentStatus;
    }

    public String getAkCashCurrency() {
        return akCashCurrency;
    }

    public void setAkCashCurrency(String akCashCurrency) {
        this.akCashCurrency = akCashCurrency;
    }

    public BigDecimal getCashAmount() {
        return cashAmount;
    }

    public void setCashAmount(BigDecimal cashAmount) {
        this.cashAmount = cashAmount;
    }

    public BigDecimal getPointAmount() {
        return pointAmount;
    }

    public void setPointAmount(BigDecimal pointAmount) {
        this.pointAmount = pointAmount;
    }

    public BigDecimal getLyvalueAmount() {
        return lyvalueAmount;
    }

    public void setLyvalueAmount(BigDecimal lyvalueAmount) {
        this.lyvalueAmount = lyvalueAmount;
    }

    public Boolean getAkCoupon() {
        return akCoupon;
    }

    public void setAkCoupon(Boolean akCoupon) {
        this.akCoupon = akCoupon;
    }

    public Integer getCouponCount() {
        return couponCount;
    }

    public void setCouponCount(Integer couponCount) {
        this.couponCount = couponCount;
    }

    public Double getCouponDiscount() {
        return couponDiscount;
    }

    public void setCouponDiscount(Double couponDiscount) {
        this.couponDiscount = couponDiscount;
    }

    public String getCouponCode() {
        return couponCode;
    }

    public void setCouponCode(String couponCode) {
        this.couponCode = couponCode;
    }

//    public String getCouponType() {
//        return couponType;
//    }

//    public void setCouponType(String couponType) {
//        this.couponType = couponType;
//    }

    public String getCouponNo() {
        return couponNo;
    }

    public void setCouponNo(String couponNo) {
        this.couponNo = couponNo;
    }

    public String getCouponName() {
        return couponName;
    }

    public void setCouponName(String couponName) {
        this.couponName = couponName;
    }

    public String getSystemCreatetime() {
        return systemCreatetime;
    }

    public void setSystemCreatetime(String systemCreatetime) {
        this.systemCreatetime = systemCreatetime;
    }

    public String getSourceLastUpdatetime() {
        return sourceLastUpdatetime;
    }

    public void setSourceLastUpdatetime(String sourceLastUpdatetime) {
        this.sourceLastUpdatetime = sourceLastUpdatetime;
    }


    public String getSystemLastUpdatetime() {
        return systemLastUpdatetime;
    }

    public void setSystemLastUpdatetime(String systemLastUpdatetime) {
        this.systemLastUpdatetime = systemLastUpdatetime;
    }

    public Integer getOrdCount() {
        return ordCount;
    }

    public void setOrdCount(Integer ordCount) {
        this.ordCount = ordCount;
    }

    public String getBusinessOrderId() {
        return businessOrderId;
    }

    public void setBusinessOrderId(String businessOrderId) {
        this.businessOrderId = businessOrderId;
    }

    public String getCouponCategory() {
        return couponCategory;
    }

    public void setCouponCategory(String couponCategory) {
        this.couponCategory = couponCategory;
    }

    @Override
    public String toString() {
        return "PaydetailsOrdFactModel{" + "pkId='" + pkId + '\'' + ", businessOrderId='" + businessOrderId + '\'' + ", payNo='" + payNo + '\'' + ", fkBookingUserTid='" + fkBookingUserTid + '\'' + ", fkBookingUserOriginId='" + fkBookingUserOriginId + '\'' + ", fkBookingDate='" + fkBookingDate + '\'' + ", fkBookingTime='" + fkBookingTime + '\'' + ", fkPaymentDate='" + fkPaymentDate + '\'' + ", fkPaymentTime='" + fkPaymentTime + '\'' + ", akPayChannel='" + akPayChannel + '\'' + ", akPaymentMethod='" + akPaymentMethod + '\'' + ", akCashPlatform='" + akCashPlatform + '\'' + ", akPaymentStatus='" + akPaymentStatus + '\'' + ", akCashCurrency='" + akCashCurrency + '\'' + ", cashAmount=" + cashAmount + ", pointAmount=" + pointAmount + ", lyvalueAmount=" + lyvalueAmount + ", akCoupon=" + akCoupon + ", couponCount=" + couponCount + ", couponDiscount=" + couponDiscount + ", couponCode='" + couponCode + '\'' + ", couponNo='" + couponNo + '\'' + ", couponName='" + couponName + '\'' + ", sourceLastUpdatetime='" + sourceLastUpdatetime + '\'' + ", systemCreatetime='" + systemCreatetime + '\'' + ", systemLastUpdatetime='" + systemLastUpdatetime + '\'' + ", ordCount=" + ordCount + ", couponCategory='" + couponCategory + '\'' + '}';
    }
}
