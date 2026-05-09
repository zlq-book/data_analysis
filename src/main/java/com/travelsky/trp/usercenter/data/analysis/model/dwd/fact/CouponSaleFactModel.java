package com.travelsky.trp.usercenter.data.analysis.model.dwd.fact;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * DWD层业务事实表 - TRP-休息室券销售
 * T_DWD_TRP_COUPON_SALE_FACT
 */
public class CouponSaleFactModel {

    /**
     * 主键ID (订单号+券码)
     */
    @JsonProperty("PK_ID")
    private String pkId;

    /**
     * 订单号
     */
    @JsonProperty("ORDER_NUMBER")
    private String orderNumber;

    /**
     * 购买渠道
     */
    @JsonProperty("PURCHASE_CHANNEL")
    private String purchaseChannel;

    /**
     * 购买日期
     */
    @JsonProperty("PURCHASE_DATE")
    private String purchaseDate;

    /**
     * 购买时间
     */
    @JsonProperty("PURCHASE_TIME")
    private String purchaseTime;

    /**
     * 券名称
     */
    @JsonProperty("VOUCHER_NAME")
    private String voucherName;

    /**
     * 券面额
     */
    @JsonProperty("VOUCHER_AMOUNT")
    private BigDecimal voucherAmount;

    /**
     * 券支付金额
     */
    @JsonProperty("VOUCHER_PAYMENT_AMOUNT")
    private BigDecimal voucherPaymentAmount;

    /**
     * 券码
     */
    @JsonProperty("VOUCHER_CODE")
    private String voucherCode;

    /**
     * 购买人源ID
     */
    @JsonProperty("FK_BUYER_ORIGIN_ID")
    private String fkBuyerOriginId;

    /**
     * 购买人TID
     */
    @JsonProperty("FK_BUYER_TID")
    private String fkBuyerTid;

    /**
     * 源系统最后更新时间
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

    // Getters and Setters
    public String getPkId() {
        return pkId;
    }

    public void setPkId(String pkId) {
        this.pkId = pkId;
    }

    public String getOrderNumber() {
        return orderNumber;
    }

    public void setOrderNumber(String orderNumber) {
        this.orderNumber = orderNumber;
    }

    public String getPurchaseChannel() {
        return purchaseChannel;
    }

    public void setPurchaseChannel(String purchaseChannel) {
        this.purchaseChannel = purchaseChannel;
    }

    public String getPurchaseDate() {
        return purchaseDate;
    }

    public void setPurchaseDate(String purchaseDate) {
        this.purchaseDate = purchaseDate;
    }

    public String getPurchaseTime() {
        return purchaseTime;
    }

    public void setPurchaseTime(String purchaseTime) {
        this.purchaseTime = purchaseTime;
    }

    public String getVoucherName() {
        return voucherName;
    }

    public void setVoucherName(String voucherName) {
        this.voucherName = voucherName;
    }

    public BigDecimal getVoucherAmount() {
        return voucherAmount;
    }

    public void setVoucherAmount(BigDecimal voucherAmount) {
        this.voucherAmount = voucherAmount;
    }

    public BigDecimal getVoucherPaymentAmount() {
        return voucherPaymentAmount;
    }

    public void setVoucherPaymentAmount(BigDecimal voucherPaymentAmount) {
        this.voucherPaymentAmount = voucherPaymentAmount;
    }

    public String getVoucherCode() {
        return voucherCode;
    }

    public void setVoucherCode(String voucherCode) {
        this.voucherCode = voucherCode;
    }

    public String getFkBuyerOriginId() {
        return fkBuyerOriginId;
    }

    public void setFkBuyerOriginId(String fkBuyerOriginId) {
        this.fkBuyerOriginId = fkBuyerOriginId;
    }

    public String getFkBuyerTid() {
        return fkBuyerTid;
    }

    public void setFkBuyerTid(String fkBuyerTid) {
        this.fkBuyerTid = fkBuyerTid;
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
        return "CouponSaleFactModel{" +
                "pkId='" + pkId + '\'' +
                ", orderNumber='" + orderNumber + '\'' +
                ", purchaseChannel='" + purchaseChannel + '\'' +
                ", purchaseDate='" + purchaseDate + '\'' +
                ", purchaseTime='" + purchaseTime + '\'' +
                ", voucherName='" + voucherName + '\'' +
                ", voucherAmount=" + voucherAmount +
                ", voucherPaymentAmount=" + voucherPaymentAmount +
                ", voucherCode='" + voucherCode + '\'' +
                ", fkBuyerOriginId='" + fkBuyerOriginId + '\'' +
                ", fkBuyerTid='" + fkBuyerTid + '\'' +
                ", sourceLastUpdatetime='" + sourceLastUpdatetime + '\'' +
                ", systemCreatetime='" + systemCreatetime + '\'' +
                ", systemLastUpdatetime='" + systemLastUpdatetime + '\'' +
                '}';
    }
}
