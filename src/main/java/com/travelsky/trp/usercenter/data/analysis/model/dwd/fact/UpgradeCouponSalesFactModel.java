package com.travelsky.trp.usercenter.data.analysis.model.dwd.fact;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 升舱-券销售-业务事实表-T_DWD_UPGRADE_COUPON_SALES_FACT
 *
 * @author TS.SHA.fuhuazhang
 * @date 2025/10/11  14:08
 */
public class UpgradeCouponSalesFactModel {
    /**
     * 主键ID
     */
    @JsonProperty("PK_ID")
    private String pkId;

    /**
     * 销售日期
     */
    @JsonProperty("SALES_DATE")
    private String salesDate;

    /**
     * 销售时间
     */
    @JsonProperty("SALES_TIME")
    private String salesTime;

    /**
     * 销售渠道
     */
    @JsonProperty("SALES_CHANNEL")
    private String salesChannel;

    /**
     * 销售订单号
     */
    @JsonProperty("SALES_ORDER_NUMBER")
    private String salesOrderNumber;

    /**
     * 购买人TID
     */
    @JsonProperty("BUYER_TID")
    private String buyerTid;

    /**
     * 购买人源ID
     */
    @JsonProperty("BUYER_SOURCE_ID")
    private String buyerSourceId;

    /**
     * 券码
     */
    @JsonProperty("COUPON_CODE")
    private String couponCode;

    /**
     * 券面额
     */
    @JsonProperty("COUPON_VALUE")
    private BigDecimal couponValue;

    /**
     * 券状态
     */
    @JsonProperty("COUPON_STATUS")
    private String couponStatus;

    /**
     * 券使用日期
     */
    @JsonProperty("COUPON_USE_DATE")
    private String couponUseDate;

    /**
     * 券使用时间
     */
    @JsonProperty("COUPON_USE_TIME")
    private String couponUseTime;

    /**
     * 券数量计数
     */
    @JsonProperty("COUPON_COUNT")
    private Integer couponCount = 1;

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

    @Override
    public String toString() {
        return "{"
                + "UpgradeCouponSalesFactModel: {"
                + "pkId: " + pkId
                + ", " + "salesDate: " + salesDate
                + ", " + "salesTime: " + salesTime
                + ", " + "salesChannel: " + salesChannel
                + ", " + "salesOrderNumber: " + salesOrderNumber
                + ", " + "buyerTid: " + buyerTid
                + ", " + "buyerSourceId: " + buyerSourceId
                + ", " + "couponCode: " + couponCode
                + ", " + "couponValue: " + couponValue
                + ", " + "couponStatus: " + couponStatus
                + ", " + "couponUseDate: " + couponUseDate
                + ", " + "couponUseTime: " + couponUseTime
                + ", " + "couponCount: " + couponCount
                + ", " + "systemCreatetime: " + systemCreatetime
                + ", " + "systemLastUpdatetime: " + systemLastUpdatetime
                + "}"
                + "}";
    }

    public String getPkId() {
        return this.pkId;
    }

    public String getSalesDate() {
        return this.salesDate;
    }

    public String getSalesTime() {
        return this.salesTime;
    }

    public String getSalesChannel() {
        return this.salesChannel;
    }

    public String getSalesOrderNumber() {
        return this.salesOrderNumber;
    }

    public String getBuyerTid() {
        return this.buyerTid;
    }

    public String getBuyerSourceId() {
        return this.buyerSourceId;
    }

    public String getCouponCode() {
        return this.couponCode;
    }

    public BigDecimal getCouponValue() {
        return this.couponValue;
    }

    public String getCouponStatus() {
        return this.couponStatus;
    }

    public String getCouponUseDate() {
        return this.couponUseDate;
    }

    public String getCouponUseTime() {
        return this.couponUseTime;
    }

    public Integer getCouponCount() {
        return this.couponCount;
    }

    public String getSystemCreatetime() {
        return this.systemCreatetime;
    }

    public String getSystemLastUpdatetime() {
        return this.systemLastUpdatetime;
    }

    @JsonProperty("PK_ID")
    public void setPkId(String pkId) {
        this.pkId = pkId;
    }

    @JsonProperty("SALES_DATE")
    public void setSalesDate(String salesDate) {
        this.salesDate = salesDate;
    }

    @JsonProperty("SALES_TIME")
    public void setSalesTime(String salesTime) {
        this.salesTime = salesTime;
    }

    @JsonProperty("SALES_CHANNEL")
    public void setSalesChannel(String salesChannel) {
        this.salesChannel = salesChannel;
    }

    @JsonProperty("SALES_ORDER_NUMBER")
    public void setSalesOrderNumber(String salesOrderNumber) {
        this.salesOrderNumber = salesOrderNumber;
    }

    @JsonProperty("BUYER_TID")
    public void setBuyerTid(String buyerTid) {
        this.buyerTid = buyerTid;
    }

    @JsonProperty("BUYER_SOURCE_ID")
    public void setBuyerSourceId(String buyerSourceId) {
        this.buyerSourceId = buyerSourceId;
    }

    @JsonProperty("COUPON_CODE")
    public void setCouponCode(String couponCode) {
        this.couponCode = couponCode;
    }

    @JsonProperty("COUPON_VALUE")
    public void setCouponValue(BigDecimal couponValue) {
        this.couponValue = couponValue;
    }

    @JsonProperty("COUPON_STATUS")
    public void setCouponStatus(String couponStatus) {
        this.couponStatus = couponStatus;
    }

    @JsonProperty("COUPON_USE_DATE")
    public void setCouponUseDate(String couponUseDate) {
        this.couponUseDate = couponUseDate;
    }

    @JsonProperty("COUPON_USE_TIME")
    public void setCouponUseTime(String couponUseTime) {
        this.couponUseTime = couponUseTime;
    }

    @JsonProperty("COUPON_COUNT")
    public void setCouponCount(Integer couponCount) {
        this.couponCount = couponCount;
    }

    @JsonProperty("SYSTEM_CREATETIME")
    public void setSystemCreatetime(String systemCreatetime) {
        this.systemCreatetime = systemCreatetime;
    }

    @JsonProperty("SYSTEM_LAST_UPDATETIME")
    public void setSystemLastUpdatetime(String systemLastUpdatetime) {
        this.systemLastUpdatetime = systemLastUpdatetime;
    }
}
