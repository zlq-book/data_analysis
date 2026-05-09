package com.travelsky.trp.usercenter.data.analysis.model.dwd.fact;

import java.time.LocalDateTime;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 明细事实表-掌尚飞-抖音券码购买
 */
public class ZsfTiktokVchrPurFactModel {

    /**
     * pk_id
     */
    @JsonProperty("PK_ID")
    private String pkId;

    /**
     * 抖音侧订单号
     */
    @JsonProperty("AK_ORDER_ID")
    private String akOrderId;

    /**
     * 发码数量
     */
    @JsonProperty("ISSUED_COUNT")
    private Integer issuedCount;

    /**
     * 有效期开始时间
     */
    @JsonProperty("START_TIME")
    private String startTime;

    /**
     * 有效期截止时间
     */
    @JsonProperty("EXPIRE_TIME")
    private String expireTime;

    /**
     * 抖音商品名
     */
    @JsonProperty("DOUYIN_GOODS_NAME")
    private String douyinGoodsName;

    /**
     * 抖音商品ID
     */
    @JsonProperty("DOUYIN_GOODS_ID")
    private String douyinGoodsId;

    /**
     * 次卡编码
     */
    @JsonProperty("CARD_CODE")
    private String cardCode;

    /**
     * 次卡券码
     */
    @JsonProperty("COUPON_CODE")
    private String couponCode;

    /**
     * 购买人手机号
     */
    @JsonProperty("PURCHASER_MOBILE_NUMBER")
    private String purchaserMobileNumber;

    /**
     * 状态
     */
    @JsonProperty("COUPON_STATUS")
    private String couponStatus;

    /**
     * 核销触发时间
     */
    @JsonProperty("VERIFICATION_TIME")
    private String verificationTime;

    /**
     * 退款类型
     */
    @JsonProperty("REFUND_TYPE")
    private Integer refundType;

    /**
     * 退款ID
     */
    @JsonProperty("REFUND_ID")
    private String refundId;

    /**
     * 券ID
     */
    @JsonProperty("COUPON_ID")
    private String couponId;

    /**
     * 退款申请时间
     */
    @JsonProperty("REFUND_APPLY_TIME")
    private String refundApplyTime;

    /**
     * 退款审核时间
     */
    @JsonProperty("REFUND_AUDIT_TIME")
    private String refundAuditTime;

    /**
     * 审核结果
     */
    @JsonProperty("REFUND_AUDIT_RESULT")
    private Integer refundAuditResult;

    /**
     * 购买时间
     */
    @JsonProperty("FK_PURCHASE_TIME")
    private String fkPurchaseTime;

    /**
     * 购买日期
     */
    @JsonProperty("FK_PURCHASE_DATE")
    private String fkPurchaseDate;

    /**
     * 更新时间
     */
    @JsonProperty("UPDATE_TIME")
    private String updateTime;

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
     * 购买次数计数
     */
    @JsonProperty("PURCHASE_COUNT")
    private Integer purchaseCount = 1;


    /**
     * 源系统最后更新时间（时间戳）
     */
    @JsonProperty("SOURCE_LAST_UPDATETIME")
    private String sourceLastUpdatetime = LocalDateTime.now().toString();

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

    public String getAkOrderId() {
        return akOrderId;
    }

    public void setAkOrderId(String akOrderId) {
        this.akOrderId = akOrderId;
    }

    public Integer getIssuedCount() {
        return issuedCount;
    }

    public void setIssuedCount(Integer issuedCount) {
        this.issuedCount = issuedCount;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getExpireTime() {
        return expireTime;
    }

    public void setExpireTime(String expireTime) {
        this.expireTime = expireTime;
    }

    public String getDouyinGoodsName() {
        return douyinGoodsName;
    }

    public void setDouyinGoodsName(String douyinGoodsName) {
        this.douyinGoodsName = douyinGoodsName;
    }

    public String getDouyinGoodsId() {
        return douyinGoodsId;
    }

    public void setDouyinGoodsId(String douyinGoodsId) {
        this.douyinGoodsId = douyinGoodsId;
    }

    public String getCardCode() {
        return cardCode;
    }

    public void setCardCode(String cardCode) {
        this.cardCode = cardCode;
    }

    public String getCouponCode() {
        return couponCode;
    }

    public void setCouponCode(String couponCode) {
        this.couponCode = couponCode;
    }

    public String getPurchaserMobileNumber() {
        return purchaserMobileNumber;
    }

    public void setPurchaserMobileNumber(String purchaserMobileNumber) {
        this.purchaserMobileNumber = purchaserMobileNumber;
    }

    public String getCouponStatus() {
        return couponStatus;
    }

    public void setCouponStatus(String couponStatus) {
        this.couponStatus = couponStatus;
    }

    public String getVerificationTime() {
        return verificationTime;
    }

    public void setVerificationTime(String verificationTime) {
        this.verificationTime = verificationTime;
    }

    public Integer getRefundType() {
        return refundType;
    }

    public void setRefundType(Integer refundType) {
        this.refundType = refundType;
    }

    public String getRefundId() {
        return refundId;
    }

    public void setRefundId(String refundId) {
        this.refundId = refundId;
    }

    public String getCouponId() {
        return couponId;
    }

    public void setCouponId(String couponId) {
        this.couponId = couponId;
    }

    public String getRefundApplyTime() {
        return refundApplyTime;
    }

    public void setRefundApplyTime(String refundApplyTime) {
        this.refundApplyTime = refundApplyTime;
    }

    public String getRefundAuditTime() {
        return refundAuditTime;
    }

    public void setRefundAuditTime(String refundAuditTime) {
        this.refundAuditTime = refundAuditTime;
    }

    public Integer getRefundAuditResult() {
        return refundAuditResult;
    }

    public void setRefundAuditResult(Integer refundAuditResult) {
        this.refundAuditResult = refundAuditResult;
    }

    public String getFkPurchaseTime() {
        return fkPurchaseTime;
    }

    public void setFkPurchaseTime(String fkPurchaseTime) {
        this.fkPurchaseTime = fkPurchaseTime;
    }

    public String getFkPurchaseDate() {
        return fkPurchaseDate;
    }

    public void setFkPurchaseDate(String fkPurchaseDate) {
        this.fkPurchaseDate = fkPurchaseDate;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
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

    public Integer getPurchaseCount() {
        return purchaseCount;
    }

    public void setPurchaseCount(Integer purchaseCount) {
        this.purchaseCount = purchaseCount;
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
        return "ZsfTiktokVchrPurFactModel{" +
                "pkId='" + pkId + '\'' +
                ", akOrderId='" + akOrderId + '\'' +
                ", issuedCount=" + issuedCount +
                ", startTime='" + startTime + '\'' +
                ", expireTime='" + expireTime + '\'' +
                ", douyinGoodsName='" + douyinGoodsName + '\'' +
                ", douyinGoodsId='" + douyinGoodsId + '\'' +
                ", cardCode='" + cardCode + '\'' +
                ", couponCode='" + couponCode + '\'' +
                ", purchaserMobileNumber='" + purchaserMobileNumber + '\'' +
                ", couponStatus='" + couponStatus + '\'' +
                ", verificationTime='" + verificationTime + '\'' +
                ", refundType=" + refundType +
                ", refundId='" + refundId + '\'' +
                ", couponId='" + couponId + '\'' +
                ", refundApplyTime='" + refundApplyTime + '\'' +
                ", refundAuditTime='" + refundAuditTime + '\'' +
                ", refundAuditResult=" + refundAuditResult +
                ", fkPurchaseTime='" + fkPurchaseTime + '\'' +
                ", fkPurchaseDate='" + fkPurchaseDate + '\'' +
                ", updateTime='" + updateTime + '\'' +
                ", fkBookingUserTid='" + fkBookingUserTid + '\'' +
                ", fkBookingUserOriginId='" + fkBookingUserOriginId + '\'' +
                ", purchaseCount=" + purchaseCount +
                ", sourceLastUpdatetime='" + sourceLastUpdatetime + '\'' +
                ", systemCreatetime='" + systemCreatetime + '\'' +
                ", systemLastUpdatetime='" + systemLastUpdatetime + '\'' +
                '}';
    }
}
