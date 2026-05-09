package com.travelsky.dataplatform.module.ods.trp;

import com.alibaba.excel.annotation.ExcelIgnore;
import com.alibaba.excel.annotation.ExcelProperty;

import java.math.BigDecimal;

/**
 * SC_优惠券退款报表明细 实体类
 * 用于读取和写入优惠券退款报表Excel文件
 */
public class TOdsScCouponRefundDetail {

    /**
     * 主键ID
     */
    @ExcelProperty("序号")
    private Long id;


    /**
     * 订单号
     */
    @ExcelProperty("订单号")
    private String orderNumber;

    /**
     * 银行订单号
     */
    @ExcelProperty("银行订单号")
    private String bankOrderNumber;

    /**
     * 银行名称
     */
    @ExcelProperty("银行名称")
    private String bankName;

    /**
     * 渠道
     */
    @ExcelProperty("渠道")
    private String channel;

    /**
     * 联系人姓名
     */
    @ExcelProperty("联系人姓名")
    private String contactName;

    /**
     * 联系人手机号码
     */
    @ExcelProperty("联系人手机号码")
    private String contactPhoneNumber;

    /**
     * 订单日期
     */
    @ExcelProperty("订单日期")
    private String orderDate;

    /**
     * 订单状态
     */
    @ExcelProperty("订单状态")
    private String orderStatus;

    /**
     * 券码
     */
    @ExcelProperty("券码")
    private String voucherCode;

    /**
     * 券名称
     */
    @ExcelProperty("券名称")
    private String voucherName;

    /**
     * 券面额
     */
    @ExcelProperty("券面额")
    private BigDecimal voucherAmount;

    /**
     * 支付金额
     */
    @ExcelProperty("支付金额")
    private BigDecimal paymentAmount;

    /**
     * 退款金额
     */
    @ExcelProperty("退款金额")
    private BigDecimal refundAmount;

    /**
     * 退款日期
     */
    @ExcelProperty("退款日期")
    private String refundDate;

    /**
     * 退款状态
     */
    @ExcelProperty("退款状态")
    private String refundStatus;

    /**
     * 用户名
     */
    @ExcelProperty("用户名")
    private String username;


    /**
     * 数据入仓时间
     */
    @ExcelIgnore
    private String etlCreateTime;

    /**
     * 数据在数仓更新时间
     */
    @ExcelIgnore
    private String etlUpdateTime;

    /**
     * 数据ETL日期
     */
    @ExcelIgnore
    private String etlDate;

    public TOdsScCouponRefundDetail() {
    }

    public Long getId() {
        return this.id;
    }

    public String getOrderNumber() {
        return this.orderNumber;
    }

    public String getBankOrderNumber() {
        return this.bankOrderNumber;
    }

    public String getBankName() {
        return this.bankName;
    }

    public String getChannel() {
        return this.channel;
    }

    public String getContactName() {
        return this.contactName;
    }

    public String getContactPhoneNumber() {
        return this.contactPhoneNumber;
    }

    public String getOrderDate() {
        return this.orderDate;
    }

    public String getOrderStatus() {
        return this.orderStatus;
    }

    public String getVoucherCode() {
        return this.voucherCode;
    }

    public String getVoucherName() {
        return this.voucherName;
    }

    public BigDecimal getVoucherAmount() {
        return this.voucherAmount;
    }

    public BigDecimal getPaymentAmount() {
        return this.paymentAmount;
    }

    public BigDecimal getRefundAmount() {
        return this.refundAmount;
    }

    public String getRefundDate() {
        return this.refundDate;
    }

    public String getRefundStatus() {
        return this.refundStatus;
    }

    public String getUsername() {
        return this.username;
    }

    public String getEtlCreateTime() {
        return this.etlCreateTime;
    }

    public String getEtlUpdateTime() {
        return this.etlUpdateTime;
    }

    public String getEtlDate() {
        return this.etlDate;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public void setOrderNumber(String orderNumber) {
        this.orderNumber = orderNumber;
    }

    public void setBankOrderNumber(String bankOrderNumber) {
        this.bankOrderNumber = bankOrderNumber;
    }

    public void setBankName(String bankName) {
        this.bankName = bankName;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public void setContactName(String contactName) {
        this.contactName = contactName;
    }

    public void setContactPhoneNumber(String contactPhoneNumber) {
        this.contactPhoneNumber = contactPhoneNumber;
    }

    public void setOrderDate(String orderDate) {
        this.orderDate = orderDate;
    }

    public void setOrderStatus(String orderStatus) {
        this.orderStatus = orderStatus;
    }

    public void setVoucherCode(String voucherCode) {
        this.voucherCode = voucherCode;
    }

    public void setVoucherName(String voucherName) {
        this.voucherName = voucherName;
    }

    public void setVoucherAmount(BigDecimal voucherAmount) {
        this.voucherAmount = voucherAmount;
    }

    public void setPaymentAmount(BigDecimal paymentAmount) {
        this.paymentAmount = paymentAmount;
    }

    public void setRefundAmount(BigDecimal refundAmount) {
        this.refundAmount = refundAmount;
    }

    public void setRefundDate(String refundDate) {
        this.refundDate = refundDate;
    }

    public void setRefundStatus(String refundStatus) {
        this.refundStatus = refundStatus;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setEtlCreateTime(String etlCreateTime) {
        this.etlCreateTime = etlCreateTime;
    }

    public void setEtlUpdateTime(String etlUpdateTime) {
        this.etlUpdateTime = etlUpdateTime;
    }

    public void setEtlDate(String etlDate) {
        this.etlDate = etlDate;
    }

    public boolean equals(final Object o) {
        if (o == this) return true;
        if (!(o instanceof TOdsScCouponRefundDetail)) return false;
        final TOdsScCouponRefundDetail other = (TOdsScCouponRefundDetail) o;
        if (!other.canEqual((Object) this)) return false;
        final Object this$id = this.getId();
        final Object other$id = other.getId();
        if (this$id == null ? other$id != null : !this$id.equals(other$id)) return false;
        final Object this$orderNumber = this.getOrderNumber();
        final Object other$orderNumber = other.getOrderNumber();
        if (this$orderNumber == null ? other$orderNumber != null : !this$orderNumber.equals(other$orderNumber))
            return false;
        final Object this$bankOrderNumber = this.getBankOrderNumber();
        final Object other$bankOrderNumber = other.getBankOrderNumber();
        if (this$bankOrderNumber == null ? other$bankOrderNumber != null : !this$bankOrderNumber.equals(other$bankOrderNumber))
            return false;
        final Object this$bankName = this.getBankName();
        final Object other$bankName = other.getBankName();
        if (this$bankName == null ? other$bankName != null : !this$bankName.equals(other$bankName)) return false;
        final Object this$channel = this.getChannel();
        final Object other$channel = other.getChannel();
        if (this$channel == null ? other$channel != null : !this$channel.equals(other$channel)) return false;
        final Object this$contactName = this.getContactName();
        final Object other$contactName = other.getContactName();
        if (this$contactName == null ? other$contactName != null : !this$contactName.equals(other$contactName))
            return false;
        final Object this$contactPhoneNumber = this.getContactPhoneNumber();
        final Object other$contactPhoneNumber = other.getContactPhoneNumber();
        if (this$contactPhoneNumber == null ? other$contactPhoneNumber != null : !this$contactPhoneNumber.equals(other$contactPhoneNumber))
            return false;
        final Object this$orderDate = this.getOrderDate();
        final Object other$orderDate = other.getOrderDate();
        if (this$orderDate == null ? other$orderDate != null : !this$orderDate.equals(other$orderDate)) return false;
        final Object this$orderStatus = this.getOrderStatus();
        final Object other$orderStatus = other.getOrderStatus();
        if (this$orderStatus == null ? other$orderStatus != null : !this$orderStatus.equals(other$orderStatus))
            return false;
        final Object this$voucherCode = this.getVoucherCode();
        final Object other$voucherCode = other.getVoucherCode();
        if (this$voucherCode == null ? other$voucherCode != null : !this$voucherCode.equals(other$voucherCode))
            return false;
        final Object this$voucherName = this.getVoucherName();
        final Object other$voucherName = other.getVoucherName();
        if (this$voucherName == null ? other$voucherName != null : !this$voucherName.equals(other$voucherName))
            return false;
        final Object this$voucherAmount = this.getVoucherAmount();
        final Object other$voucherAmount = other.getVoucherAmount();
        if (this$voucherAmount == null ? other$voucherAmount != null : !this$voucherAmount.equals(other$voucherAmount))
            return false;
        final Object this$paymentAmount = this.getPaymentAmount();
        final Object other$paymentAmount = other.getPaymentAmount();
        if (this$paymentAmount == null ? other$paymentAmount != null : !this$paymentAmount.equals(other$paymentAmount))
            return false;
        final Object this$refundAmount = this.getRefundAmount();
        final Object other$refundAmount = other.getRefundAmount();
        if (this$refundAmount == null ? other$refundAmount != null : !this$refundAmount.equals(other$refundAmount))
            return false;
        final Object this$refundDate = this.getRefundDate();
        final Object other$refundDate = other.getRefundDate();
        if (this$refundDate == null ? other$refundDate != null : !this$refundDate.equals(other$refundDate))
            return false;
        final Object this$refundStatus = this.getRefundStatus();
        final Object other$refundStatus = other.getRefundStatus();
        if (this$refundStatus == null ? other$refundStatus != null : !this$refundStatus.equals(other$refundStatus))
            return false;
        final Object this$username = this.getUsername();
        final Object other$username = other.getUsername();
        if (this$username == null ? other$username != null : !this$username.equals(other$username)) return false;
        final Object this$etlCreateTime = this.getEtlCreateTime();
        final Object other$etlCreateTime = other.getEtlCreateTime();
        if (this$etlCreateTime == null ? other$etlCreateTime != null : !this$etlCreateTime.equals(other$etlCreateTime))
            return false;
        final Object this$etlUpdateTime = this.getEtlUpdateTime();
        final Object other$etlUpdateTime = other.getEtlUpdateTime();
        if (this$etlUpdateTime == null ? other$etlUpdateTime != null : !this$etlUpdateTime.equals(other$etlUpdateTime))
            return false;
        final Object this$etlDate = this.getEtlDate();
        final Object other$etlDate = other.getEtlDate();
        if (this$etlDate == null ? other$etlDate != null : !this$etlDate.equals(other$etlDate)) return false;
        return true;
    }

    protected boolean canEqual(final Object other) {
        return other instanceof TOdsScCouponRefundDetail;
    }

    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        final Object $id = this.getId();
        result = result * PRIME + ($id == null ? 43 : $id.hashCode());
        final Object $orderNumber = this.getOrderNumber();
        result = result * PRIME + ($orderNumber == null ? 43 : $orderNumber.hashCode());
        final Object $bankOrderNumber = this.getBankOrderNumber();
        result = result * PRIME + ($bankOrderNumber == null ? 43 : $bankOrderNumber.hashCode());
        final Object $bankName = this.getBankName();
        result = result * PRIME + ($bankName == null ? 43 : $bankName.hashCode());
        final Object $channel = this.getChannel();
        result = result * PRIME + ($channel == null ? 43 : $channel.hashCode());
        final Object $contactName = this.getContactName();
        result = result * PRIME + ($contactName == null ? 43 : $contactName.hashCode());
        final Object $contactPhoneNumber = this.getContactPhoneNumber();
        result = result * PRIME + ($contactPhoneNumber == null ? 43 : $contactPhoneNumber.hashCode());
        final Object $orderDate = this.getOrderDate();
        result = result * PRIME + ($orderDate == null ? 43 : $orderDate.hashCode());
        final Object $orderStatus = this.getOrderStatus();
        result = result * PRIME + ($orderStatus == null ? 43 : $orderStatus.hashCode());
        final Object $voucherCode = this.getVoucherCode();
        result = result * PRIME + ($voucherCode == null ? 43 : $voucherCode.hashCode());
        final Object $voucherName = this.getVoucherName();
        result = result * PRIME + ($voucherName == null ? 43 : $voucherName.hashCode());
        final Object $voucherAmount = this.getVoucherAmount();
        result = result * PRIME + ($voucherAmount == null ? 43 : $voucherAmount.hashCode());
        final Object $paymentAmount = this.getPaymentAmount();
        result = result * PRIME + ($paymentAmount == null ? 43 : $paymentAmount.hashCode());
        final Object $refundAmount = this.getRefundAmount();
        result = result * PRIME + ($refundAmount == null ? 43 : $refundAmount.hashCode());
        final Object $refundDate = this.getRefundDate();
        result = result * PRIME + ($refundDate == null ? 43 : $refundDate.hashCode());
        final Object $refundStatus = this.getRefundStatus();
        result = result * PRIME + ($refundStatus == null ? 43 : $refundStatus.hashCode());
        final Object $username = this.getUsername();
        result = result * PRIME + ($username == null ? 43 : $username.hashCode());
        final Object $etlCreateTime = this.getEtlCreateTime();
        result = result * PRIME + ($etlCreateTime == null ? 43 : $etlCreateTime.hashCode());
        final Object $etlUpdateTime = this.getEtlUpdateTime();
        result = result * PRIME + ($etlUpdateTime == null ? 43 : $etlUpdateTime.hashCode());
        final Object $etlDate = this.getEtlDate();
        result = result * PRIME + ($etlDate == null ? 43 : $etlDate.hashCode());
        return result;
    }

    public String toString() {
        return "TOdsScCouponRefundDetail(id=" + this.getId() + ", orderNumber=" + this.getOrderNumber() + ", bankOrderNumber=" + this.getBankOrderNumber() + ", bankName=" + this.getBankName() + ", channel=" + this.getChannel() + ", contactName=" + this.getContactName() + ", contactPhoneNumber=" + this.getContactPhoneNumber() + ", orderDate=" + this.getOrderDate() + ", orderStatus=" + this.getOrderStatus() + ", voucherCode=" + this.getVoucherCode() + ", voucherName=" + this.getVoucherName() + ", voucherAmount=" + this.getVoucherAmount() + ", paymentAmount=" + this.getPaymentAmount() + ", refundAmount=" + this.getRefundAmount() + ", refundDate=" + this.getRefundDate() + ", refundStatus=" + this.getRefundStatus() + ", username=" + this.getUsername() + ", etlCreateTime=" + this.getEtlCreateTime() + ", etlUpdateTime=" + this.getEtlUpdateTime() + ", etlDate=" + this.getEtlDate() + ")";
    }
}