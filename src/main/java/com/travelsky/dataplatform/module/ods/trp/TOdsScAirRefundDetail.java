package com.travelsky.dataplatform.module.ods.trp;

import com.alibaba.excel.annotation.ExcelIgnore;
import com.alibaba.excel.annotation.ExcelProperty;

import java.math.BigDecimal;

/**
 * SC_机票退款报表明细 实体类
 * 用于读取和写入机票退款报表Excel文件
 */
public class TOdsScAirRefundDetail {

    /**
     * 主键ID
     */
    @ExcelProperty("序号")
    private Long id;


    /**
     * 订单日期
     */
    @ExcelProperty("订单日期")
    private String orderDate;

    /**
     * 订单号
     */
    @ExcelProperty("订单号")
    private String orderNumber;

    /**
     * PNR
     */
    @ExcelProperty("PNR")
    private String pnr;

    /**
     * 客票性质
     */
    @ExcelProperty("客票性质")
    private String ticketNature;

    /**
     * 票号
     */
    @ExcelProperty("票号")
    private String ticketNumber;

    /**
     * 旅客姓名
     */
    @ExcelProperty("旅客姓名")
    private String passengerName;

    /**
     * 航段序号
     */
    @ExcelProperty("航段序号")
    private String segmentSequenceNumber;

    /**
     * 航班号
     */
    @ExcelProperty("航班号")
    private String flightNumber;

    /**
     * 支付银行
     */
    @ExcelProperty("支付银行")
    private String paymentBank;

    /**
     * 银行订单号
     */
    @ExcelProperty("银行订单号")
    private String bankOrderNumber;

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
     * 退票金额
     */
    @ExcelProperty("退票金额")
    private BigDecimal ticketRefundAmount;

    /**
     * 退票费率
     */
    @ExcelProperty("退票费率")
    private BigDecimal ticketRefundRate;

    /**
     * 退票手续费
     */
    @ExcelProperty("退票手续费")
    private BigDecimal ticketRefundFee;

    /**
     * 基建退费
     */
    @ExcelProperty("基建退费")
    private BigDecimal airportConstructionRefund;

    /**
     * 燃油退费
     */
    @ExcelProperty("燃油退费")
    private BigDecimal fuelSurchargeRefund;

    /**
     * 其他税费退费
     */
    @ExcelProperty("其他税费退费")
    private BigDecimal otherTaxRefund;

    /**
     * 退税总计
     */
    @ExcelProperty("退税总计")
    private BigDecimal totalTaxRefund;

    /**
     * 退税手续费总计
     */
    @ExcelProperty("退税手续费总计")
    private BigDecimal totalTaxRefundFee;

    /**
     * 保险退款金额
     */
    @ExcelProperty("保险退款金额")
    private BigDecimal insuranceRefundAmount;

    /**
     * 一审执行人
     */
    @ExcelProperty("一审执行人")
    private String firstReviewExecutor;

    /**
     * 一审执行时间
     */
    @ExcelProperty("一审执行时间")
    private String firstReviewTime;

    /**
     * 一审审核意见
     */
    @ExcelProperty("一审审核意见")
    private String firstReviewOpinion;

    /**
     * 二审执行人
     */
    @ExcelProperty("二审执行人")
    private String secondReviewExecutor;

    /**
     * 二审执行时间
     */
    @ExcelProperty("二审执行时间")
    private String secondReviewTime;

    /**
     * 二审审核意见
     */
    @ExcelProperty("二审审核意见")
    private String secondReviewOpinion;

    /**
     * 退款执行人
     */
    @ExcelProperty("退款执行人")
    private String refundExecutor;

    /**
     * 退票完成时间
     */
    @ExcelProperty("退票完成时间")
    private String ticketRefundCompletionTime;

    /**
     * 退款审核意见
     */
    @ExcelProperty("退款审核意见")
    private String refundReviewOpinion;

    /**
     * 支付时间
     */
    @ExcelProperty("支付时间")
    private String paymentTime;

    /**
     * 退票性质
     */
    @ExcelProperty("退票性质")
    private String ticketRefundNature;

    /**
     * 退票申请人
     */
    @ExcelProperty("退票申请人")
    private String ticketRefundApplicant;

    /**
     * 退票申请时间
     */
    @ExcelProperty("退票申请时间")
    private String ticketRefundApplicationTime;

    /**
     * PNR取消时间
     */
    @ExcelProperty("PNR取消时间")
    private String pnrCancellationTime;

    /**
     * 退票原因
     */
    @ExcelProperty("退票原因")
    private String ticketRefundReason;

    /**
     * 币种
     */
    @ExcelProperty("币种")
    private String currency;

    /**
     * 结算号
     */
    @ExcelProperty("结算号")
    private String settlementNumber;

    /**
     * 退票单号
     */
    @ExcelProperty("退票单号")
    private String ticketRefundNumber;

    /**
     * 退票状态
     */
    @ExcelProperty("退票状态")
    private String ticketRefundStatus;

    /**
     * 渠道
     */
    @ExcelProperty("渠道")
    private String channel;

    /**
     * 站点
     */
    @ExcelProperty("站点")
    private String site;

    /**
     * 票面REFUND时间
     */
    @ExcelProperty("票面REFUND时间")
    private String ticketRefundTime;

    /**
     * 退款发起时间
     */
    @ExcelProperty("退款发起时间")
    private String refundInitiationTime;

    /**
     * 客票类型
     */
    @ExcelProperty("客票类型")
    private String ticketType;

    /**
     * 退分状态
     */
    @ExcelProperty("退分状态")
    private String pointsRefundStatus;

    /**
     * 里程支付订单号
     */
    @ExcelProperty("里程支付订单号")
    private String mileagePaymentOrderNumber;

    /**
     * 支付里程数
     */
    @ExcelProperty("支付里程数")
    private BigDecimal paidMileage;

    /**
     * 退里程数
     */
    @ExcelProperty("退里程数")
    private BigDecimal refundedMileage;

    /**
     * 里程手续费
     */
    @ExcelProperty("里程手续费")
    private BigDecimal mileageFee;

    /**
     * 退分执行人
     */
    @ExcelProperty("退分执行人")
    private String pointsRefundExecutor;

    /**
     * 退分完成时间
     */
    @ExcelProperty("退分完成时间")
    private String pointsRefundCompletionTime;

    /**
     * 退分审核意见
     */
    @ExcelProperty("退分审核意见")
    private String pointsRefundReviewOpinion;

    /**
     * 退票申请渠道
     */
    @ExcelProperty("退票申请渠道")
    private String ticketRefundApplicationChannel;

    /**
     * 起飞日期
     */
    @ExcelProperty("起飞日期")
    private String departureDate;

    /**
     * 出发地三字码
     */
    @ExcelProperty("出发地三字码")
    private String departureCityCode;

    /**
     * 到达地三字码
     */
    @ExcelProperty("到达地三字码")
    private String arrivalCityCode;

    /**
     * 账户用户名
     */
    @ExcelProperty("账户用户名")
    private String accountUsername;

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

    public TOdsScAirRefundDetail() {
    }

    public Long getId() {
        return this.id;
    }

    public String getOrderDate() {
        return this.orderDate;
    }

    public String getOrderNumber() {
        return this.orderNumber;
    }

    public String getPnr() {
        return this.pnr;
    }

    public String getTicketNature() {
        return this.ticketNature;
    }

    public String getTicketNumber() {
        return this.ticketNumber;
    }

    public String getPassengerName() {
        return this.passengerName;
    }

    public String getSegmentSequenceNumber() {
        return this.segmentSequenceNumber;
    }

    public String getFlightNumber() {
        return this.flightNumber;
    }

    public String getPaymentBank() {
        return this.paymentBank;
    }

    public String getBankOrderNumber() {
        return this.bankOrderNumber;
    }

    public BigDecimal getPaymentAmount() {
        return this.paymentAmount;
    }

    public BigDecimal getRefundAmount() {
        return this.refundAmount;
    }

    public BigDecimal getTicketRefundAmount() {
        return this.ticketRefundAmount;
    }

    public BigDecimal getTicketRefundRate() {
        return this.ticketRefundRate;
    }

    public BigDecimal getTicketRefundFee() {
        return this.ticketRefundFee;
    }

    public BigDecimal getAirportConstructionRefund() {
        return this.airportConstructionRefund;
    }

    public BigDecimal getFuelSurchargeRefund() {
        return this.fuelSurchargeRefund;
    }

    public BigDecimal getOtherTaxRefund() {
        return this.otherTaxRefund;
    }

    public BigDecimal getTotalTaxRefund() {
        return this.totalTaxRefund;
    }

    public BigDecimal getTotalTaxRefundFee() {
        return this.totalTaxRefundFee;
    }

    public BigDecimal getInsuranceRefundAmount() {
        return this.insuranceRefundAmount;
    }

    public String getSecondReviewExecutor() {
        return this.secondReviewExecutor;
    }

    public String getSecondReviewTime() {
        return this.secondReviewTime;
    }

    public String getSecondReviewOpinion() {
        return this.secondReviewOpinion;
    }

    public String getRefundExecutor() {
        return this.refundExecutor;
    }

    public String getTicketRefundCompletionTime() {
        return this.ticketRefundCompletionTime;
    }

    public String getRefundReviewOpinion() {
        return this.refundReviewOpinion;
    }

    public String getPaymentTime() {
        return this.paymentTime;
    }

    public String getTicketRefundNature() {
        return this.ticketRefundNature;
    }

    public String getTicketRefundApplicant() {
        return this.ticketRefundApplicant;
    }

    public String getTicketRefundApplicationTime() {
        return this.ticketRefundApplicationTime;
    }

    public String getPnrCancellationTime() {
        return this.pnrCancellationTime;
    }

    public String getTicketRefundReason() {
        return this.ticketRefundReason;
    }

    public String getCurrency() {
        return this.currency;
    }

    public String getSettlementNumber() {
        return this.settlementNumber;
    }

    public String getTicketRefundNumber() {
        return this.ticketRefundNumber;
    }

    public String getTicketRefundStatus() {
        return this.ticketRefundStatus;
    }

    public String getChannel() {
        return this.channel;
    }

    public String getSite() {
        return this.site;
    }

    public String getTicketRefundTime() {
        return this.ticketRefundTime;
    }

    public String getRefundInitiationTime() {
        return this.refundInitiationTime;
    }

    public String getTicketType() {
        return this.ticketType;
    }

    public String getPointsRefundStatus() {
        return this.pointsRefundStatus;
    }

    public String getMileagePaymentOrderNumber() {
        return this.mileagePaymentOrderNumber;
    }

    public BigDecimal getPaidMileage() {
        return this.paidMileage;
    }

    public BigDecimal getRefundedMileage() {
        return this.refundedMileage;
    }

    public BigDecimal getMileageFee() {
        return this.mileageFee;
    }

    public String getPointsRefundExecutor() {
        return this.pointsRefundExecutor;
    }

    public String getPointsRefundCompletionTime() {
        return this.pointsRefundCompletionTime;
    }

    public String getPointsRefundReviewOpinion() {
        return this.pointsRefundReviewOpinion;
    }

    public String getTicketRefundApplicationChannel() {
        return this.ticketRefundApplicationChannel;
    }

    public String getDepartureDate() {
        return this.departureDate;
    }

    public String getDepartureCityCode() {
        return this.departureCityCode;
    }

    public String getArrivalCityCode() {
        return this.arrivalCityCode;
    }

    public String getAccountUsername() {
        return this.accountUsername;
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

    public void setOrderDate(String orderDate) {
        this.orderDate = orderDate;
    }

    public void setOrderNumber(String orderNumber) {
        this.orderNumber = orderNumber;
    }

    public void setPnr(String pnr) {
        this.pnr = pnr;
    }

    public void setTicketNature(String ticketNature) {
        this.ticketNature = ticketNature;
    }

    public void setTicketNumber(String ticketNumber) {
        this.ticketNumber = ticketNumber;
    }

    public void setPassengerName(String passengerName) {
        this.passengerName = passengerName;
    }

    public void setSegmentSequenceNumber(String segmentSequenceNumber) {
        this.segmentSequenceNumber = segmentSequenceNumber;
    }

    public void setFlightNumber(String flightNumber) {
        this.flightNumber = flightNumber;
    }

    public void setPaymentBank(String paymentBank) {
        this.paymentBank = paymentBank;
    }

    public void setBankOrderNumber(String bankOrderNumber) {
        this.bankOrderNumber = bankOrderNumber;
    }

    public void setPaymentAmount(BigDecimal paymentAmount) {
        this.paymentAmount = paymentAmount;
    }

    public void setRefundAmount(BigDecimal refundAmount) {
        this.refundAmount = refundAmount;
    }

    public void setTicketRefundAmount(BigDecimal ticketRefundAmount) {
        this.ticketRefundAmount = ticketRefundAmount;
    }

    public void setTicketRefundRate(BigDecimal ticketRefundRate) {
        this.ticketRefundRate = ticketRefundRate;
    }

    public void setTicketRefundFee(BigDecimal ticketRefundFee) {
        this.ticketRefundFee = ticketRefundFee;
    }

    public void setAirportConstructionRefund(BigDecimal airportConstructionRefund) {
        this.airportConstructionRefund = airportConstructionRefund;
    }

    public void setFuelSurchargeRefund(BigDecimal fuelSurchargeRefund) {
        this.fuelSurchargeRefund = fuelSurchargeRefund;
    }

    public void setOtherTaxRefund(BigDecimal otherTaxRefund) {
        this.otherTaxRefund = otherTaxRefund;
    }

    public void setTotalTaxRefund(BigDecimal totalTaxRefund) {
        this.totalTaxRefund = totalTaxRefund;
    }

    public void setTotalTaxRefundFee(BigDecimal totalTaxRefundFee) {
        this.totalTaxRefundFee = totalTaxRefundFee;
    }

    public void setInsuranceRefundAmount(BigDecimal insuranceRefundAmount) {
        this.insuranceRefundAmount = insuranceRefundAmount;
    }

    public void setSecondReviewExecutor(String secondReviewExecutor) {
        this.secondReviewExecutor = secondReviewExecutor;
    }

    public void setSecondReviewTime(String secondReviewTime) {
        this.secondReviewTime = secondReviewTime;
    }

    public void setSecondReviewOpinion(String secondReviewOpinion) {
        this.secondReviewOpinion = secondReviewOpinion;
    }

    public void setRefundExecutor(String refundExecutor) {
        this.refundExecutor = refundExecutor;
    }

    public void setTicketRefundCompletionTime(String ticketRefundCompletionTime) {
        this.ticketRefundCompletionTime = ticketRefundCompletionTime;
    }

    public void setRefundReviewOpinion(String refundReviewOpinion) {
        this.refundReviewOpinion = refundReviewOpinion;
    }

    public void setPaymentTime(String paymentTime) {
        this.paymentTime = paymentTime;
    }

    public void setTicketRefundNature(String ticketRefundNature) {
        this.ticketRefundNature = ticketRefundNature;
    }

    public void setTicketRefundApplicant(String ticketRefundApplicant) {
        this.ticketRefundApplicant = ticketRefundApplicant;
    }

    public void setTicketRefundApplicationTime(String ticketRefundApplicationTime) {
        this.ticketRefundApplicationTime = ticketRefundApplicationTime;
    }

    public void setPnrCancellationTime(String pnrCancellationTime) {
        this.pnrCancellationTime = pnrCancellationTime;
    }

    public void setTicketRefundReason(String ticketRefundReason) {
        this.ticketRefundReason = ticketRefundReason;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public void setSettlementNumber(String settlementNumber) {
        this.settlementNumber = settlementNumber;
    }

    public void setTicketRefundNumber(String ticketRefundNumber) {
        this.ticketRefundNumber = ticketRefundNumber;
    }

    public void setTicketRefundStatus(String ticketRefundStatus) {
        this.ticketRefundStatus = ticketRefundStatus;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public void setSite(String site) {
        this.site = site;
    }

    public void setTicketRefundTime(String ticketRefundTime) {
        this.ticketRefundTime = ticketRefundTime;
    }

    public void setRefundInitiationTime(String refundInitiationTime) {
        this.refundInitiationTime = refundInitiationTime;
    }

    public void setTicketType(String ticketType) {
        this.ticketType = ticketType;
    }

    public void setPointsRefundStatus(String pointsRefundStatus) {
        this.pointsRefundStatus = pointsRefundStatus;
    }

    public void setMileagePaymentOrderNumber(String mileagePaymentOrderNumber) {
        this.mileagePaymentOrderNumber = mileagePaymentOrderNumber;
    }

    public void setPaidMileage(BigDecimal paidMileage) {
        this.paidMileage = paidMileage;
    }

    public void setRefundedMileage(BigDecimal refundedMileage) {
        this.refundedMileage = refundedMileage;
    }

    public void setMileageFee(BigDecimal mileageFee) {
        this.mileageFee = mileageFee;
    }

    public void setPointsRefundExecutor(String pointsRefundExecutor) {
        this.pointsRefundExecutor = pointsRefundExecutor;
    }

    public void setPointsRefundCompletionTime(String pointsRefundCompletionTime) {
        this.pointsRefundCompletionTime = pointsRefundCompletionTime;
    }

    public void setPointsRefundReviewOpinion(String pointsRefundReviewOpinion) {
        this.pointsRefundReviewOpinion = pointsRefundReviewOpinion;
    }

    public void setTicketRefundApplicationChannel(String ticketRefundApplicationChannel) {
        this.ticketRefundApplicationChannel = ticketRefundApplicationChannel;
    }

    public void setDepartureDate(String departureDate) {
        this.departureDate = departureDate;
    }

    public void setDepartureCityCode(String departureCityCode) {
        this.departureCityCode = departureCityCode;
    }

    public void setArrivalCityCode(String arrivalCityCode) {
        this.arrivalCityCode = arrivalCityCode;
    }

    public void setAccountUsername(String accountUsername) {
        this.accountUsername = accountUsername;
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
        if (!(o instanceof TOdsScAirRefundDetail)) return false;
        final TOdsScAirRefundDetail other = (TOdsScAirRefundDetail) o;
        if (!other.canEqual((Object) this)) return false;
        final Object this$id = this.getId();
        final Object other$id = other.getId();
        if (this$id == null ? other$id != null : !this$id.equals(other$id)) return false;
        final Object this$orderDate = this.getOrderDate();
        final Object other$orderDate = other.getOrderDate();
        if (this$orderDate == null ? other$orderDate != null : !this$orderDate.equals(other$orderDate)) return false;
        final Object this$orderNumber = this.getOrderNumber();
        final Object other$orderNumber = other.getOrderNumber();
        if (this$orderNumber == null ? other$orderNumber != null : !this$orderNumber.equals(other$orderNumber))
            return false;
        final Object this$pnr = this.getPnr();
        final Object other$pnr = other.getPnr();
        if (this$pnr == null ? other$pnr != null : !this$pnr.equals(other$pnr)) return false;
        final Object this$ticketNature = this.getTicketNature();
        final Object other$ticketNature = other.getTicketNature();
        if (this$ticketNature == null ? other$ticketNature != null : !this$ticketNature.equals(other$ticketNature))
            return false;
        final Object this$ticketNumber = this.getTicketNumber();
        final Object other$ticketNumber = other.getTicketNumber();
        if (this$ticketNumber == null ? other$ticketNumber != null : !this$ticketNumber.equals(other$ticketNumber))
            return false;
        final Object this$passengerName = this.getPassengerName();
        final Object other$passengerName = other.getPassengerName();
        if (this$passengerName == null ? other$passengerName != null : !this$passengerName.equals(other$passengerName))
            return false;
        final Object this$segmentSequenceNumber = this.getSegmentSequenceNumber();
        final Object other$segmentSequenceNumber = other.getSegmentSequenceNumber();
        if (this$segmentSequenceNumber == null ? other$segmentSequenceNumber != null : !this$segmentSequenceNumber.equals(other$segmentSequenceNumber))
            return false;
        final Object this$flightNumber = this.getFlightNumber();
        final Object other$flightNumber = other.getFlightNumber();
        if (this$flightNumber == null ? other$flightNumber != null : !this$flightNumber.equals(other$flightNumber))
            return false;
        final Object this$paymentBank = this.getPaymentBank();
        final Object other$paymentBank = other.getPaymentBank();
        if (this$paymentBank == null ? other$paymentBank != null : !this$paymentBank.equals(other$paymentBank))
            return false;
        final Object this$bankOrderNumber = this.getBankOrderNumber();
        final Object other$bankOrderNumber = other.getBankOrderNumber();
        if (this$bankOrderNumber == null ? other$bankOrderNumber != null : !this$bankOrderNumber.equals(other$bankOrderNumber))
            return false;
        final Object this$paymentAmount = this.getPaymentAmount();
        final Object other$paymentAmount = other.getPaymentAmount();
        if (this$paymentAmount == null ? other$paymentAmount != null : !this$paymentAmount.equals(other$paymentAmount))
            return false;
        final Object this$refundAmount = this.getRefundAmount();
        final Object other$refundAmount = other.getRefundAmount();
        if (this$refundAmount == null ? other$refundAmount != null : !this$refundAmount.equals(other$refundAmount))
            return false;
        final Object this$ticketRefundAmount = this.getTicketRefundAmount();
        final Object other$ticketRefundAmount = other.getTicketRefundAmount();
        if (this$ticketRefundAmount == null ? other$ticketRefundAmount != null : !this$ticketRefundAmount.equals(other$ticketRefundAmount))
            return false;
        final Object this$ticketRefundRate = this.getTicketRefundRate();
        final Object other$ticketRefundRate = other.getTicketRefundRate();
        if (this$ticketRefundRate == null ? other$ticketRefundRate != null : !this$ticketRefundRate.equals(other$ticketRefundRate))
            return false;
        final Object this$ticketRefundFee = this.getTicketRefundFee();
        final Object other$ticketRefundFee = other.getTicketRefundFee();
        if (this$ticketRefundFee == null ? other$ticketRefundFee != null : !this$ticketRefundFee.equals(other$ticketRefundFee))
            return false;
        final Object this$airportConstructionRefund = this.getAirportConstructionRefund();
        final Object other$airportConstructionRefund = other.getAirportConstructionRefund();
        if (this$airportConstructionRefund == null ? other$airportConstructionRefund != null : !this$airportConstructionRefund.equals(other$airportConstructionRefund))
            return false;
        final Object this$fuelSurchargeRefund = this.getFuelSurchargeRefund();
        final Object other$fuelSurchargeRefund = other.getFuelSurchargeRefund();
        if (this$fuelSurchargeRefund == null ? other$fuelSurchargeRefund != null : !this$fuelSurchargeRefund.equals(other$fuelSurchargeRefund))
            return false;
        final Object this$otherTaxRefund = this.getOtherTaxRefund();
        final Object other$otherTaxRefund = other.getOtherTaxRefund();
        if (this$otherTaxRefund == null ? other$otherTaxRefund != null : !this$otherTaxRefund.equals(other$otherTaxRefund))
            return false;
        final Object this$totalTaxRefund = this.getTotalTaxRefund();
        final Object other$totalTaxRefund = other.getTotalTaxRefund();
        if (this$totalTaxRefund == null ? other$totalTaxRefund != null : !this$totalTaxRefund.equals(other$totalTaxRefund))
            return false;
        final Object this$totalTaxRefundFee = this.getTotalTaxRefundFee();
        final Object other$totalTaxRefundFee = other.getTotalTaxRefundFee();
        if (this$totalTaxRefundFee == null ? other$totalTaxRefundFee != null : !this$totalTaxRefundFee.equals(other$totalTaxRefundFee))
            return false;
        final Object this$insuranceRefundAmount = this.getInsuranceRefundAmount();
        final Object other$insuranceRefundAmount = other.getInsuranceRefundAmount();
        if (this$insuranceRefundAmount == null ? other$insuranceRefundAmount != null : !this$insuranceRefundAmount.equals(other$insuranceRefundAmount))
            return false;
        final Object this$secondReviewExecutor = this.getSecondReviewExecutor();
        final Object other$secondReviewExecutor = other.getSecondReviewExecutor();
        if (this$secondReviewExecutor == null ? other$secondReviewExecutor != null : !this$secondReviewExecutor.equals(other$secondReviewExecutor))
            return false;
        final Object this$secondReviewTime = this.getSecondReviewTime();
        final Object other$secondReviewTime = other.getSecondReviewTime();
        if (this$secondReviewTime == null ? other$secondReviewTime != null : !this$secondReviewTime.equals(other$secondReviewTime))
            return false;
        final Object this$secondReviewOpinion = this.getSecondReviewOpinion();
        final Object other$secondReviewOpinion = other.getSecondReviewOpinion();
        if (this$secondReviewOpinion == null ? other$secondReviewOpinion != null : !this$secondReviewOpinion.equals(other$secondReviewOpinion))
            return false;
        final Object this$refundExecutor = this.getRefundExecutor();
        final Object other$refundExecutor = other.getRefundExecutor();
        if (this$refundExecutor == null ? other$refundExecutor != null : !this$refundExecutor.equals(other$refundExecutor))
            return false;
        final Object this$ticketRefundCompletionTime = this.getTicketRefundCompletionTime();
        final Object other$ticketRefundCompletionTime = other.getTicketRefundCompletionTime();
        if (this$ticketRefundCompletionTime == null ? other$ticketRefundCompletionTime != null : !this$ticketRefundCompletionTime.equals(other$ticketRefundCompletionTime))
            return false;
        final Object this$refundReviewOpinion = this.getRefundReviewOpinion();
        final Object other$refundReviewOpinion = other.getRefundReviewOpinion();
        if (this$refundReviewOpinion == null ? other$refundReviewOpinion != null : !this$refundReviewOpinion.equals(other$refundReviewOpinion))
            return false;
        final Object this$paymentTime = this.getPaymentTime();
        final Object other$paymentTime = other.getPaymentTime();
        if (this$paymentTime == null ? other$paymentTime != null : !this$paymentTime.equals(other$paymentTime))
            return false;
        final Object this$ticketRefundNature = this.getTicketRefundNature();
        final Object other$ticketRefundNature = other.getTicketRefundNature();
        if (this$ticketRefundNature == null ? other$ticketRefundNature != null : !this$ticketRefundNature.equals(other$ticketRefundNature))
            return false;
        final Object this$ticketRefundApplicant = this.getTicketRefundApplicant();
        final Object other$ticketRefundApplicant = other.getTicketRefundApplicant();
        if (this$ticketRefundApplicant == null ? other$ticketRefundApplicant != null : !this$ticketRefundApplicant.equals(other$ticketRefundApplicant))
            return false;
        final Object this$ticketRefundApplicationTime = this.getTicketRefundApplicationTime();
        final Object other$ticketRefundApplicationTime = other.getTicketRefundApplicationTime();
        if (this$ticketRefundApplicationTime == null ? other$ticketRefundApplicationTime != null : !this$ticketRefundApplicationTime.equals(other$ticketRefundApplicationTime))
            return false;
        final Object this$pnrCancellationTime = this.getPnrCancellationTime();
        final Object other$pnrCancellationTime = other.getPnrCancellationTime();
        if (this$pnrCancellationTime == null ? other$pnrCancellationTime != null : !this$pnrCancellationTime.equals(other$pnrCancellationTime))
            return false;
        final Object this$ticketRefundReason = this.getTicketRefundReason();
        final Object other$ticketRefundReason = other.getTicketRefundReason();
        if (this$ticketRefundReason == null ? other$ticketRefundReason != null : !this$ticketRefundReason.equals(other$ticketRefundReason))
            return false;
        final Object this$currency = this.getCurrency();
        final Object other$currency = other.getCurrency();
        if (this$currency == null ? other$currency != null : !this$currency.equals(other$currency)) return false;
        final Object this$settlementNumber = this.getSettlementNumber();
        final Object other$settlementNumber = other.getSettlementNumber();
        if (this$settlementNumber == null ? other$settlementNumber != null : !this$settlementNumber.equals(other$settlementNumber))
            return false;
        final Object this$ticketRefundNumber = this.getTicketRefundNumber();
        final Object other$ticketRefundNumber = other.getTicketRefundNumber();
        if (this$ticketRefundNumber == null ? other$ticketRefundNumber != null : !this$ticketRefundNumber.equals(other$ticketRefundNumber))
            return false;
        final Object this$ticketRefundStatus = this.getTicketRefundStatus();
        final Object other$ticketRefundStatus = other.getTicketRefundStatus();
        if (this$ticketRefundStatus == null ? other$ticketRefundStatus != null : !this$ticketRefundStatus.equals(other$ticketRefundStatus))
            return false;
        final Object this$channel = this.getChannel();
        final Object other$channel = other.getChannel();
        if (this$channel == null ? other$channel != null : !this$channel.equals(other$channel)) return false;
        final Object this$site = this.getSite();
        final Object other$site = other.getSite();
        if (this$site == null ? other$site != null : !this$site.equals(other$site)) return false;
        final Object this$ticketRefundTime = this.getTicketRefundTime();
        final Object other$ticketRefundTime = other.getTicketRefundTime();
        if (this$ticketRefundTime == null ? other$ticketRefundTime != null : !this$ticketRefundTime.equals(other$ticketRefundTime))
            return false;
        final Object this$refundInitiationTime = this.getRefundInitiationTime();
        final Object other$refundInitiationTime = other.getRefundInitiationTime();
        if (this$refundInitiationTime == null ? other$refundInitiationTime != null : !this$refundInitiationTime.equals(other$refundInitiationTime))
            return false;
        final Object this$ticketType = this.getTicketType();
        final Object other$ticketType = other.getTicketType();
        if (this$ticketType == null ? other$ticketType != null : !this$ticketType.equals(other$ticketType))
            return false;
        final Object this$pointsRefundStatus = this.getPointsRefundStatus();
        final Object other$pointsRefundStatus = other.getPointsRefundStatus();
        if (this$pointsRefundStatus == null ? other$pointsRefundStatus != null : !this$pointsRefundStatus.equals(other$pointsRefundStatus))
            return false;
        final Object this$mileagePaymentOrderNumber = this.getMileagePaymentOrderNumber();
        final Object other$mileagePaymentOrderNumber = other.getMileagePaymentOrderNumber();
        if (this$mileagePaymentOrderNumber == null ? other$mileagePaymentOrderNumber != null : !this$mileagePaymentOrderNumber.equals(other$mileagePaymentOrderNumber))
            return false;
        final Object this$paidMileage = this.getPaidMileage();
        final Object other$paidMileage = other.getPaidMileage();
        if (this$paidMileage == null ? other$paidMileage != null : !this$paidMileage.equals(other$paidMileage))
            return false;
        final Object this$refundedMileage = this.getRefundedMileage();
        final Object other$refundedMileage = other.getRefundedMileage();
        if (this$refundedMileage == null ? other$refundedMileage != null : !this$refundedMileage.equals(other$refundedMileage))
            return false;
        final Object this$mileageFee = this.getMileageFee();
        final Object other$mileageFee = other.getMileageFee();
        if (this$mileageFee == null ? other$mileageFee != null : !this$mileageFee.equals(other$mileageFee))
            return false;
        final Object this$pointsRefundExecutor = this.getPointsRefundExecutor();
        final Object other$pointsRefundExecutor = other.getPointsRefundExecutor();
        if (this$pointsRefundExecutor == null ? other$pointsRefundExecutor != null : !this$pointsRefundExecutor.equals(other$pointsRefundExecutor))
            return false;
        final Object this$pointsRefundCompletionTime = this.getPointsRefundCompletionTime();
        final Object other$pointsRefundCompletionTime = other.getPointsRefundCompletionTime();
        if (this$pointsRefundCompletionTime == null ? other$pointsRefundCompletionTime != null : !this$pointsRefundCompletionTime.equals(other$pointsRefundCompletionTime))
            return false;
        final Object this$pointsRefundReviewOpinion = this.getPointsRefundReviewOpinion();
        final Object other$pointsRefundReviewOpinion = other.getPointsRefundReviewOpinion();
        if (this$pointsRefundReviewOpinion == null ? other$pointsRefundReviewOpinion != null : !this$pointsRefundReviewOpinion.equals(other$pointsRefundReviewOpinion))
            return false;
        final Object this$ticketRefundApplicationChannel = this.getTicketRefundApplicationChannel();
        final Object other$ticketRefundApplicationChannel = other.getTicketRefundApplicationChannel();
        if (this$ticketRefundApplicationChannel == null ? other$ticketRefundApplicationChannel != null : !this$ticketRefundApplicationChannel.equals(other$ticketRefundApplicationChannel))
            return false;
        final Object this$departureDate = this.getDepartureDate();
        final Object other$departureDate = other.getDepartureDate();
        if (this$departureDate == null ? other$departureDate != null : !this$departureDate.equals(other$departureDate))
            return false;
        final Object this$departureCityCode = this.getDepartureCityCode();
        final Object other$departureCityCode = other.getDepartureCityCode();
        if (this$departureCityCode == null ? other$departureCityCode != null : !this$departureCityCode.equals(other$departureCityCode))
            return false;
        final Object this$arrivalCityCode = this.getArrivalCityCode();
        final Object other$arrivalCityCode = other.getArrivalCityCode();
        if (this$arrivalCityCode == null ? other$arrivalCityCode != null : !this$arrivalCityCode.equals(other$arrivalCityCode))
            return false;
        final Object this$accountUsername = this.getAccountUsername();
        final Object other$accountUsername = other.getAccountUsername();
        if (this$accountUsername == null ? other$accountUsername != null : !this$accountUsername.equals(other$accountUsername))
            return false;
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
        return other instanceof TOdsScAirRefundDetail;
    }

    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        final Object $id = this.getId();
        result = result * PRIME + ($id == null ? 43 : $id.hashCode());
        final Object $orderDate = this.getOrderDate();
        result = result * PRIME + ($orderDate == null ? 43 : $orderDate.hashCode());
        final Object $orderNumber = this.getOrderNumber();
        result = result * PRIME + ($orderNumber == null ? 43 : $orderNumber.hashCode());
        final Object $pnr = this.getPnr();
        result = result * PRIME + ($pnr == null ? 43 : $pnr.hashCode());
        final Object $ticketNature = this.getTicketNature();
        result = result * PRIME + ($ticketNature == null ? 43 : $ticketNature.hashCode());
        final Object $ticketNumber = this.getTicketNumber();
        result = result * PRIME + ($ticketNumber == null ? 43 : $ticketNumber.hashCode());
        final Object $passengerName = this.getPassengerName();
        result = result * PRIME + ($passengerName == null ? 43 : $passengerName.hashCode());
        final Object $segmentSequenceNumber = this.getSegmentSequenceNumber();
        result = result * PRIME + ($segmentSequenceNumber == null ? 43 : $segmentSequenceNumber.hashCode());
        final Object $flightNumber = this.getFlightNumber();
        result = result * PRIME + ($flightNumber == null ? 43 : $flightNumber.hashCode());
        final Object $paymentBank = this.getPaymentBank();
        result = result * PRIME + ($paymentBank == null ? 43 : $paymentBank.hashCode());
        final Object $bankOrderNumber = this.getBankOrderNumber();
        result = result * PRIME + ($bankOrderNumber == null ? 43 : $bankOrderNumber.hashCode());
        final Object $paymentAmount = this.getPaymentAmount();
        result = result * PRIME + ($paymentAmount == null ? 43 : $paymentAmount.hashCode());
        final Object $refundAmount = this.getRefundAmount();
        result = result * PRIME + ($refundAmount == null ? 43 : $refundAmount.hashCode());
        final Object $ticketRefundAmount = this.getTicketRefundAmount();
        result = result * PRIME + ($ticketRefundAmount == null ? 43 : $ticketRefundAmount.hashCode());
        final Object $ticketRefundRate = this.getTicketRefundRate();
        result = result * PRIME + ($ticketRefundRate == null ? 43 : $ticketRefundRate.hashCode());
        final Object $ticketRefundFee = this.getTicketRefundFee();
        result = result * PRIME + ($ticketRefundFee == null ? 43 : $ticketRefundFee.hashCode());
        final Object $airportConstructionRefund = this.getAirportConstructionRefund();
        result = result * PRIME + ($airportConstructionRefund == null ? 43 : $airportConstructionRefund.hashCode());
        final Object $fuelSurchargeRefund = this.getFuelSurchargeRefund();
        result = result * PRIME + ($fuelSurchargeRefund == null ? 43 : $fuelSurchargeRefund.hashCode());
        final Object $otherTaxRefund = this.getOtherTaxRefund();
        result = result * PRIME + ($otherTaxRefund == null ? 43 : $otherTaxRefund.hashCode());
        final Object $totalTaxRefund = this.getTotalTaxRefund();
        result = result * PRIME + ($totalTaxRefund == null ? 43 : $totalTaxRefund.hashCode());
        final Object $totalTaxRefundFee = this.getTotalTaxRefundFee();
        result = result * PRIME + ($totalTaxRefundFee == null ? 43 : $totalTaxRefundFee.hashCode());
        final Object $insuranceRefundAmount = this.getInsuranceRefundAmount();
        result = result * PRIME + ($insuranceRefundAmount == null ? 43 : $insuranceRefundAmount.hashCode());
        final Object $secondReviewExecutor = this.getSecondReviewExecutor();
        result = result * PRIME + ($secondReviewExecutor == null ? 43 : $secondReviewExecutor.hashCode());
        final Object $secondReviewTime = this.getSecondReviewTime();
        result = result * PRIME + ($secondReviewTime == null ? 43 : $secondReviewTime.hashCode());
        final Object $secondReviewOpinion = this.getSecondReviewOpinion();
        result = result * PRIME + ($secondReviewOpinion == null ? 43 : $secondReviewOpinion.hashCode());
        final Object $refundExecutor = this.getRefundExecutor();
        result = result * PRIME + ($refundExecutor == null ? 43 : $refundExecutor.hashCode());
        final Object $ticketRefundCompletionTime = this.getTicketRefundCompletionTime();
        result = result * PRIME + ($ticketRefundCompletionTime == null ? 43 : $ticketRefundCompletionTime.hashCode());
        final Object $refundReviewOpinion = this.getRefundReviewOpinion();
        result = result * PRIME + ($refundReviewOpinion == null ? 43 : $refundReviewOpinion.hashCode());
        final Object $paymentTime = this.getPaymentTime();
        result = result * PRIME + ($paymentTime == null ? 43 : $paymentTime.hashCode());
        final Object $ticketRefundNature = this.getTicketRefundNature();
        result = result * PRIME + ($ticketRefundNature == null ? 43 : $ticketRefundNature.hashCode());
        final Object $ticketRefundApplicant = this.getTicketRefundApplicant();
        result = result * PRIME + ($ticketRefundApplicant == null ? 43 : $ticketRefundApplicant.hashCode());
        final Object $ticketRefundApplicationTime = this.getTicketRefundApplicationTime();
        result = result * PRIME + ($ticketRefundApplicationTime == null ? 43 : $ticketRefundApplicationTime.hashCode());
        final Object $pnrCancellationTime = this.getPnrCancellationTime();
        result = result * PRIME + ($pnrCancellationTime == null ? 43 : $pnrCancellationTime.hashCode());
        final Object $ticketRefundReason = this.getTicketRefundReason();
        result = result * PRIME + ($ticketRefundReason == null ? 43 : $ticketRefundReason.hashCode());
        final Object $currency = this.getCurrency();
        result = result * PRIME + ($currency == null ? 43 : $currency.hashCode());
        final Object $settlementNumber = this.getSettlementNumber();
        result = result * PRIME + ($settlementNumber == null ? 43 : $settlementNumber.hashCode());
        final Object $ticketRefundNumber = this.getTicketRefundNumber();
        result = result * PRIME + ($ticketRefundNumber == null ? 43 : $ticketRefundNumber.hashCode());
        final Object $ticketRefundStatus = this.getTicketRefundStatus();
        result = result * PRIME + ($ticketRefundStatus == null ? 43 : $ticketRefundStatus.hashCode());
        final Object $channel = this.getChannel();
        result = result * PRIME + ($channel == null ? 43 : $channel.hashCode());
        final Object $site = this.getSite();
        result = result * PRIME + ($site == null ? 43 : $site.hashCode());
        final Object $ticketRefundTime = this.getTicketRefundTime();
        result = result * PRIME + ($ticketRefundTime == null ? 43 : $ticketRefundTime.hashCode());
        final Object $refundInitiationTime = this.getRefundInitiationTime();
        result = result * PRIME + ($refundInitiationTime == null ? 43 : $refundInitiationTime.hashCode());
        final Object $ticketType = this.getTicketType();
        result = result * PRIME + ($ticketType == null ? 43 : $ticketType.hashCode());
        final Object $pointsRefundStatus = this.getPointsRefundStatus();
        result = result * PRIME + ($pointsRefundStatus == null ? 43 : $pointsRefundStatus.hashCode());
        final Object $mileagePaymentOrderNumber = this.getMileagePaymentOrderNumber();
        result = result * PRIME + ($mileagePaymentOrderNumber == null ? 43 : $mileagePaymentOrderNumber.hashCode());
        final Object $paidMileage = this.getPaidMileage();
        result = result * PRIME + ($paidMileage == null ? 43 : $paidMileage.hashCode());
        final Object $refundedMileage = this.getRefundedMileage();
        result = result * PRIME + ($refundedMileage == null ? 43 : $refundedMileage.hashCode());
        final Object $mileageFee = this.getMileageFee();
        result = result * PRIME + ($mileageFee == null ? 43 : $mileageFee.hashCode());
        final Object $pointsRefundExecutor = this.getPointsRefundExecutor();
        result = result * PRIME + ($pointsRefundExecutor == null ? 43 : $pointsRefundExecutor.hashCode());
        final Object $pointsRefundCompletionTime = this.getPointsRefundCompletionTime();
        result = result * PRIME + ($pointsRefundCompletionTime == null ? 43 : $pointsRefundCompletionTime.hashCode());
        final Object $pointsRefundReviewOpinion = this.getPointsRefundReviewOpinion();
        result = result * PRIME + ($pointsRefundReviewOpinion == null ? 43 : $pointsRefundReviewOpinion.hashCode());
        final Object $ticketRefundApplicationChannel = this.getTicketRefundApplicationChannel();
        result = result * PRIME + ($ticketRefundApplicationChannel == null ? 43 : $ticketRefundApplicationChannel.hashCode());
        final Object $departureDate = this.getDepartureDate();
        result = result * PRIME + ($departureDate == null ? 43 : $departureDate.hashCode());
        final Object $departureCityCode = this.getDepartureCityCode();
        result = result * PRIME + ($departureCityCode == null ? 43 : $departureCityCode.hashCode());
        final Object $arrivalCityCode = this.getArrivalCityCode();
        result = result * PRIME + ($arrivalCityCode == null ? 43 : $arrivalCityCode.hashCode());
        final Object $accountUsername = this.getAccountUsername();
        result = result * PRIME + ($accountUsername == null ? 43 : $accountUsername.hashCode());
        final Object $etlCreateTime = this.getEtlCreateTime();
        result = result * PRIME + ($etlCreateTime == null ? 43 : $etlCreateTime.hashCode());
        final Object $etlUpdateTime = this.getEtlUpdateTime();
        result = result * PRIME + ($etlUpdateTime == null ? 43 : $etlUpdateTime.hashCode());
        final Object $etlDate = this.getEtlDate();
        result = result * PRIME + ($etlDate == null ? 43 : $etlDate.hashCode());
        return result;
    }

    public String toString() {
        return "TOdsScAirRefundDetail(id=" + this.getId() + ", orderDate=" + this.getOrderDate() + ", orderNumber=" + this.getOrderNumber() + ", pnr=" + this.getPnr() + ", ticketNature=" + this.getTicketNature() + ", ticketNumber=" + this.getTicketNumber() + ", passengerName=" + this.getPassengerName() + ", segmentSequenceNumber=" + this.getSegmentSequenceNumber() + ", flightNumber=" + this.getFlightNumber() + ", paymentBank=" + this.getPaymentBank() + ", bankOrderNumber=" + this.getBankOrderNumber() + ", paymentAmount=" + this.getPaymentAmount() + ", refundAmount=" + this.getRefundAmount() + ", ticketRefundAmount=" + this.getTicketRefundAmount() + ", ticketRefundRate=" + this.getTicketRefundRate() + ", ticketRefundFee=" + this.getTicketRefundFee() + ", airportConstructionRefund=" + this.getAirportConstructionRefund() + ", fuelSurchargeRefund=" + this.getFuelSurchargeRefund() + ", otherTaxRefund=" + this.getOtherTaxRefund() + ", totalTaxRefund=" + this.getTotalTaxRefund() + ", totalTaxRefundFee=" + this.getTotalTaxRefundFee() + ", insuranceRefundAmount=" + this.getInsuranceRefundAmount() + ", secondReviewExecutor=" + this.getSecondReviewExecutor() + ", secondReviewTime=" + this.getSecondReviewTime() + ", secondReviewOpinion=" + this.getSecondReviewOpinion() + ", refundExecutor=" + this.getRefundExecutor() + ", ticketRefundCompletionTime=" + this.getTicketRefundCompletionTime() + ", refundReviewOpinion=" + this.getRefundReviewOpinion() + ", paymentTime=" + this.getPaymentTime() + ", ticketRefundNature=" + this.getTicketRefundNature() + ", ticketRefundApplicant=" + this.getTicketRefundApplicant() + ", ticketRefundApplicationTime=" + this.getTicketRefundApplicationTime() + ", pnrCancellationTime=" + this.getPnrCancellationTime() + ", ticketRefundReason=" + this.getTicketRefundReason() + ", currency=" + this.getCurrency() + ", settlementNumber=" + this.getSettlementNumber() + ", ticketRefundNumber=" + this.getTicketRefundNumber() + ", ticketRefundStatus=" + this.getTicketRefundStatus() + ", channel=" + this.getChannel() + ", site=" + this.getSite() + ", ticketRefundTime=" + this.getTicketRefundTime() + ", refundInitiationTime=" + this.getRefundInitiationTime() + ", ticketType=" + this.getTicketType() + ", pointsRefundStatus=" + this.getPointsRefundStatus() + ", mileagePaymentOrderNumber=" + this.getMileagePaymentOrderNumber() + ", paidMileage=" + this.getPaidMileage() + ", refundedMileage=" + this.getRefundedMileage() + ", mileageFee=" + this.getMileageFee() + ", pointsRefundExecutor=" + this.getPointsRefundExecutor() + ", pointsRefundCompletionTime=" + this.getPointsRefundCompletionTime() + ", pointsRefundReviewOpinion=" + this.getPointsRefundReviewOpinion() + ", ticketRefundApplicationChannel=" + this.getTicketRefundApplicationChannel() + ", departureDate=" + this.getDepartureDate() + ", departureCityCode=" + this.getDepartureCityCode() + ", arrivalCityCode=" + this.getArrivalCityCode() + ", accountUsername=" + this.getAccountUsername() + ", etlCreateTime=" + this.getEtlCreateTime() + ", etlUpdateTime=" + this.getEtlUpdateTime() + ", etlDate=" + this.getEtlDate() + ")";
    }

    public String getFirstReviewExecutor() {
        return firstReviewExecutor;
    }

    public void setFirstReviewExecutor(String firstReviewExecutor) {
        this.firstReviewExecutor = firstReviewExecutor;
    }

    public String getFirstReviewTime() {
        return firstReviewTime;
    }

    public void setFirstReviewTime(String firstReviewTime) {
        this.firstReviewTime = firstReviewTime;
    }

    public String getFirstReviewOpinion() {
        return firstReviewOpinion;
    }

    public void setFirstReviewOpinion(String firstReviewOpinion) {
        this.firstReviewOpinion = firstReviewOpinion;
    }
}