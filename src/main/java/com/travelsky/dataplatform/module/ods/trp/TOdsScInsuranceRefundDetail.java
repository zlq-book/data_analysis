package com.travelsky.dataplatform.module.ods.trp;

import com.alibaba.excel.annotation.ExcelIgnore;
import com.alibaba.excel.annotation.ExcelProperty;

import java.math.BigDecimal;

/**
 * SC 保险退款报表明细 实体
 */
public class TOdsScInsuranceRefundDetail {

    /**
     * 主键ID
     */
    @ExcelProperty("序号")
    private Long id;

    @ExcelProperty("银行订单号")
    private String bankOrderNumber;

    @ExcelProperty("银行名称")
    private String bankName;

    @ExcelProperty("票号")
    private String ticketNumber;

    @ExcelProperty("保险单号")
    private String insurancePolicyNumber;

    @ExcelProperty("客票性质")
    private String ticketNature;

    @ExcelProperty("账号用户名")
    private String accountUsername;

    @ExcelProperty("起飞日期")
    private String departureDate;

    @ExcelProperty("出发地")
    private String departureCity;

    @ExcelProperty("到达地")
    private String arrivalCity;

    @ExcelProperty("乘客姓名")
    private String passengerName;

    @ExcelProperty("证件类型")
    private String idType;

    @ExcelProperty("证件号码")
    private String idNumber;

    @ExcelProperty("保险支付金额")
    private BigDecimal insurancePaymentAmount;

    @ExcelProperty("退款金额")
    private BigDecimal refundAmount;

    @ExcelProperty("退保类型")
    private String insuranceRefundType;

    @ExcelProperty("备注")
    private String remarks;

    @ExcelProperty("保险类型")
    private String insuranceType;

    @ExcelProperty("渠道来源")
    private String channelSource;

    @ExcelProperty("退保审核人员")
    private String refundApprover;

    @ExcelProperty("退保审核时间")
    private String refundApprovalTime;

    @ExcelProperty("退款状态")
    private String refundStatus;

    @ExcelProperty("订单号")
    private String orderNumber;

    @ExcelProperty("退款日期")
    private String refundDate;

    @ExcelProperty("购保类型")
    private String insurancePurchaseType;

    @ExcelProperty("退保成功时间")
    private String refundSuccessTime;

    @ExcelProperty("退保退款发起时间")
    private String refundInitiationTime;

    @ExcelProperty("退保申退时间")
    private String refundApplicationTime;

    @ExcelProperty("退保退款成功时间")
    private String refundCompletionTime;

    @ExcelProperty("站点")
    private String site;

    @ExcelProperty("币种")
    private String currency;

    @ExcelProperty("航联订单号")
    private String airlineOrderNumber;

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

    public TOdsScInsuranceRefundDetail() {
    }

    public Long getId() {
        return this.id;
    }

    public String getBankOrderNumber() {
        return this.bankOrderNumber;
    }

    public String getBankName() {
        return this.bankName;
    }

    public String getTicketNumber() {
        return this.ticketNumber;
    }

    public String getInsurancePolicyNumber() {
        return this.insurancePolicyNumber;
    }

    public String getTicketNature() {
        return this.ticketNature;
    }

    public String getAccountUsername() {
        return this.accountUsername;
    }

    public String getDepartureDate() {
        return this.departureDate;
    }

    public String getDepartureCity() {
        return this.departureCity;
    }

    public String getArrivalCity() {
        return this.arrivalCity;
    }

    public String getPassengerName() {
        return this.passengerName;
    }

    public String getIdType() {
        return this.idType;
    }

    public String getIdNumber() {
        return this.idNumber;
    }

    public BigDecimal getInsurancePaymentAmount() {
        return this.insurancePaymentAmount;
    }

    public BigDecimal getRefundAmount() {
        return this.refundAmount;
    }

    public String getInsuranceRefundType() {
        return this.insuranceRefundType;
    }

    public String getRemarks() {
        return this.remarks;
    }

    public String getInsuranceType() {
        return this.insuranceType;
    }

    public String getChannelSource() {
        return this.channelSource;
    }

    public String getRefundApprover() {
        return this.refundApprover;
    }

    public String getRefundApprovalTime() {
        return this.refundApprovalTime;
    }

    public String getRefundStatus() {
        return this.refundStatus;
    }

    public String getOrderNumber() {
        return this.orderNumber;
    }

    public String getRefundDate() {
        return this.refundDate;
    }

    public String getInsurancePurchaseType() {
        return this.insurancePurchaseType;
    }

    public String getRefundSuccessTime() {
        return this.refundSuccessTime;
    }

    public String getRefundInitiationTime() {
        return this.refundInitiationTime;
    }

    public String getRefundApplicationTime() {
        return this.refundApplicationTime;
    }

    public String getRefundCompletionTime() {
        return this.refundCompletionTime;
    }

    public String getSite() {
        return this.site;
    }

    public String getCurrency() {
        return this.currency;
    }

    public String getAirlineOrderNumber() {
        return this.airlineOrderNumber;
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

    public void setBankOrderNumber(String bankOrderNumber) {
        this.bankOrderNumber = bankOrderNumber;
    }

    public void setBankName(String bankName) {
        this.bankName = bankName;
    }

    public void setTicketNumber(String ticketNumber) {
        this.ticketNumber = ticketNumber;
    }

    public void setInsurancePolicyNumber(String insurancePolicyNumber) {
        this.insurancePolicyNumber = insurancePolicyNumber;
    }

    public void setTicketNature(String ticketNature) {
        this.ticketNature = ticketNature;
    }

    public void setAccountUsername(String accountUsername) {
        this.accountUsername = accountUsername;
    }

    public void setDepartureDate(String departureDate) {
        this.departureDate = departureDate;
    }

    public void setDepartureCity(String departureCity) {
        this.departureCity = departureCity;
    }

    public void setArrivalCity(String arrivalCity) {
        this.arrivalCity = arrivalCity;
    }

    public void setPassengerName(String passengerName) {
        this.passengerName = passengerName;
    }

    public void setIdType(String idType) {
        this.idType = idType;
    }

    public void setIdNumber(String idNumber) {
        this.idNumber = idNumber;
    }

    public void setInsurancePaymentAmount(BigDecimal insurancePaymentAmount) {
        this.insurancePaymentAmount = insurancePaymentAmount;
    }

    public void setRefundAmount(BigDecimal refundAmount) {
        this.refundAmount = refundAmount;
    }

    public void setInsuranceRefundType(String insuranceRefundType) {
        this.insuranceRefundType = insuranceRefundType;
    }

    public void setRemarks(String remarks) {
        this.remarks = remarks;
    }

    public void setInsuranceType(String insuranceType) {
        this.insuranceType = insuranceType;
    }

    public void setChannelSource(String channelSource) {
        this.channelSource = channelSource;
    }

    public void setRefundApprover(String refundApprover) {
        this.refundApprover = refundApprover;
    }

    public void setRefundApprovalTime(String refundApprovalTime) {
        this.refundApprovalTime = refundApprovalTime;
    }

    public void setRefundStatus(String refundStatus) {
        this.refundStatus = refundStatus;
    }

    public void setOrderNumber(String orderNumber) {
        this.orderNumber = orderNumber;
    }

    public void setRefundDate(String refundDate) {
        this.refundDate = refundDate;
    }

    public void setInsurancePurchaseType(String insurancePurchaseType) {
        this.insurancePurchaseType = insurancePurchaseType;
    }

    public void setRefundSuccessTime(String refundSuccessTime) {
        this.refundSuccessTime = refundSuccessTime;
    }

    public void setRefundInitiationTime(String refundInitiationTime) {
        this.refundInitiationTime = refundInitiationTime;
    }

    public void setRefundApplicationTime(String refundApplicationTime) {
        this.refundApplicationTime = refundApplicationTime;
    }

    public void setRefundCompletionTime(String refundCompletionTime) {
        this.refundCompletionTime = refundCompletionTime;
    }

    public void setSite(String site) {
        this.site = site;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public void setAirlineOrderNumber(String airlineOrderNumber) {
        this.airlineOrderNumber = airlineOrderNumber;
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
        if (!(o instanceof TOdsScInsuranceRefundDetail)) return false;
        final TOdsScInsuranceRefundDetail other = (TOdsScInsuranceRefundDetail) o;
        if (!other.canEqual((Object) this)) return false;
        final Object this$id = this.getId();
        final Object other$id = other.getId();
        if (this$id == null ? other$id != null : !this$id.equals(other$id)) return false;
        final Object this$bankOrderNumber = this.getBankOrderNumber();
        final Object other$bankOrderNumber = other.getBankOrderNumber();
        if (this$bankOrderNumber == null ? other$bankOrderNumber != null : !this$bankOrderNumber.equals(other$bankOrderNumber))
            return false;
        final Object this$bankName = this.getBankName();
        final Object other$bankName = other.getBankName();
        if (this$bankName == null ? other$bankName != null : !this$bankName.equals(other$bankName)) return false;
        final Object this$ticketNumber = this.getTicketNumber();
        final Object other$ticketNumber = other.getTicketNumber();
        if (this$ticketNumber == null ? other$ticketNumber != null : !this$ticketNumber.equals(other$ticketNumber))
            return false;
        final Object this$insurancePolicyNumber = this.getInsurancePolicyNumber();
        final Object other$insurancePolicyNumber = other.getInsurancePolicyNumber();
        if (this$insurancePolicyNumber == null ? other$insurancePolicyNumber != null : !this$insurancePolicyNumber.equals(other$insurancePolicyNumber))
            return false;
        final Object this$ticketNature = this.getTicketNature();
        final Object other$ticketNature = other.getTicketNature();
        if (this$ticketNature == null ? other$ticketNature != null : !this$ticketNature.equals(other$ticketNature))
            return false;
        final Object this$accountUsername = this.getAccountUsername();
        final Object other$accountUsername = other.getAccountUsername();
        if (this$accountUsername == null ? other$accountUsername != null : !this$accountUsername.equals(other$accountUsername))
            return false;
        final Object this$departureDate = this.getDepartureDate();
        final Object other$departureDate = other.getDepartureDate();
        if (this$departureDate == null ? other$departureDate != null : !this$departureDate.equals(other$departureDate))
            return false;
        final Object this$departureCity = this.getDepartureCity();
        final Object other$departureCity = other.getDepartureCity();
        if (this$departureCity == null ? other$departureCity != null : !this$departureCity.equals(other$departureCity))
            return false;
        final Object this$arrivalCity = this.getArrivalCity();
        final Object other$arrivalCity = other.getArrivalCity();
        if (this$arrivalCity == null ? other$arrivalCity != null : !this$arrivalCity.equals(other$arrivalCity))
            return false;
        final Object this$passengerName = this.getPassengerName();
        final Object other$passengerName = other.getPassengerName();
        if (this$passengerName == null ? other$passengerName != null : !this$passengerName.equals(other$passengerName))
            return false;
        final Object this$idType = this.getIdType();
        final Object other$idType = other.getIdType();
        if (this$idType == null ? other$idType != null : !this$idType.equals(other$idType)) return false;
        final Object this$idNumber = this.getIdNumber();
        final Object other$idNumber = other.getIdNumber();
        if (this$idNumber == null ? other$idNumber != null : !this$idNumber.equals(other$idNumber)) return false;
        final Object this$insurancePaymentAmount = this.getInsurancePaymentAmount();
        final Object other$insurancePaymentAmount = other.getInsurancePaymentAmount();
        if (this$insurancePaymentAmount == null ? other$insurancePaymentAmount != null : !this$insurancePaymentAmount.equals(other$insurancePaymentAmount))
            return false;
        final Object this$refundAmount = this.getRefundAmount();
        final Object other$refundAmount = other.getRefundAmount();
        if (this$refundAmount == null ? other$refundAmount != null : !this$refundAmount.equals(other$refundAmount))
            return false;
        final Object this$insuranceRefundType = this.getInsuranceRefundType();
        final Object other$insuranceRefundType = other.getInsuranceRefundType();
        if (this$insuranceRefundType == null ? other$insuranceRefundType != null : !this$insuranceRefundType.equals(other$insuranceRefundType))
            return false;
        final Object this$remarks = this.getRemarks();
        final Object other$remarks = other.getRemarks();
        if (this$remarks == null ? other$remarks != null : !this$remarks.equals(other$remarks)) return false;
        final Object this$insuranceType = this.getInsuranceType();
        final Object other$insuranceType = other.getInsuranceType();
        if (this$insuranceType == null ? other$insuranceType != null : !this$insuranceType.equals(other$insuranceType))
            return false;
        final Object this$channelSource = this.getChannelSource();
        final Object other$channelSource = other.getChannelSource();
        if (this$channelSource == null ? other$channelSource != null : !this$channelSource.equals(other$channelSource))
            return false;
        final Object this$refundApprover = this.getRefundApprover();
        final Object other$refundApprover = other.getRefundApprover();
        if (this$refundApprover == null ? other$refundApprover != null : !this$refundApprover.equals(other$refundApprover))
            return false;
        final Object this$refundApprovalTime = this.getRefundApprovalTime();
        final Object other$refundApprovalTime = other.getRefundApprovalTime();
        if (this$refundApprovalTime == null ? other$refundApprovalTime != null : !this$refundApprovalTime.equals(other$refundApprovalTime))
            return false;
        final Object this$refundStatus = this.getRefundStatus();
        final Object other$refundStatus = other.getRefundStatus();
        if (this$refundStatus == null ? other$refundStatus != null : !this$refundStatus.equals(other$refundStatus))
            return false;
        final Object this$orderNumber = this.getOrderNumber();
        final Object other$orderNumber = other.getOrderNumber();
        if (this$orderNumber == null ? other$orderNumber != null : !this$orderNumber.equals(other$orderNumber))
            return false;
        final Object this$refundDate = this.getRefundDate();
        final Object other$refundDate = other.getRefundDate();
        if (this$refundDate == null ? other$refundDate != null : !this$refundDate.equals(other$refundDate))
            return false;
        final Object this$insurancePurchaseType = this.getInsurancePurchaseType();
        final Object other$insurancePurchaseType = other.getInsurancePurchaseType();
        if (this$insurancePurchaseType == null ? other$insurancePurchaseType != null : !this$insurancePurchaseType.equals(other$insurancePurchaseType))
            return false;
        final Object this$refundSuccessTime = this.getRefundSuccessTime();
        final Object other$refundSuccessTime = other.getRefundSuccessTime();
        if (this$refundSuccessTime == null ? other$refundSuccessTime != null : !this$refundSuccessTime.equals(other$refundSuccessTime))
            return false;
        final Object this$refundInitiationTime = this.getRefundInitiationTime();
        final Object other$refundInitiationTime = other.getRefundInitiationTime();
        if (this$refundInitiationTime == null ? other$refundInitiationTime != null : !this$refundInitiationTime.equals(other$refundInitiationTime))
            return false;
        final Object this$refundApplicationTime = this.getRefundApplicationTime();
        final Object other$refundApplicationTime = other.getRefundApplicationTime();
        if (this$refundApplicationTime == null ? other$refundApplicationTime != null : !this$refundApplicationTime.equals(other$refundApplicationTime))
            return false;
        final Object this$refundCompletionTime = this.getRefundCompletionTime();
        final Object other$refundCompletionTime = other.getRefundCompletionTime();
        if (this$refundCompletionTime == null ? other$refundCompletionTime != null : !this$refundCompletionTime.equals(other$refundCompletionTime))
            return false;
        final Object this$site = this.getSite();
        final Object other$site = other.getSite();
        if (this$site == null ? other$site != null : !this$site.equals(other$site)) return false;
        final Object this$currency = this.getCurrency();
        final Object other$currency = other.getCurrency();
        if (this$currency == null ? other$currency != null : !this$currency.equals(other$currency)) return false;
        final Object this$airlineOrderNumber = this.getAirlineOrderNumber();
        final Object other$airlineOrderNumber = other.getAirlineOrderNumber();
        if (this$airlineOrderNumber == null ? other$airlineOrderNumber != null : !this$airlineOrderNumber.equals(other$airlineOrderNumber))
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
        return other instanceof TOdsScInsuranceRefundDetail;
    }

    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        final Object $id = this.getId();
        result = result * PRIME + ($id == null ? 43 : $id.hashCode());
        final Object $bankOrderNumber = this.getBankOrderNumber();
        result = result * PRIME + ($bankOrderNumber == null ? 43 : $bankOrderNumber.hashCode());
        final Object $bankName = this.getBankName();
        result = result * PRIME + ($bankName == null ? 43 : $bankName.hashCode());
        final Object $ticketNumber = this.getTicketNumber();
        result = result * PRIME + ($ticketNumber == null ? 43 : $ticketNumber.hashCode());
        final Object $insurancePolicyNumber = this.getInsurancePolicyNumber();
        result = result * PRIME + ($insurancePolicyNumber == null ? 43 : $insurancePolicyNumber.hashCode());
        final Object $ticketNature = this.getTicketNature();
        result = result * PRIME + ($ticketNature == null ? 43 : $ticketNature.hashCode());
        final Object $accountUsername = this.getAccountUsername();
        result = result * PRIME + ($accountUsername == null ? 43 : $accountUsername.hashCode());
        final Object $departureDate = this.getDepartureDate();
        result = result * PRIME + ($departureDate == null ? 43 : $departureDate.hashCode());
        final Object $departureCity = this.getDepartureCity();
        result = result * PRIME + ($departureCity == null ? 43 : $departureCity.hashCode());
        final Object $arrivalCity = this.getArrivalCity();
        result = result * PRIME + ($arrivalCity == null ? 43 : $arrivalCity.hashCode());
        final Object $passengerName = this.getPassengerName();
        result = result * PRIME + ($passengerName == null ? 43 : $passengerName.hashCode());
        final Object $idType = this.getIdType();
        result = result * PRIME + ($idType == null ? 43 : $idType.hashCode());
        final Object $idNumber = this.getIdNumber();
        result = result * PRIME + ($idNumber == null ? 43 : $idNumber.hashCode());
        final Object $insurancePaymentAmount = this.getInsurancePaymentAmount();
        result = result * PRIME + ($insurancePaymentAmount == null ? 43 : $insurancePaymentAmount.hashCode());
        final Object $refundAmount = this.getRefundAmount();
        result = result * PRIME + ($refundAmount == null ? 43 : $refundAmount.hashCode());
        final Object $insuranceRefundType = this.getInsuranceRefundType();
        result = result * PRIME + ($insuranceRefundType == null ? 43 : $insuranceRefundType.hashCode());
        final Object $remarks = this.getRemarks();
        result = result * PRIME + ($remarks == null ? 43 : $remarks.hashCode());
        final Object $insuranceType = this.getInsuranceType();
        result = result * PRIME + ($insuranceType == null ? 43 : $insuranceType.hashCode());
        final Object $channelSource = this.getChannelSource();
        result = result * PRIME + ($channelSource == null ? 43 : $channelSource.hashCode());
        final Object $refundApprover = this.getRefundApprover();
        result = result * PRIME + ($refundApprover == null ? 43 : $refundApprover.hashCode());
        final Object $refundApprovalTime = this.getRefundApprovalTime();
        result = result * PRIME + ($refundApprovalTime == null ? 43 : $refundApprovalTime.hashCode());
        final Object $refundStatus = this.getRefundStatus();
        result = result * PRIME + ($refundStatus == null ? 43 : $refundStatus.hashCode());
        final Object $orderNumber = this.getOrderNumber();
        result = result * PRIME + ($orderNumber == null ? 43 : $orderNumber.hashCode());
        final Object $refundDate = this.getRefundDate();
        result = result * PRIME + ($refundDate == null ? 43 : $refundDate.hashCode());
        final Object $insurancePurchaseType = this.getInsurancePurchaseType();
        result = result * PRIME + ($insurancePurchaseType == null ? 43 : $insurancePurchaseType.hashCode());
        final Object $refundSuccessTime = this.getRefundSuccessTime();
        result = result * PRIME + ($refundSuccessTime == null ? 43 : $refundSuccessTime.hashCode());
        final Object $refundInitiationTime = this.getRefundInitiationTime();
        result = result * PRIME + ($refundInitiationTime == null ? 43 : $refundInitiationTime.hashCode());
        final Object $refundApplicationTime = this.getRefundApplicationTime();
        result = result * PRIME + ($refundApplicationTime == null ? 43 : $refundApplicationTime.hashCode());
        final Object $refundCompletionTime = this.getRefundCompletionTime();
        result = result * PRIME + ($refundCompletionTime == null ? 43 : $refundCompletionTime.hashCode());
        final Object $site = this.getSite();
        result = result * PRIME + ($site == null ? 43 : $site.hashCode());
        final Object $currency = this.getCurrency();
        result = result * PRIME + ($currency == null ? 43 : $currency.hashCode());
        final Object $airlineOrderNumber = this.getAirlineOrderNumber();
        result = result * PRIME + ($airlineOrderNumber == null ? 43 : $airlineOrderNumber.hashCode());
        final Object $etlCreateTime = this.getEtlCreateTime();
        result = result * PRIME + ($etlCreateTime == null ? 43 : $etlCreateTime.hashCode());
        final Object $etlUpdateTime = this.getEtlUpdateTime();
        result = result * PRIME + ($etlUpdateTime == null ? 43 : $etlUpdateTime.hashCode());
        final Object $etlDate = this.getEtlDate();
        result = result * PRIME + ($etlDate == null ? 43 : $etlDate.hashCode());
        return result;
    }

    public String toString() {
        return "TOdsScInsuranceRefundDetail(id=" + this.getId() + ", bankOrderNumber=" + this.getBankOrderNumber() + ", bankName=" + this.getBankName() + ", ticketNumber=" + this.getTicketNumber() + ", insurancePolicyNumber=" + this.getInsurancePolicyNumber() + ", ticketNature=" + this.getTicketNature() + ", accountUsername=" + this.getAccountUsername() + ", departureDate=" + this.getDepartureDate() + ", departureCity=" + this.getDepartureCity() + ", arrivalCity=" + this.getArrivalCity() + ", passengerName=" + this.getPassengerName() + ", idType=" + this.getIdType() + ", idNumber=" + this.getIdNumber() + ", insurancePaymentAmount=" + this.getInsurancePaymentAmount() + ", refundAmount=" + this.getRefundAmount() + ", insuranceRefundType=" + this.getInsuranceRefundType() + ", remarks=" + this.getRemarks() + ", insuranceType=" + this.getInsuranceType() + ", channelSource=" + this.getChannelSource() + ", refundApprover=" + this.getRefundApprover() + ", refundApprovalTime=" + this.getRefundApprovalTime() + ", refundStatus=" + this.getRefundStatus() + ", orderNumber=" + this.getOrderNumber() + ", refundDate=" + this.getRefundDate() + ", insurancePurchaseType=" + this.getInsurancePurchaseType() + ", refundSuccessTime=" + this.getRefundSuccessTime() + ", refundInitiationTime=" + this.getRefundInitiationTime() + ", refundApplicationTime=" + this.getRefundApplicationTime() + ", refundCompletionTime=" + this.getRefundCompletionTime() + ", site=" + this.getSite() + ", currency=" + this.getCurrency() + ", airlineOrderNumber=" + this.getAirlineOrderNumber() + ", etlCreateTime=" + this.getEtlCreateTime() + ", etlUpdateTime=" + this.getEtlUpdateTime() + ", etlDate=" + this.getEtlDate() + ")";
    }
}