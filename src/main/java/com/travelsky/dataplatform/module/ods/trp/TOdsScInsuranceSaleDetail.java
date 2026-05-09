package com.travelsky.dataplatform.module.ods.trp;

import com.alibaba.excel.annotation.ExcelIgnore;
import com.alibaba.excel.annotation.ExcelProperty;

import java.math.BigDecimal;

/**
 * SC 保险销售报表明细 实体
 */
public class TOdsScInsuranceSaleDetail {

    /**
     * 主键ID
     */
    @ExcelProperty("序号")
    private Long id;

    @ExcelProperty("订单号")
    private String orderNumber;

    @ExcelProperty("旅客姓名")
    private String passengerName;

    @ExcelProperty("订票时间")
    private String bookingTime;

    @ExcelProperty("银行订单号")
    private String bankOrderNumber;

    @ExcelProperty("银行名称")
    private String bankName;

    @ExcelProperty("PNR")
    private String pnr;

    @ExcelProperty("票号")
    private String ticketNumber;

    @ExcelProperty("保险单号")
    private String insurancePolicyNumber;

    @ExcelProperty("保险费用")
    private BigDecimal insuranceFee;

    @ExcelProperty("支付时间")
    private String paymentTime;

    @ExcelProperty("购保成功时间")
    private String insurancePurchaseSuccessTime;

    @ExcelProperty("购保类型")
    private String insurancePurchaseType;

    @ExcelProperty("保险状态")
    private String insuranceStatus;

    @ExcelProperty("备注")
    private String remarks;

    @ExcelProperty("保险类型")
    private String insuranceType;

    @ExcelProperty("渠道来源")
    private String channelSource;

    @ExcelProperty("购保状态")
    private String insurancePurchaseStatus;

    @ExcelProperty("购保发起时间")
    private String insurancePurchaseInitiationTime;

    @ExcelProperty("购保失败时间")
    private String insurancePurchaseFailureTime;

    @ExcelProperty("站点")
    private String site;

    @ExcelProperty("币种")
    private String currency;

    @ExcelProperty("航联订单号")
    private String airlineOrderNumber;

    @ExcelProperty("客票性质")
    private String ticketNature;

    @ExcelProperty("账号用户名")
    private String accountUsername;

    @ExcelProperty("起飞时期")
    private String departureDate;

    @ExcelProperty("出发地")
    private String departureCity;

    @ExcelProperty("到达地")
    private String arrivalCity;

    @ExcelProperty(value = "证件类型", index = 28)
    private String idType;

    @ExcelProperty(value = "证件号", index = 29)
    private String idNumber;

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

    public TOdsScInsuranceSaleDetail() {
    }

    public Long getId() {
        return this.id;
    }

    public String getOrderNumber() {
        return this.orderNumber;
    }

    public String getPassengerName() {
        return this.passengerName;
    }

    public String getBookingTime() {
        return this.bookingTime;
    }

    public String getBankOrderNumber() {
        return this.bankOrderNumber;
    }

    public String getBankName() {
        return this.bankName;
    }

    public String getPnr() {
        return this.pnr;
    }

    public String getTicketNumber() {
        return this.ticketNumber;
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

    public String getIdType() {
        return this.idType;
    }

    public String getIdNumber() {
        return this.idNumber;
    }

    public String getInsurancePolicyNumber() {
        return this.insurancePolicyNumber;
    }

    public BigDecimal getInsuranceFee() {
        return this.insuranceFee;
    }

    public String getPaymentTime() {
        return this.paymentTime;
    }

    public String getInsurancePurchaseSuccessTime() {
        return this.insurancePurchaseSuccessTime;
    }

    public String getInsurancePurchaseType() {
        return this.insurancePurchaseType;
    }

    public String getInsuranceStatus() {
        return this.insuranceStatus;
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

    public String getInsurancePurchaseStatus() {
        return this.insurancePurchaseStatus;
    }

    public String getInsurancePurchaseInitiationTime() {
        return this.insurancePurchaseInitiationTime;
    }

    public String getInsurancePurchaseFailureTime() {
        return this.insurancePurchaseFailureTime;
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

    public void setOrderNumber(String orderNumber) {
        this.orderNumber = orderNumber;
    }

    public void setPassengerName(String passengerName) {
        this.passengerName = passengerName;
    }

    public void setBookingTime(String bookingTime) {
        this.bookingTime = bookingTime;
    }

    public void setBankOrderNumber(String bankOrderNumber) {
        this.bankOrderNumber = bankOrderNumber;
    }

    public void setBankName(String bankName) {
        this.bankName = bankName;
    }

    public void setPnr(String pnr) {
        this.pnr = pnr;
    }

    public void setTicketNumber(String ticketNumber) {
        this.ticketNumber = ticketNumber;
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

    public void setIdType(String idType) {
        this.idType = idType;
    }

    public void setIdNumber(String idNumber) {
        this.idNumber = idNumber;
    }

    public void setInsurancePolicyNumber(String insurancePolicyNumber) {
        this.insurancePolicyNumber = insurancePolicyNumber;
    }

    public void setInsuranceFee(BigDecimal insuranceFee) {
        this.insuranceFee = insuranceFee;
    }

    public void setPaymentTime(String paymentTime) {
        this.paymentTime = paymentTime;
    }

    public void setInsurancePurchaseSuccessTime(String insurancePurchaseSuccessTime) {
        this.insurancePurchaseSuccessTime = insurancePurchaseSuccessTime;
    }

    public void setInsurancePurchaseType(String insurancePurchaseType) {
        this.insurancePurchaseType = insurancePurchaseType;
    }

    public void setInsuranceStatus(String insuranceStatus) {
        this.insuranceStatus = insuranceStatus;
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

    public void setInsurancePurchaseStatus(String insurancePurchaseStatus) {
        this.insurancePurchaseStatus = insurancePurchaseStatus;
    }

    public void setInsurancePurchaseInitiationTime(String insurancePurchaseInitiationTime) {
        this.insurancePurchaseInitiationTime = insurancePurchaseInitiationTime;
    }

    public void setInsurancePurchaseFailureTime(String insurancePurchaseFailureTime) {
        this.insurancePurchaseFailureTime = insurancePurchaseFailureTime;
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
        if (!(o instanceof TOdsScInsuranceSaleDetail)) return false;
        final TOdsScInsuranceSaleDetail other = (TOdsScInsuranceSaleDetail) o;
        if (!other.canEqual((Object) this)) return false;
        final Object this$id = this.getId();
        final Object other$id = other.getId();
        if (this$id == null ? other$id != null : !this$id.equals(other$id)) return false;
        final Object this$orderNumber = this.getOrderNumber();
        final Object other$orderNumber = other.getOrderNumber();
        if (this$orderNumber == null ? other$orderNumber != null : !this$orderNumber.equals(other$orderNumber))
            return false;
        final Object this$passengerName = this.getPassengerName();
        final Object other$passengerName = other.getPassengerName();
        if (this$passengerName == null ? other$passengerName != null : !this$passengerName.equals(other$passengerName))
            return false;
        final Object this$bookingTime = this.getBookingTime();
        final Object other$bookingTime = other.getBookingTime();
        if (this$bookingTime == null ? other$bookingTime != null : !this$bookingTime.equals(other$bookingTime))
            return false;
        final Object this$bankOrderNumber = this.getBankOrderNumber();
        final Object other$bankOrderNumber = other.getBankOrderNumber();
        if (this$bankOrderNumber == null ? other$bankOrderNumber != null : !this$bankOrderNumber.equals(other$bankOrderNumber))
            return false;
        final Object this$bankName = this.getBankName();
        final Object other$bankName = other.getBankName();
        if (this$bankName == null ? other$bankName != null : !this$bankName.equals(other$bankName)) return false;
        final Object this$pnr = this.getPnr();
        final Object other$pnr = other.getPnr();
        if (this$pnr == null ? other$pnr != null : !this$pnr.equals(other$pnr)) return false;
        final Object this$ticketNumber = this.getTicketNumber();
        final Object other$ticketNumber = other.getTicketNumber();
        if (this$ticketNumber == null ? other$ticketNumber != null : !this$ticketNumber.equals(other$ticketNumber))
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
        final Object this$idType = this.getIdType();
        final Object other$idType = other.getIdType();
        if (this$idType == null ? other$idType != null : !this$idType.equals(other$idType)) return false;
        final Object this$idNumber = this.getIdNumber();
        final Object other$idNumber = other.getIdNumber();
        if (this$idNumber == null ? other$idNumber != null : !this$idNumber.equals(other$idNumber)) return false;
        final Object this$insurancePolicyNumber = this.getInsurancePolicyNumber();
        final Object other$insurancePolicyNumber = other.getInsurancePolicyNumber();
        if (this$insurancePolicyNumber == null ? other$insurancePolicyNumber != null : !this$insurancePolicyNumber.equals(other$insurancePolicyNumber))
            return false;
        final Object this$insuranceFee = this.getInsuranceFee();
        final Object other$insuranceFee = other.getInsuranceFee();
        if (this$insuranceFee == null ? other$insuranceFee != null : !this$insuranceFee.equals(other$insuranceFee))
            return false;
        final Object this$paymentTime = this.getPaymentTime();
        final Object other$paymentTime = other.getPaymentTime();
        if (this$paymentTime == null ? other$paymentTime != null : !this$paymentTime.equals(other$paymentTime))
            return false;
        final Object this$insurancePurchaseSuccessTime = this.getInsurancePurchaseSuccessTime();
        final Object other$insurancePurchaseSuccessTime = other.getInsurancePurchaseSuccessTime();
        if (this$insurancePurchaseSuccessTime == null ? other$insurancePurchaseSuccessTime != null : !this$insurancePurchaseSuccessTime.equals(other$insurancePurchaseSuccessTime))
            return false;
        final Object this$insurancePurchaseType = this.getInsurancePurchaseType();
        final Object other$insurancePurchaseType = other.getInsurancePurchaseType();
        if (this$insurancePurchaseType == null ? other$insurancePurchaseType != null : !this$insurancePurchaseType.equals(other$insurancePurchaseType))
            return false;
        final Object this$insuranceStatus = this.getInsuranceStatus();
        final Object other$insuranceStatus = other.getInsuranceStatus();
        if (this$insuranceStatus == null ? other$insuranceStatus != null : !this$insuranceStatus.equals(other$insuranceStatus))
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
        final Object this$insurancePurchaseStatus = this.getInsurancePurchaseStatus();
        final Object other$insurancePurchaseStatus = other.getInsurancePurchaseStatus();
        if (this$insurancePurchaseStatus == null ? other$insurancePurchaseStatus != null : !this$insurancePurchaseStatus.equals(other$insurancePurchaseStatus))
            return false;
        final Object this$insurancePurchaseInitiationTime = this.getInsurancePurchaseInitiationTime();
        final Object other$insurancePurchaseInitiationTime = other.getInsurancePurchaseInitiationTime();
        if (this$insurancePurchaseInitiationTime == null ? other$insurancePurchaseInitiationTime != null : !this$insurancePurchaseInitiationTime.equals(other$insurancePurchaseInitiationTime))
            return false;
        final Object this$insurancePurchaseFailureTime = this.getInsurancePurchaseFailureTime();
        final Object other$insurancePurchaseFailureTime = other.getInsurancePurchaseFailureTime();
        if (this$insurancePurchaseFailureTime == null ? other$insurancePurchaseFailureTime != null : !this$insurancePurchaseFailureTime.equals(other$insurancePurchaseFailureTime))
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
        return other instanceof TOdsScInsuranceSaleDetail;
    }

    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        final Object $id = this.getId();
        result = result * PRIME + ($id == null ? 43 : $id.hashCode());
        final Object $orderNumber = this.getOrderNumber();
        result = result * PRIME + ($orderNumber == null ? 43 : $orderNumber.hashCode());
        final Object $passengerName = this.getPassengerName();
        result = result * PRIME + ($passengerName == null ? 43 : $passengerName.hashCode());
        final Object $bookingTime = this.getBookingTime();
        result = result * PRIME + ($bookingTime == null ? 43 : $bookingTime.hashCode());
        final Object $bankOrderNumber = this.getBankOrderNumber();
        result = result * PRIME + ($bankOrderNumber == null ? 43 : $bankOrderNumber.hashCode());
        final Object $bankName = this.getBankName();
        result = result * PRIME + ($bankName == null ? 43 : $bankName.hashCode());
        final Object $pnr = this.getPnr();
        result = result * PRIME + ($pnr == null ? 43 : $pnr.hashCode());
        final Object $ticketNumber = this.getTicketNumber();
        result = result * PRIME + ($ticketNumber == null ? 43 : $ticketNumber.hashCode());
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
        final Object $idType = this.getIdType();
        result = result * PRIME + ($idType == null ? 43 : $idType.hashCode());
        final Object $idNumber = this.getIdNumber();
        result = result * PRIME + ($idNumber == null ? 43 : $idNumber.hashCode());
        final Object $insurancePolicyNumber = this.getInsurancePolicyNumber();
        result = result * PRIME + ($insurancePolicyNumber == null ? 43 : $insurancePolicyNumber.hashCode());
        final Object $insuranceFee = this.getInsuranceFee();
        result = result * PRIME + ($insuranceFee == null ? 43 : $insuranceFee.hashCode());
        final Object $paymentTime = this.getPaymentTime();
        result = result * PRIME + ($paymentTime == null ? 43 : $paymentTime.hashCode());
        final Object $insurancePurchaseSuccessTime = this.getInsurancePurchaseSuccessTime();
        result = result * PRIME + ($insurancePurchaseSuccessTime == null ? 43 : $insurancePurchaseSuccessTime.hashCode());
        final Object $insurancePurchaseType = this.getInsurancePurchaseType();
        result = result * PRIME + ($insurancePurchaseType == null ? 43 : $insurancePurchaseType.hashCode());
        final Object $insuranceStatus = this.getInsuranceStatus();
        result = result * PRIME + ($insuranceStatus == null ? 43 : $insuranceStatus.hashCode());
        final Object $remarks = this.getRemarks();
        result = result * PRIME + ($remarks == null ? 43 : $remarks.hashCode());
        final Object $insuranceType = this.getInsuranceType();
        result = result * PRIME + ($insuranceType == null ? 43 : $insuranceType.hashCode());
        final Object $channelSource = this.getChannelSource();
        result = result * PRIME + ($channelSource == null ? 43 : $channelSource.hashCode());
        final Object $insurancePurchaseStatus = this.getInsurancePurchaseStatus();
        result = result * PRIME + ($insurancePurchaseStatus == null ? 43 : $insurancePurchaseStatus.hashCode());
        final Object $insurancePurchaseInitiationTime = this.getInsurancePurchaseInitiationTime();
        result = result * PRIME + ($insurancePurchaseInitiationTime == null ? 43 : $insurancePurchaseInitiationTime.hashCode());
        final Object $insurancePurchaseFailureTime = this.getInsurancePurchaseFailureTime();
        result = result * PRIME + ($insurancePurchaseFailureTime == null ? 43 : $insurancePurchaseFailureTime.hashCode());
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
        return "TOdsScInsuranceSaleDetail(id=" + this.getId() + ", orderNumber=" + this.getOrderNumber() + ", passengerName=" + this.getPassengerName() + ", bookingTime=" + this.getBookingTime() + ", bankOrderNumber=" + this.getBankOrderNumber() + ", bankName=" + this.getBankName() + ", pnr=" + this.getPnr() + ", ticketNumber=" + this.getTicketNumber() + ", ticketNature=" + this.getTicketNature() + ", accountUsername=" + this.getAccountUsername() + ", departureDate=" + this.getDepartureDate() + ", departureCity=" + this.getDepartureCity() + ", arrivalCity=" + this.getArrivalCity() + ", idType=" + this.getIdType() + ", idNumber=" + this.getIdNumber() + ", insurancePolicyNumber=" + this.getInsurancePolicyNumber() + ", insuranceFee=" + this.getInsuranceFee() + ", paymentTime=" + this.getPaymentTime() + ", insurancePurchaseSuccessTime=" + this.getInsurancePurchaseSuccessTime() + ", insurancePurchaseType=" + this.getInsurancePurchaseType() + ", insuranceStatus=" + this.getInsuranceStatus() + ", remarks=" + this.getRemarks() + ", insuranceType=" + this.getInsuranceType() + ", channelSource=" + this.getChannelSource() + ", insurancePurchaseStatus=" + this.getInsurancePurchaseStatus() + ", insurancePurchaseInitiationTime=" + this.getInsurancePurchaseInitiationTime() + ", insurancePurchaseFailureTime=" + this.getInsurancePurchaseFailureTime() + ", site=" + this.getSite() + ", currency=" + this.getCurrency() + ", airlineOrderNumber=" + this.getAirlineOrderNumber() + ", etlCreateTime=" + this.getEtlCreateTime() + ", etlUpdateTime=" + this.getEtlUpdateTime() + ", etlDate=" + this.getEtlDate() + ")";
    }
}