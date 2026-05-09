package com.travelsky.trp.usercenter.data.analysis.model.dwd.fact;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * 常客-里程累积-业务级mile_accrual_fact
 */
public class MileAccrualFactModal {
    @JsonProperty("PK_ID")
    private String pkId;

    @JsonProperty("MILEAGE_TYPE")
    private String mileageType;

    @JsonProperty("FILE_NAME")
    private String fileName;

    @JsonProperty("TOTAL_RECORDS")
    private Integer totalRecords;

    @JsonProperty("TOTAL_MILES")
    private BigDecimal totalMiles;

    @JsonProperty("TOTAL_AMOUNT")
    private BigDecimal totalAmount;

    @JsonProperty("BILLING_MONTH")
    private String billingMonth;

    @JsonProperty("BILLING_START_DATE")
    private String billingStartDate;

    @JsonProperty("BILLING_END_DATE")
    private String billingEndDate;

    @JsonProperty("MEMBER_NUMBER")
    private String memberNumber;

    @JsonProperty("MEMBER_LEVEL")
    private String memberLevel;

    @JsonProperty("MEMBER_BRAND")
    private String memberBrand;

    @JsonProperty("AK_BIZ_TYPE")
    private String akBizType;

    @JsonProperty("AK_BIZ_SUBTYPE")
    private String akBizSubtype;

    @JsonProperty("CHANNEL_CODE")
    private String channelCode;

    @JsonProperty("PARTNER_CODE")
    private String partnerCode;

    @JsonProperty("EVENT_NUMBER")
    private String eventNumber;

    @JsonProperty("ACTIVITY_ID")
    private String activityId;

    @JsonProperty("TRANSACTION_ID")
    private String transactionId;

    @JsonProperty("ORDER_NO")
    private String orderNo;

    @JsonProperty("TICKET_NO")
    private String ticketNo;

    @JsonProperty("COUPON_NO")
    private String couponNo;

    @JsonProperty("BILL_COMPANY")
    private String billCompany;

    @JsonProperty("BILLED_COMPANY")
    private String billedCompany;

    @JsonProperty("MILES_ACCUMULATED")
    private BigDecimal milesAccumulated;

    @JsonProperty("CURRENCY")
    private String currency;

    @JsonProperty("SALES_AMOUNT")
    private BigDecimal salesAmount;

    @JsonProperty("COST_AMOUNT")
    private BigDecimal costAmount;

    @JsonProperty("MILEAGE_VALUE")
    private BigDecimal mileageValue;

    @JsonProperty("EXCHANGE_POINT")
    private BigDecimal exchangePoint;

    @JsonProperty("FK_ACTIVITY_DATE")
    private String fkActivityDate;

    @JsonProperty("OC_CARRIER")
    private String ocCarrier;

    @JsonProperty("OC_FLIGHT_NO")
    private String ocFlightNo;

    @JsonProperty("OC_CABIN")
    private String ocCabin;

    @JsonProperty("OC_SUBCABIN")
    private String ocSubcabin;

    @JsonProperty("ORIGIN_STATION")
    private String originStation;

    @JsonProperty("DESTINATION_STATION")
    private String destinationStation;

    @JsonProperty("PROFIT_SHARING")
    private String profitSharing;

    @JsonProperty("INTERNAL_REF")
    private String internalRef;

    @JsonProperty("AUTHORIZATION_NUMBER")
    private String authorizationNumber;

    @JsonProperty("CUMULATION_TID")
    private String cumulationTid;

    @JsonProperty("TOTAL_COUNT")
    private Integer totalCount = 1;

    @JsonProperty("SYSTEM_CREATETIME")
    private String systemCreatetime;

    @JsonProperty("SYSTEM_LAST_UPDATETIME")
    private String systemLastUpdatetime = LocalDateTime.now().toString();

    // Getters and Setters
    public String getPkId() {
        return pkId;
    }

    public void setPkId(String pkId) {
        this.pkId = pkId;
    }

    public String getMileageType() {
        return mileageType;
    }

    public void setMileageType(String mileageType) {
        this.mileageType = mileageType;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public Integer getTotalRecords() {
        return totalRecords;
    }

    public void setTotalRecords(Integer totalRecords) {
        this.totalRecords = totalRecords;
    }

    public BigDecimal getTotalMiles() {
        return totalMiles;
    }

    public void setTotalMiles(BigDecimal totalMiles) {
        this.totalMiles = totalMiles;
    }

    public BigDecimal getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(BigDecimal totalAmount) {
        this.totalAmount = totalAmount;
    }

    public String getBillingMonth() {
        return billingMonth;
    }

    public void setBillingMonth(String billingMonth) {
        this.billingMonth = billingMonth;
    }

    public String getBillingStartDate() {
        return billingStartDate;
    }

    public void setBillingStartDate(String billingStartDate) {
        this.billingStartDate = billingStartDate;
    }

    public String getBillingEndDate() {
        return billingEndDate;
    }

    public void setBillingEndDate(String billingEndDate) {
        this.billingEndDate = billingEndDate;
    }

    public String getMemberNumber() {
        return memberNumber;
    }

    public void setMemberNumber(String memberNumber) {
        this.memberNumber = memberNumber;
    }

    public String getMemberLevel() {
        return memberLevel;
    }

    public void setMemberLevel(String memberLevel) {
        this.memberLevel = memberLevel;
    }

    public String getMemberBrand() {
        return memberBrand;
    }

    public void setMemberBrand(String memberBrand) {
        this.memberBrand = memberBrand;
    }

    public String getAkBizType() {
        return akBizType;
    }

    public void setAkBizType(String akBizType) {
        this.akBizType = akBizType;
    }

    public String getAkBizSubtype() {
        return akBizSubtype;
    }

    public void setAkBizSubtype(String akBizSubtype) {
        this.akBizSubtype = akBizSubtype;
    }

    public String getChannelCode() {
        return channelCode;
    }

    public void setChannelCode(String channelCode) {
        this.channelCode = channelCode;
    }

    public String getPartnerCode() {
        return partnerCode;
    }

    public void setPartnerCode(String partnerCode) {
        this.partnerCode = partnerCode;
    }

    public String getEventNumber() {
        return eventNumber;
    }

    public void setEventNumber(String eventNumber) {
        this.eventNumber = eventNumber;
    }

    public String getActivityId() {
        return activityId;
    }

    public void setActivityId(String activityId) {
        this.activityId = activityId;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getOrderNo() {
        return orderNo;
    }

    public void setOrderNo(String orderNo) {
        this.orderNo = orderNo;
    }

    public String getTicketNo() {
        return ticketNo;
    }

    public void setTicketNo(String ticketNo) {
        this.ticketNo = ticketNo;
    }

    public String getCouponNo() {
        return couponNo;
    }

    public void setCouponNo(String couponNo) {
        this.couponNo = couponNo;
    }

    public String getBillCompany() {
        return billCompany;
    }

    public void setBillCompany(String billCompany) {
        this.billCompany = billCompany;
    }

    public String getBilledCompany() {
        return billedCompany;
    }

    public void setBilledCompany(String billedCompany) {
        this.billedCompany = billedCompany;
    }

    public BigDecimal getMilesAccumulated() {
        return milesAccumulated;
    }

    public void setMilesAccumulated(BigDecimal milesAccumulated) {
        this.milesAccumulated = milesAccumulated;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public BigDecimal getSalesAmount() {
        return salesAmount;
    }

    public void setSalesAmount(BigDecimal salesAmount) {
        this.salesAmount = salesAmount;
    }

    public BigDecimal getCostAmount() {
        return costAmount;
    }

    public void setCostAmount(BigDecimal costAmount) {
        this.costAmount = costAmount;
    }

    public BigDecimal getMileageValue() {
        return mileageValue;
    }

    public void setMileageValue(BigDecimal mileageValue) {
        this.mileageValue = mileageValue;
    }

    public BigDecimal getExchangePoint() {
        return exchangePoint;
    }

    public void setExchangePoint(BigDecimal exchangePoint) {
        this.exchangePoint = exchangePoint;
    }

    public String getFkActivityDate() {
        return fkActivityDate;
    }

    public void setFkActivityDate(String fkActivityDate) {
        this.fkActivityDate = fkActivityDate;
    }

    public String getOcCarrier() {
        return ocCarrier;
    }

    public void setOcCarrier(String ocCarrier) {
        this.ocCarrier = ocCarrier;
    }

    public String getOcFlightNo() {
        return ocFlightNo;
    }

    public void setOcFlightNo(String ocFlightNo) {
        this.ocFlightNo = ocFlightNo;
    }

    public String getOcCabin() {
        return ocCabin;
    }

    public void setOcCabin(String ocCabin) {
        this.ocCabin = ocCabin;
    }

    public String getOcSubcabin() {
        return ocSubcabin;
    }

    public void setOcSubcabin(String ocSubcabin) {
        this.ocSubcabin = ocSubcabin;
    }

    public String getOriginStation() {
        return originStation;
    }

    public void setOriginStation(String originStation) {
        this.originStation = originStation;
    }

    public String getDestinationStation() {
        return destinationStation;
    }

    public void setDestinationStation(String destinationStation) {
        this.destinationStation = destinationStation;
    }

    public String getProfitSharing() {
        return profitSharing;
    }

    public void setProfitSharing(String profitSharing) {
        this.profitSharing = profitSharing;
    }

    public String getInternalRef() {
        return internalRef;
    }

    public void setInternalRef(String internalRef) {
        this.internalRef = internalRef;
    }

    public String getAuthorizationNumber() {
        return authorizationNumber;
    }

    public void setAuthorizationNumber(String authorizationNumber) {
        this.authorizationNumber = authorizationNumber;
    }

    public Integer getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(Integer totalCount) {
        this.totalCount = totalCount;
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

    public String getCumulationTid() {
        return cumulationTid;
    }

    public void setCumulationTid(String cumulationTid) {
        this.cumulationTid = cumulationTid;
    }

    @Override
    public String toString() {
        return "MileAccrualFactModal{" +
                "pkId='" + pkId + '\'' +
                ", mileageType='" + mileageType + '\'' +
                ", fileName='" + fileName + '\'' +
                ", totalRecords=" + totalRecords +
                ", totalMiles=" + totalMiles +
                ", totalAmount=" + totalAmount +
                ", billingMonth='" + billingMonth + '\'' +
                ", billingStartDate='" + billingStartDate + '\'' +
                ", billingEndDate='" + billingEndDate + '\'' +
                ", memberNumber='" + memberNumber + '\'' +
                ", memberLevel='" + memberLevel + '\'' +
                ", memberBrand='" + memberBrand + '\'' +
                ", akBizType='" + akBizType + '\'' +
                ", akBizSubtype='" + akBizSubtype + '\'' +
                ", channelCode='" + channelCode + '\'' +
                ", partnerCode='" + partnerCode + '\'' +
                ", eventNumber='" + eventNumber + '\'' +
                ", activityId='" + activityId + '\'' +
                ", transactionId='" + transactionId + '\'' +
                ", orderNo='" + orderNo + '\'' +
                ", ticketNo='" + ticketNo + '\'' +
                ", couponNo='" + couponNo + '\'' +
                ", billCompany='" + billCompany + '\'' +
                ", billedCompany='" + billedCompany + '\'' +
                ", milesAccumulated=" + milesAccumulated +
                ", currency='" + currency + '\'' +
                ", salesAmount=" + salesAmount +
                ", costAmount=" + costAmount +
                ", mileageValue=" + mileageValue +
                ", exchangePoint=" + exchangePoint +
                ", fkActivityDate='" + fkActivityDate + '\'' +
                ", ocCarrier='" + ocCarrier + '\'' +
                ", ocFlightNo='" + ocFlightNo + '\'' +
                ", ocCabin='" + ocCabin + '\'' +
                ", ocSubcabin='" + ocSubcabin + '\'' +
                ", originStation='" + originStation + '\'' +
                ", destinationStation='" + destinationStation + '\'' +
                ", profitSharing='" + profitSharing + '\'' +
                ", internalRef='" + internalRef + '\'' +
                ", authorizationNumber='" + authorizationNumber + '\'' +
                ", cumulationTid='" + cumulationTid + '\'' +
                ", totalCount=" + totalCount +
                ", systemCreatetime='" + systemCreatetime + '\'' +
                ", systemLastUpdatetime='" + systemLastUpdatetime + '\'' +
                '}';
    }
}
