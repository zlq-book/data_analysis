package com.travelsky.trp.usercenter.data.analysis.model.dwd.fact;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * 常客-里程兑换-业务级
 */
public class MileRedemptionFactModal {

    @JsonProperty("PK_ID")
    private String pkId; // 主键

    @JsonProperty("MILEAGE_TYPE")
    private String mileageType; // 里程累积类型

    @JsonProperty("FILE_NAME")
    private String fileName; // 文件名

    @JsonProperty("TOTAL_RECORDS")
    private Integer totalRecords; // 总记录数

    @JsonProperty("TOTAL_MILES")
    private BigDecimal totalMiles; // 总里程数

    @JsonProperty("TOTAL_AMOUNT")
    private BigDecimal totalAmount; // 总金额

    @JsonProperty("BILLING_MONTH")
    private String billingMonth; // 结算月

    @JsonProperty("BILLING_START_DATE")
    private String billingStartDate; // 结算开始日期

    @JsonProperty("BILLING_END_DATE")
    private String billingEndDate; // 结算结束日期

    @JsonProperty("MEMBER_NUMBER")
    private String memberNumber; // 常旅客编号

    @JsonProperty("MEMBER_LEVEL")
    private String memberLevel; // 常旅客会员级别代码

    @JsonProperty("MEMBER_BRAND")
    private String memberBrand; // 会员品牌

    @JsonProperty("BIZ_TYPE")
    private String bizType; // 兑换类型代码（一级分类）

    @JsonProperty("BIZ_SUBTYPE")
    private String bizSubtype; // 兑换类型代码（二级分类）

    @JsonProperty("CHANNEL_CODE")
    private String channelCode; // 兑换渠道代码

    @JsonProperty("PARTNER_CODE")
    private String partnerCode; // 合作伙伴代码

    @JsonProperty("EVENT_NUMBER")
    private String eventNumber; // 兑换事件号

    @JsonProperty("ACTIVITY_ID")
    private String activityId; // 兑换流水号

    @JsonProperty("TRANSACTION_ID")
    private String transactionId; // 交易ID

    @JsonProperty("ORDER_NO")
    private String orderNo; // 订单号

    @JsonProperty("TICKET_NO")
    private String ticketNo; // 机票票号

    @JsonProperty("COUPON_NO")
    private String couponNo; // 机票票联号

    @JsonProperty("BILL_COMPANY")
    private String billCompany; // 开账公司

    @JsonProperty("BILLED_COMPANY")
    private String billedCompany; // 被开账公司

    @JsonProperty("MILES_ACCUMULATED")
    private BigDecimal milesAccumulated; // 里程数

    @JsonProperty("MILEAGE_VALUE")
    private BigDecimal mileageValue; // 里程数金额

    @JsonProperty("TPM")
    private String tpm; // TPM

    @JsonProperty("CURRENCY")
    private String currency; // 本位币

    @JsonProperty("SALES_AMOUNT")
    private BigDecimal salesAmount; // 销售金额

    @JsonProperty("COST_AMOUNT")
    private BigDecimal costAmount; // 成本金额

    @JsonProperty("PNR")
    private String pnr; // PNR编号

    @JsonProperty("EXCH_DATE")
    private String exchDate; // 兑换日期

    @JsonProperty("FLIGHT_DATE")
    private String flightDate; // 飞行日期

    @JsonProperty("OC_CARRIER")
    private String ocCarrier; // OC实际承运人

    @JsonProperty("OC_FLIGHT_NO")
    private String ocFlightNo; // OC实际航班号

    @JsonProperty("CABIN_CLASS")
    private String cabinClass; // 舱等

    @JsonProperty("OC_CABIN")
    private String ocCabin; // OC舱位(大舱位)

    @JsonProperty("OC_SUBCABIN")
    private String ocSubcabin; // OC实际累积舱位（子舱位）

    @JsonProperty("ORIGINAL_SUBCABIN")
    private String originalSubcabin; // 升舱前子舱位

    @JsonProperty("ORIGIN_STATION")
    private String originStation; // 始发地

    @JsonProperty("DESTINATION_STATION")
    private String destinationStation; // 目的地

    @JsonProperty("PRODUCT_ID")
    private String productId; // 商品编号

    @JsonProperty("PRODUCT_INFO")
    private String productInfo; // 兑换产品描述

    @JsonProperty("PROFIT_SHARING")
    private String profitSharing; // 分润标识

    @JsonProperty("SYSTEM_CREATETIME")
    private String systemCreatetime; // 本系统创建日期时间

    @JsonProperty("SYSTEM_LAST_UPDATETIME")
    private String systemLastUpdatetime = LocalDateTime.now().toString(); // 本系统最后更新日期时间

    @JsonProperty("EXCH_COUNT")
    private Integer exchCount = 1; // 兑换次数

    @JsonProperty("CUMULATION_TID")
    private String cumulationTid;


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

    public String getBizType() {
        return bizType;
    }

    public void setBizType(String bizType) {
        this.bizType = bizType;
    }

    public String getBizSubtype() {
        return bizSubtype;
    }

    public void setBizSubtype(String bizSubtype) {
        this.bizSubtype = bizSubtype;
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

    public BigDecimal getMileageValue() {
        return mileageValue;
    }

    public void setMileageValue(BigDecimal mileageValue) {
        this.mileageValue = mileageValue;
    }

    public String getTpm() {
        return tpm;
    }

    public void setTpm(String tpm) {
        this.tpm = tpm;
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

    public String getPnr() {
        return pnr;
    }

    public void setPnr(String pnr) {
        this.pnr = pnr;
    }

    public String getExchDate() {
        return exchDate;
    }

    public void setExchDate(String exchDate) {
        this.exchDate = exchDate;
    }

    public String getFlightDate() {
        return flightDate;
    }

    public void setFlightDate(String flightDate) {
        this.flightDate = flightDate;
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

    public String getCabinClass() {
        return cabinClass;
    }

    public void setCabinClass(String cabinClass) {
        this.cabinClass = cabinClass;
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

    public String getOriginalSubcabin() {
        return originalSubcabin;
    }

    public void setOriginalSubcabin(String originalSubcabin) {
        this.originalSubcabin = originalSubcabin;
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

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getProductInfo() {
        return productInfo;
    }

    public void setProductInfo(String productInfo) {
        this.productInfo = productInfo;
    }

    public String getProfitSharing() {
        return profitSharing;
    }

    public void setProfitSharing(String profitSharing) {
        this.profitSharing = profitSharing;
    }

    public String getSystemLastUpdatetime() {
        return systemLastUpdatetime;
    }

    public void setSystemLastUpdatetime(String systemLastUpdatetime) {
        this.systemLastUpdatetime = systemLastUpdatetime;
    }

    public Integer getExchCount() {
        return exchCount;
    }

    public void setExchCount(Integer exchCount) {
        this.exchCount = exchCount;
    }


    public String getCumulationTid() {
        return cumulationTid;
    }

    public void setCumulationTid(String cumulationTid) {
        this.cumulationTid = cumulationTid;
    }

    public String getSystemCreatetime() {
        return systemCreatetime;
    }

    public void setSystemCreatetime(String systemCreatetime) {
        this.systemCreatetime = systemCreatetime;
    }

    @Override
    public String toString() {
        return "MileRedemptionFactModal{" +
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
                ", bizType='" + bizType + '\'' +
                ", bizSubtype='" + bizSubtype + '\'' +
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
                ", mileageValue=" + mileageValue +
                ", tpm='" + tpm + '\'' +
                ", currency='" + currency + '\'' +
                ", salesAmount=" + salesAmount +
                ", costAmount=" + costAmount +
                ", pnr='" + pnr + '\'' +
                ", exchDate='" + exchDate + '\'' +
                ", flightDate='" + flightDate + '\'' +
                ", ocCarrier='" + ocCarrier + '\'' +
                ", ocFlightNo='" + ocFlightNo + '\'' +
                ", cabinClass='" + cabinClass + '\'' +
                ", ocCabin='" + ocCabin + '\'' +
                ", ocSubcabin='" + ocSubcabin + '\'' +
                ", originalSubcabin='" + originalSubcabin + '\'' +
                ", originStation='" + originStation + '\'' +
                ", destinationStation='" + destinationStation + '\'' +
                ", productId='" + productId + '\'' +
                ", productInfo='" + productInfo + '\'' +
                ", profitSharing='" + profitSharing + '\'' +
                ", systemCreatetime='" + systemCreatetime + '\'' +
                ", systemLastUpdatetime='" + systemLastUpdatetime + '\'' +
                ", exchCount=" + exchCount +
                ", cumulationTid='" + cumulationTid + '\'' +
                '}';
    }
}
