package com.travelsky.dataplatform.module.ods.trp;

import com.alibaba.excel.annotation.ExcelIgnore;
import com.alibaba.excel.annotation.ExcelProperty;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * SC 销售报表明细 实体
 */
public class TOdsScSaleDetail implements Serializable {

    private static final long serialVersionUID = 1L;
    /**
     * 主键ID
     */
    @ExcelProperty("序号")
    private Long id;

    @ExcelProperty("订单号")
    private String orderNumber;

    @ExcelProperty("银行订单号")
    private String bankOrderNumber;

    @ExcelProperty("PNR")
    private String pnr;

    @ExcelProperty("票号")
    private String ticketNumber;

    @ExcelProperty("航班号")
    private String flightNumber;

    @ExcelProperty("航段三字码")
    private String segmentThreeCode;

    @ExcelProperty("航段")
    private String segment;

    @ExcelProperty("舱位")
    private String cabin;

    @ExcelProperty("起飞时间")
    private String departureTime;

    @ExcelProperty("乘机人姓名")
    private String passengerName;

    @ExcelProperty("优惠前票面价格")
    private BigDecimal priceBeforeDiscount;

    @ExcelProperty("优惠后票面价格")
    private BigDecimal priceAfterDiscount;

    @ExcelProperty("机建")
    private BigDecimal airportConstructionFee;

    @ExcelProperty("燃油附加税")
    private BigDecimal fuelSurcharge;

    @ExcelProperty("其他税")
    private BigDecimal otherTax;

    @ExcelProperty("保险费用")
    private BigDecimal insuranceFee;

    @ExcelProperty("应付金额")
    private BigDecimal amountPayable;

    @ExcelProperty("产品名称")
    private String productName;

    @ExcelProperty("订票日期")
    private String bookingDate;

    @ExcelProperty("出票日期")
    private String ticketingDate;

    @ExcelProperty("订票状态")
    private String bookingStatus;

    @ExcelProperty("订单来源")
    private String orderSource;

    @ExcelProperty("订单渠道")
    private String orderChannel;

    @ExcelProperty("订票用户名")
    private String bookingUsername;

    @ExcelProperty("注册用户姓名")
    private String registeredUserName;

    @ExcelProperty("订票人手机号")
    private String bookingUserPhone;

    @ExcelProperty("订票人邮箱")
    private String bookingUserEmail;

    @ExcelProperty("银行名称")
    private String bankName;

    @ExcelProperty("实际承运人")
    private String actualCarrier;

    @ExcelProperty("大客户号")
    private String bigCustomerNumber;

    @ExcelProperty("常旅客卡号")
    private String frequentFlyerCardNo;

    @ExcelProperty("支付时间")
    private String paymentTime;

    @ExcelProperty("购票流程")
    private String purchaseProcess;

    @ExcelProperty("乘客类型")
    private String passengerType;

    @ExcelProperty("证件类型")
    private String idType;

    @ExcelProperty("证件号码")
    private String idNumber;

    @ExcelProperty("行程类型")
    private String itineraryType;

    @ExcelProperty("客票性质")
    private String ticketNature;

    @ExcelProperty("是否直减")
    private String isDirectDiscount;

    @ExcelProperty("联系人姓名")
    private String contactName;

    @ExcelProperty("联系人手机号")
    private String contactPhone;

    @ExcelProperty("站点")
    private String site;

    @ExcelProperty("语言")
    private String language;

    @ExcelProperty("币种")
    private String currency;

    @ExcelProperty("卡券名称")
    private String couponName;

    @ExcelProperty("卡券金额")
    private BigDecimal couponAmount;

    @ExcelProperty("卡券code")
    private String couponCode;

    @ExcelProperty("券码")
    private String voucherCode;

    @ExcelProperty("直减金额")
    private BigDecimal directDiscountAmount;

    @ExcelProperty("静态促销直减金额")
    private BigDecimal staticPromotionDirectDiscountAmount;

    @ExcelProperty("是否静态促销直减")
    private String isStaticPromotionDirectDiscount;

    @ExcelProperty("FareBasis")
    private String fareBasis;

    @ExcelProperty("是否赠送保险")
    private String isGiftInsurance;

    @ExcelProperty("赠送保险名称")
    private String giftInsuranceName;

    @ExcelProperty("是否赠送卡劵")
    private String isGiftCoupon;

    @ExcelProperty("是否为会员专享直减")
    private String isMemberExclusiveDirectDiscount;

    @ExcelProperty("是否为实名用户专享直减")
    private String isRealNameExclusiveDirectDiscount;

    @ExcelProperty("航空公司")
    private String airline;

    @ExcelProperty("里程抵扣金额")
    private BigDecimal mileageDeductionAmount;

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

    public TOdsScSaleDetail() {
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

    public String getPnr() {
        return this.pnr;
    }

    public String getTicketNumber() {
        return this.ticketNumber;
    }

    public String getFlightNumber() {
        return this.flightNumber;
    }

    public String getSegmentThreeCode() {
        return this.segmentThreeCode;
    }

    public String getSegment() {
        return this.segment;
    }

    public String getCabin() {
        return this.cabin;
    }

    public String getDepartureTime() {
        return this.departureTime;
    }

    public String getPassengerName() {
        return this.passengerName;
    }

    public BigDecimal getPriceBeforeDiscount() {
        return this.priceBeforeDiscount;
    }

    public BigDecimal getPriceAfterDiscount() {
        return this.priceAfterDiscount;
    }

    public BigDecimal getAirportConstructionFee() {
        return this.airportConstructionFee;
    }

    public BigDecimal getFuelSurcharge() {
        return this.fuelSurcharge;
    }

    public BigDecimal getOtherTax() {
        return this.otherTax;
    }

    public BigDecimal getInsuranceFee() {
        return this.insuranceFee;
    }

    public BigDecimal getAmountPayable() {
        return this.amountPayable;
    }

    public String getProductName() {
        return this.productName;
    }

    public String getBookingDate() {
        return this.bookingDate;
    }

    public String getTicketingDate() {
        return this.ticketingDate;
    }

    public String getBookingStatus() {
        return this.bookingStatus;
    }

    public String getOrderSource() {
        return this.orderSource;
    }

    public String getOrderChannel() {
        return this.orderChannel;
    }

    public String getBookingUsername() {
        return this.bookingUsername;
    }

    public String getRegisteredUserName() {
        return this.registeredUserName;
    }

    public String getBookingUserPhone() {
        return this.bookingUserPhone;
    }

    public String getBookingUserEmail() {
        return this.bookingUserEmail;
    }

    public String getBankName() {
        return this.bankName;
    }

    public String getActualCarrier() {
        return this.actualCarrier;
    }

    public String getBigCustomerNumber() {
        return this.bigCustomerNumber;
    }

    public String getFrequentFlyerCardNo() {
        return this.frequentFlyerCardNo;
    }

    public String getPaymentTime() {
        return this.paymentTime;
    }

    public String getPurchaseProcess() {
        return this.purchaseProcess;
    }

    public String getPassengerType() {
        return this.passengerType;
    }

    public String getIdType() {
        return this.idType;
    }

    public String getIdNumber() {
        return this.idNumber;
    }

    public String getItineraryType() {
        return this.itineraryType;
    }

    public String getTicketNature() {
        return this.ticketNature;
    }

    public String getIsDirectDiscount() {
        return this.isDirectDiscount;
    }

    public String getContactName() {
        return this.contactName;
    }

    public String getContactPhone() {
        return this.contactPhone;
    }

    public String getSite() {
        return this.site;
    }

    public String getLanguage() {
        return this.language;
    }

    public String getCurrency() {
        return this.currency;
    }

    public String getCouponName() {
        return this.couponName;
    }

    public BigDecimal getCouponAmount() {
        return this.couponAmount;
    }

    public String getCouponCode() {
        return this.couponCode;
    }

    public String getVoucherCode() {
        return this.voucherCode;
    }

    public BigDecimal getDirectDiscountAmount() {
        return this.directDiscountAmount;
    }

    public BigDecimal getStaticPromotionDirectDiscountAmount() {
        return this.staticPromotionDirectDiscountAmount;
    }

    public String getIsStaticPromotionDirectDiscount() {
        return this.isStaticPromotionDirectDiscount;
    }

    public String getFareBasis() {
        return this.fareBasis;
    }

    public String getIsGiftInsurance() {
        return this.isGiftInsurance;
    }

    public String getGiftInsuranceName() {
        return this.giftInsuranceName;
    }

    public String getIsGiftCoupon() {
        return this.isGiftCoupon;
    }

    public String getIsMemberExclusiveDirectDiscount() {
        return this.isMemberExclusiveDirectDiscount;
    }

    public String getIsRealNameExclusiveDirectDiscount() {
        return this.isRealNameExclusiveDirectDiscount;
    }

    public String getAirline() {
        return this.airline;
    }

    public BigDecimal getMileageDeductionAmount() {
        return this.mileageDeductionAmount;
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

    public void setPnr(String pnr) {
        this.pnr = pnr;
    }

    public void setTicketNumber(String ticketNumber) {
        this.ticketNumber = ticketNumber;
    }

    public void setFlightNumber(String flightNumber) {
        this.flightNumber = flightNumber;
    }

    public void setSegmentThreeCode(String segmentThreeCode) {
        this.segmentThreeCode = segmentThreeCode;
    }

    public void setSegment(String segment) {
        this.segment = segment;
    }

    public void setCabin(String cabin) {
        this.cabin = cabin;
    }

    public void setDepartureTime(String departureTime) {
        this.departureTime = departureTime;
    }

    public void setPassengerName(String passengerName) {
        this.passengerName = passengerName;
    }

    public void setPriceBeforeDiscount(BigDecimal priceBeforeDiscount) {
        this.priceBeforeDiscount = priceBeforeDiscount;
    }

    public void setPriceAfterDiscount(BigDecimal priceAfterDiscount) {
        this.priceAfterDiscount = priceAfterDiscount;
    }

    public void setAirportConstructionFee(BigDecimal airportConstructionFee) {
        this.airportConstructionFee = airportConstructionFee;
    }

    public void setFuelSurcharge(BigDecimal fuelSurcharge) {
        this.fuelSurcharge = fuelSurcharge;
    }

    public void setOtherTax(BigDecimal otherTax) {
        this.otherTax = otherTax;
    }

    public void setInsuranceFee(BigDecimal insuranceFee) {
        this.insuranceFee = insuranceFee;
    }

    public void setAmountPayable(BigDecimal amountPayable) {
        this.amountPayable = amountPayable;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public void setBookingDate(String bookingDate) {
        this.bookingDate = bookingDate;
    }

    public void setTicketingDate(String ticketingDate) {
        this.ticketingDate = ticketingDate;
    }

    public void setBookingStatus(String bookingStatus) {
        this.bookingStatus = bookingStatus;
    }

    public void setOrderSource(String orderSource) {
        this.orderSource = orderSource;
    }

    public void setOrderChannel(String orderChannel) {
        this.orderChannel = orderChannel;
    }

    public void setBookingUsername(String bookingUsername) {
        this.bookingUsername = bookingUsername;
    }

    public void setRegisteredUserName(String registeredUserName) {
        this.registeredUserName = registeredUserName;
    }

    public void setBookingUserPhone(String bookingUserPhone) {
        this.bookingUserPhone = bookingUserPhone;
    }

    public void setBookingUserEmail(String bookingUserEmail) {
        this.bookingUserEmail = bookingUserEmail;
    }

    public void setBankName(String bankName) {
        this.bankName = bankName;
    }

    public void setActualCarrier(String actualCarrier) {
        this.actualCarrier = actualCarrier;
    }

    public void setBigCustomerNumber(String bigCustomerNumber) {
        this.bigCustomerNumber = bigCustomerNumber;
    }

    public void setFrequentFlyerCardNo(String frequentFlyerCardNo) {
        this.frequentFlyerCardNo = frequentFlyerCardNo;
    }

    public void setPaymentTime(String paymentTime) {
        this.paymentTime = paymentTime;
    }

    public void setPurchaseProcess(String purchaseProcess) {
        this.purchaseProcess = purchaseProcess;
    }

    public void setPassengerType(String passengerType) {
        this.passengerType = passengerType;
    }

    public void setIdType(String idType) {
        this.idType = idType;
    }

    public void setIdNumber(String idNumber) {
        this.idNumber = idNumber;
    }

    public void setItineraryType(String itineraryType) {
        this.itineraryType = itineraryType;
    }

    public void setTicketNature(String ticketNature) {
        this.ticketNature = ticketNature;
    }

    public void setIsDirectDiscount(String isDirectDiscount) {
        this.isDirectDiscount = isDirectDiscount;
    }

    public void setContactName(String contactName) {
        this.contactName = contactName;
    }

    public void setContactPhone(String contactPhone) {
        this.contactPhone = contactPhone;
    }

    public void setSite(String site) {
        this.site = site;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public void setCouponName(String couponName) {
        this.couponName = couponName;
    }

    public void setCouponAmount(BigDecimal couponAmount) {
        this.couponAmount = couponAmount;
    }

    public void setCouponCode(String couponCode) {
        this.couponCode = couponCode;
    }

    public void setVoucherCode(String voucherCode) {
        this.voucherCode = voucherCode;
    }

    public void setDirectDiscountAmount(BigDecimal directDiscountAmount) {
        this.directDiscountAmount = directDiscountAmount;
    }

    public void setStaticPromotionDirectDiscountAmount(BigDecimal staticPromotionDirectDiscountAmount) {
        this.staticPromotionDirectDiscountAmount = staticPromotionDirectDiscountAmount;
    }

    public void setIsStaticPromotionDirectDiscount(String isStaticPromotionDirectDiscount) {
        this.isStaticPromotionDirectDiscount = isStaticPromotionDirectDiscount;
    }

    public void setFareBasis(String fareBasis) {
        this.fareBasis = fareBasis;
    }

    public void setIsGiftInsurance(String isGiftInsurance) {
        this.isGiftInsurance = isGiftInsurance;
    }

    public void setGiftInsuranceName(String giftInsuranceName) {
        this.giftInsuranceName = giftInsuranceName;
    }

    public void setIsGiftCoupon(String isGiftCoupon) {
        this.isGiftCoupon = isGiftCoupon;
    }

    public void setIsMemberExclusiveDirectDiscount(String isMemberExclusiveDirectDiscount) {
        this.isMemberExclusiveDirectDiscount = isMemberExclusiveDirectDiscount;
    }

    public void setIsRealNameExclusiveDirectDiscount(String isRealNameExclusiveDirectDiscount) {
        this.isRealNameExclusiveDirectDiscount = isRealNameExclusiveDirectDiscount;
    }

    public void setAirline(String airline) {
        this.airline = airline;
    }

    public void setMileageDeductionAmount(BigDecimal mileageDeductionAmount) {
        this.mileageDeductionAmount = mileageDeductionAmount;
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
        if (!(o instanceof TOdsScSaleDetail)) return false;
        final TOdsScSaleDetail other = (TOdsScSaleDetail) o;
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
        final Object this$pnr = this.getPnr();
        final Object other$pnr = other.getPnr();
        if (this$pnr == null ? other$pnr != null : !this$pnr.equals(other$pnr)) return false;
        final Object this$ticketNumber = this.getTicketNumber();
        final Object other$ticketNumber = other.getTicketNumber();
        if (this$ticketNumber == null ? other$ticketNumber != null : !this$ticketNumber.equals(other$ticketNumber))
            return false;
        final Object this$flightNumber = this.getFlightNumber();
        final Object other$flightNumber = other.getFlightNumber();
        if (this$flightNumber == null ? other$flightNumber != null : !this$flightNumber.equals(other$flightNumber))
            return false;
        final Object this$segmentThreeCode = this.getSegmentThreeCode();
        final Object other$segmentThreeCode = other.getSegmentThreeCode();
        if (this$segmentThreeCode == null ? other$segmentThreeCode != null : !this$segmentThreeCode.equals(other$segmentThreeCode))
            return false;
        final Object this$segment = this.getSegment();
        final Object other$segment = other.getSegment();
        if (this$segment == null ? other$segment != null : !this$segment.equals(other$segment)) return false;
        final Object this$cabin = this.getCabin();
        final Object other$cabin = other.getCabin();
        if (this$cabin == null ? other$cabin != null : !this$cabin.equals(other$cabin)) return false;
        final Object this$departureTime = this.getDepartureTime();
        final Object other$departureTime = other.getDepartureTime();
        if (this$departureTime == null ? other$departureTime != null : !this$departureTime.equals(other$departureTime))
            return false;
        final Object this$passengerName = this.getPassengerName();
        final Object other$passengerName = other.getPassengerName();
        if (this$passengerName == null ? other$passengerName != null : !this$passengerName.equals(other$passengerName))
            return false;
        final Object this$priceBeforeDiscount = this.getPriceBeforeDiscount();
        final Object other$priceBeforeDiscount = other.getPriceBeforeDiscount();
        if (this$priceBeforeDiscount == null ? other$priceBeforeDiscount != null : !this$priceBeforeDiscount.equals(other$priceBeforeDiscount))
            return false;
        final Object this$priceAfterDiscount = this.getPriceAfterDiscount();
        final Object other$priceAfterDiscount = other.getPriceAfterDiscount();
        if (this$priceAfterDiscount == null ? other$priceAfterDiscount != null : !this$priceAfterDiscount.equals(other$priceAfterDiscount))
            return false;
        final Object this$airportConstructionFee = this.getAirportConstructionFee();
        final Object other$airportConstructionFee = other.getAirportConstructionFee();
        if (this$airportConstructionFee == null ? other$airportConstructionFee != null : !this$airportConstructionFee.equals(other$airportConstructionFee))
            return false;
        final Object this$fuelSurcharge = this.getFuelSurcharge();
        final Object other$fuelSurcharge = other.getFuelSurcharge();
        if (this$fuelSurcharge == null ? other$fuelSurcharge != null : !this$fuelSurcharge.equals(other$fuelSurcharge))
            return false;
        final Object this$otherTax = this.getOtherTax();
        final Object other$otherTax = other.getOtherTax();
        if (this$otherTax == null ? other$otherTax != null : !this$otherTax.equals(other$otherTax)) return false;
        final Object this$insuranceFee = this.getInsuranceFee();
        final Object other$insuranceFee = other.getInsuranceFee();
        if (this$insuranceFee == null ? other$insuranceFee != null : !this$insuranceFee.equals(other$insuranceFee))
            return false;
        final Object this$amountPayable = this.getAmountPayable();
        final Object other$amountPayable = other.getAmountPayable();
        if (this$amountPayable == null ? other$amountPayable != null : !this$amountPayable.equals(other$amountPayable))
            return false;
        final Object this$productName = this.getProductName();
        final Object other$productName = other.getProductName();
        if (this$productName == null ? other$productName != null : !this$productName.equals(other$productName))
            return false;
        final Object this$bookingDate = this.getBookingDate();
        final Object other$bookingDate = other.getBookingDate();
        if (this$bookingDate == null ? other$bookingDate != null : !this$bookingDate.equals(other$bookingDate))
            return false;
        final Object this$ticketingDate = this.getTicketingDate();
        final Object other$ticketingDate = other.getTicketingDate();
        if (this$ticketingDate == null ? other$ticketingDate != null : !this$ticketingDate.equals(other$ticketingDate))
            return false;
        final Object this$bookingStatus = this.getBookingStatus();
        final Object other$bookingStatus = other.getBookingStatus();
        if (this$bookingStatus == null ? other$bookingStatus != null : !this$bookingStatus.equals(other$bookingStatus))
            return false;
        final Object this$orderSource = this.getOrderSource();
        final Object other$orderSource = other.getOrderSource();
        if (this$orderSource == null ? other$orderSource != null : !this$orderSource.equals(other$orderSource))
            return false;
        final Object this$orderChannel = this.getOrderChannel();
        final Object other$orderChannel = other.getOrderChannel();
        if (this$orderChannel == null ? other$orderChannel != null : !this$orderChannel.equals(other$orderChannel))
            return false;
        final Object this$bookingUsername = this.getBookingUsername();
        final Object other$bookingUsername = other.getBookingUsername();
        if (this$bookingUsername == null ? other$bookingUsername != null : !this$bookingUsername.equals(other$bookingUsername))
            return false;
        final Object this$registeredUserName = this.getRegisteredUserName();
        final Object other$registeredUserName = other.getRegisteredUserName();
        if (this$registeredUserName == null ? other$registeredUserName != null : !this$registeredUserName.equals(other$registeredUserName))
            return false;
        final Object this$bookingUserPhone = this.getBookingUserPhone();
        final Object other$bookingUserPhone = other.getBookingUserPhone();
        if (this$bookingUserPhone == null ? other$bookingUserPhone != null : !this$bookingUserPhone.equals(other$bookingUserPhone))
            return false;
        final Object this$bookingUserEmail = this.getBookingUserEmail();
        final Object other$bookingUserEmail = other.getBookingUserEmail();
        if (this$bookingUserEmail == null ? other$bookingUserEmail != null : !this$bookingUserEmail.equals(other$bookingUserEmail))
            return false;
        final Object this$bankName = this.getBankName();
        final Object other$bankName = other.getBankName();
        if (this$bankName == null ? other$bankName != null : !this$bankName.equals(other$bankName)) return false;
        final Object this$actualCarrier = this.getActualCarrier();
        final Object other$actualCarrier = other.getActualCarrier();
        if (this$actualCarrier == null ? other$actualCarrier != null : !this$actualCarrier.equals(other$actualCarrier))
            return false;
        final Object this$bigCustomerNumber = this.getBigCustomerNumber();
        final Object other$bigCustomerNumber = other.getBigCustomerNumber();
        if (this$bigCustomerNumber == null ? other$bigCustomerNumber != null : !this$bigCustomerNumber.equals(other$bigCustomerNumber))
            return false;
        final Object this$frequentFlyerCardNo = this.getFrequentFlyerCardNo();
        final Object other$frequentFlyerCardNo = other.getFrequentFlyerCardNo();
        if (this$frequentFlyerCardNo == null ? other$frequentFlyerCardNo != null : !this$frequentFlyerCardNo.equals(other$frequentFlyerCardNo))
            return false;
        final Object this$paymentTime = this.getPaymentTime();
        final Object other$paymentTime = other.getPaymentTime();
        if (this$paymentTime == null ? other$paymentTime != null : !this$paymentTime.equals(other$paymentTime))
            return false;
        final Object this$purchaseProcess = this.getPurchaseProcess();
        final Object other$purchaseProcess = other.getPurchaseProcess();
        if (this$purchaseProcess == null ? other$purchaseProcess != null : !this$purchaseProcess.equals(other$purchaseProcess))
            return false;
        final Object this$passengerType = this.getPassengerType();
        final Object other$passengerType = other.getPassengerType();
        if (this$passengerType == null ? other$passengerType != null : !this$passengerType.equals(other$passengerType))
            return false;
        final Object this$idType = this.getIdType();
        final Object other$idType = other.getIdType();
        if (this$idType == null ? other$idType != null : !this$idType.equals(other$idType)) return false;
        final Object this$idNumber = this.getIdNumber();
        final Object other$idNumber = other.getIdNumber();
        if (this$idNumber == null ? other$idNumber != null : !this$idNumber.equals(other$idNumber)) return false;
        final Object this$itineraryType = this.getItineraryType();
        final Object other$itineraryType = other.getItineraryType();
        if (this$itineraryType == null ? other$itineraryType != null : !this$itineraryType.equals(other$itineraryType))
            return false;
        final Object this$ticketNature = this.getTicketNature();
        final Object other$ticketNature = other.getTicketNature();
        if (this$ticketNature == null ? other$ticketNature != null : !this$ticketNature.equals(other$ticketNature))
            return false;
        final Object this$isDirectDiscount = this.getIsDirectDiscount();
        final Object other$isDirectDiscount = other.getIsDirectDiscount();
        if (this$isDirectDiscount == null ? other$isDirectDiscount != null : !this$isDirectDiscount.equals(other$isDirectDiscount))
            return false;
        final Object this$contactName = this.getContactName();
        final Object other$contactName = other.getContactName();
        if (this$contactName == null ? other$contactName != null : !this$contactName.equals(other$contactName))
            return false;
        final Object this$contactPhone = this.getContactPhone();
        final Object other$contactPhone = other.getContactPhone();
        if (this$contactPhone == null ? other$contactPhone != null : !this$contactPhone.equals(other$contactPhone))
            return false;
        final Object this$site = this.getSite();
        final Object other$site = other.getSite();
        if (this$site == null ? other$site != null : !this$site.equals(other$site)) return false;
        final Object this$language = this.getLanguage();
        final Object other$language = other.getLanguage();
        if (this$language == null ? other$language != null : !this$language.equals(other$language)) return false;
        final Object this$currency = this.getCurrency();
        final Object other$currency = other.getCurrency();
        if (this$currency == null ? other$currency != null : !this$currency.equals(other$currency)) return false;
        final Object this$couponName = this.getCouponName();
        final Object other$couponName = other.getCouponName();
        if (this$couponName == null ? other$couponName != null : !this$couponName.equals(other$couponName))
            return false;
        final Object this$couponAmount = this.getCouponAmount();
        final Object other$couponAmount = other.getCouponAmount();
        if (this$couponAmount == null ? other$couponAmount != null : !this$couponAmount.equals(other$couponAmount))
            return false;
        final Object this$couponCode = this.getCouponCode();
        final Object other$couponCode = other.getCouponCode();
        if (this$couponCode == null ? other$couponCode != null : !this$couponCode.equals(other$couponCode))
            return false;
        final Object this$voucherCode = this.getVoucherCode();
        final Object other$voucherCode = other.getVoucherCode();
        if (this$voucherCode == null ? other$voucherCode != null : !this$voucherCode.equals(other$voucherCode))
            return false;
        final Object this$directDiscountAmount = this.getDirectDiscountAmount();
        final Object other$directDiscountAmount = other.getDirectDiscountAmount();
        if (this$directDiscountAmount == null ? other$directDiscountAmount != null : !this$directDiscountAmount.equals(other$directDiscountAmount))
            return false;
        final Object this$staticPromotionDirectDiscountAmount = this.getStaticPromotionDirectDiscountAmount();
        final Object other$staticPromotionDirectDiscountAmount = other.getStaticPromotionDirectDiscountAmount();
        if (this$staticPromotionDirectDiscountAmount == null ? other$staticPromotionDirectDiscountAmount != null : !this$staticPromotionDirectDiscountAmount.equals(other$staticPromotionDirectDiscountAmount))
            return false;
        final Object this$isStaticPromotionDirectDiscount = this.getIsStaticPromotionDirectDiscount();
        final Object other$isStaticPromotionDirectDiscount = other.getIsStaticPromotionDirectDiscount();
        if (this$isStaticPromotionDirectDiscount == null ? other$isStaticPromotionDirectDiscount != null : !this$isStaticPromotionDirectDiscount.equals(other$isStaticPromotionDirectDiscount))
            return false;
        final Object this$fareBasis = this.getFareBasis();
        final Object other$fareBasis = other.getFareBasis();
        if (this$fareBasis == null ? other$fareBasis != null : !this$fareBasis.equals(other$fareBasis)) return false;
        final Object this$isGiftInsurance = this.getIsGiftInsurance();
        final Object other$isGiftInsurance = other.getIsGiftInsurance();
        if (this$isGiftInsurance == null ? other$isGiftInsurance != null : !this$isGiftInsurance.equals(other$isGiftInsurance))
            return false;
        final Object this$giftInsuranceName = this.getGiftInsuranceName();
        final Object other$giftInsuranceName = other.getGiftInsuranceName();
        if (this$giftInsuranceName == null ? other$giftInsuranceName != null : !this$giftInsuranceName.equals(other$giftInsuranceName))
            return false;
        final Object this$isGiftCoupon = this.getIsGiftCoupon();
        final Object other$isGiftCoupon = other.getIsGiftCoupon();
        if (this$isGiftCoupon == null ? other$isGiftCoupon != null : !this$isGiftCoupon.equals(other$isGiftCoupon))
            return false;
        final Object this$isMemberExclusiveDirectDiscount = this.getIsMemberExclusiveDirectDiscount();
        final Object other$isMemberExclusiveDirectDiscount = other.getIsMemberExclusiveDirectDiscount();
        if (this$isMemberExclusiveDirectDiscount == null ? other$isMemberExclusiveDirectDiscount != null : !this$isMemberExclusiveDirectDiscount.equals(other$isMemberExclusiveDirectDiscount))
            return false;
        final Object this$isRealNameExclusiveDirectDiscount = this.getIsRealNameExclusiveDirectDiscount();
        final Object other$isRealNameExclusiveDirectDiscount = other.getIsRealNameExclusiveDirectDiscount();
        if (this$isRealNameExclusiveDirectDiscount == null ? other$isRealNameExclusiveDirectDiscount != null : !this$isRealNameExclusiveDirectDiscount.equals(other$isRealNameExclusiveDirectDiscount))
            return false;
        final Object this$airline = this.getAirline();
        final Object other$airline = other.getAirline();
        if (this$airline == null ? other$airline != null : !this$airline.equals(other$airline)) return false;
        final Object this$mileageDeductionAmount = this.getMileageDeductionAmount();
        final Object other$mileageDeductionAmount = other.getMileageDeductionAmount();
        if (this$mileageDeductionAmount == null ? other$mileageDeductionAmount != null : !this$mileageDeductionAmount.equals(other$mileageDeductionAmount))
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
        return other instanceof TOdsScSaleDetail;
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
        final Object $pnr = this.getPnr();
        result = result * PRIME + ($pnr == null ? 43 : $pnr.hashCode());
        final Object $ticketNumber = this.getTicketNumber();
        result = result * PRIME + ($ticketNumber == null ? 43 : $ticketNumber.hashCode());
        final Object $flightNumber = this.getFlightNumber();
        result = result * PRIME + ($flightNumber == null ? 43 : $flightNumber.hashCode());
        final Object $segmentThreeCode = this.getSegmentThreeCode();
        result = result * PRIME + ($segmentThreeCode == null ? 43 : $segmentThreeCode.hashCode());
        final Object $segment = this.getSegment();
        result = result * PRIME + ($segment == null ? 43 : $segment.hashCode());
        final Object $cabin = this.getCabin();
        result = result * PRIME + ($cabin == null ? 43 : $cabin.hashCode());
        final Object $departureTime = this.getDepartureTime();
        result = result * PRIME + ($departureTime == null ? 43 : $departureTime.hashCode());
        final Object $passengerName = this.getPassengerName();
        result = result * PRIME + ($passengerName == null ? 43 : $passengerName.hashCode());
        final Object $priceBeforeDiscount = this.getPriceBeforeDiscount();
        result = result * PRIME + ($priceBeforeDiscount == null ? 43 : $priceBeforeDiscount.hashCode());
        final Object $priceAfterDiscount = this.getPriceAfterDiscount();
        result = result * PRIME + ($priceAfterDiscount == null ? 43 : $priceAfterDiscount.hashCode());
        final Object $airportConstructionFee = this.getAirportConstructionFee();
        result = result * PRIME + ($airportConstructionFee == null ? 43 : $airportConstructionFee.hashCode());
        final Object $fuelSurcharge = this.getFuelSurcharge();
        result = result * PRIME + ($fuelSurcharge == null ? 43 : $fuelSurcharge.hashCode());
        final Object $otherTax = this.getOtherTax();
        result = result * PRIME + ($otherTax == null ? 43 : $otherTax.hashCode());
        final Object $insuranceFee = this.getInsuranceFee();
        result = result * PRIME + ($insuranceFee == null ? 43 : $insuranceFee.hashCode());
        final Object $amountPayable = this.getAmountPayable();
        result = result * PRIME + ($amountPayable == null ? 43 : $amountPayable.hashCode());
        final Object $productName = this.getProductName();
        result = result * PRIME + ($productName == null ? 43 : $productName.hashCode());
        final Object $bookingDate = this.getBookingDate();
        result = result * PRIME + ($bookingDate == null ? 43 : $bookingDate.hashCode());
        final Object $ticketingDate = this.getTicketingDate();
        result = result * PRIME + ($ticketingDate == null ? 43 : $ticketingDate.hashCode());
        final Object $bookingStatus = this.getBookingStatus();
        result = result * PRIME + ($bookingStatus == null ? 43 : $bookingStatus.hashCode());
        final Object $orderSource = this.getOrderSource();
        result = result * PRIME + ($orderSource == null ? 43 : $orderSource.hashCode());
        final Object $orderChannel = this.getOrderChannel();
        result = result * PRIME + ($orderChannel == null ? 43 : $orderChannel.hashCode());
        final Object $bookingUsername = this.getBookingUsername();
        result = result * PRIME + ($bookingUsername == null ? 43 : $bookingUsername.hashCode());
        final Object $registeredUserName = this.getRegisteredUserName();
        result = result * PRIME + ($registeredUserName == null ? 43 : $registeredUserName.hashCode());
        final Object $bookingUserPhone = this.getBookingUserPhone();
        result = result * PRIME + ($bookingUserPhone == null ? 43 : $bookingUserPhone.hashCode());
        final Object $bookingUserEmail = this.getBookingUserEmail();
        result = result * PRIME + ($bookingUserEmail == null ? 43 : $bookingUserEmail.hashCode());
        final Object $bankName = this.getBankName();
        result = result * PRIME + ($bankName == null ? 43 : $bankName.hashCode());
        final Object $actualCarrier = this.getActualCarrier();
        result = result * PRIME + ($actualCarrier == null ? 43 : $actualCarrier.hashCode());
        final Object $bigCustomerNumber = this.getBigCustomerNumber();
        result = result * PRIME + ($bigCustomerNumber == null ? 43 : $bigCustomerNumber.hashCode());
        final Object $frequentFlyerCardNo = this.getFrequentFlyerCardNo();
        result = result * PRIME + ($frequentFlyerCardNo == null ? 43 : $frequentFlyerCardNo.hashCode());
        final Object $paymentTime = this.getPaymentTime();
        result = result * PRIME + ($paymentTime == null ? 43 : $paymentTime.hashCode());
        final Object $purchaseProcess = this.getPurchaseProcess();
        result = result * PRIME + ($purchaseProcess == null ? 43 : $purchaseProcess.hashCode());
        final Object $passengerType = this.getPassengerType();
        result = result * PRIME + ($passengerType == null ? 43 : $passengerType.hashCode());
        final Object $idType = this.getIdType();
        result = result * PRIME + ($idType == null ? 43 : $idType.hashCode());
        final Object $idNumber = this.getIdNumber();
        result = result * PRIME + ($idNumber == null ? 43 : $idNumber.hashCode());
        final Object $itineraryType = this.getItineraryType();
        result = result * PRIME + ($itineraryType == null ? 43 : $itineraryType.hashCode());
        final Object $ticketNature = this.getTicketNature();
        result = result * PRIME + ($ticketNature == null ? 43 : $ticketNature.hashCode());
        final Object $isDirectDiscount = this.getIsDirectDiscount();
        result = result * PRIME + ($isDirectDiscount == null ? 43 : $isDirectDiscount.hashCode());
        final Object $contactName = this.getContactName();
        result = result * PRIME + ($contactName == null ? 43 : $contactName.hashCode());
        final Object $contactPhone = this.getContactPhone();
        result = result * PRIME + ($contactPhone == null ? 43 : $contactPhone.hashCode());
        final Object $site = this.getSite();
        result = result * PRIME + ($site == null ? 43 : $site.hashCode());
        final Object $language = this.getLanguage();
        result = result * PRIME + ($language == null ? 43 : $language.hashCode());
        final Object $currency = this.getCurrency();
        result = result * PRIME + ($currency == null ? 43 : $currency.hashCode());
        final Object $couponName = this.getCouponName();
        result = result * PRIME + ($couponName == null ? 43 : $couponName.hashCode());
        final Object $couponAmount = this.getCouponAmount();
        result = result * PRIME + ($couponAmount == null ? 43 : $couponAmount.hashCode());
        final Object $couponCode = this.getCouponCode();
        result = result * PRIME + ($couponCode == null ? 43 : $couponCode.hashCode());
        final Object $voucherCode = this.getVoucherCode();
        result = result * PRIME + ($voucherCode == null ? 43 : $voucherCode.hashCode());
        final Object $directDiscountAmount = this.getDirectDiscountAmount();
        result = result * PRIME + ($directDiscountAmount == null ? 43 : $directDiscountAmount.hashCode());
        final Object $staticPromotionDirectDiscountAmount = this.getStaticPromotionDirectDiscountAmount();
        result = result * PRIME + ($staticPromotionDirectDiscountAmount == null ? 43 : $staticPromotionDirectDiscountAmount.hashCode());
        final Object $isStaticPromotionDirectDiscount = this.getIsStaticPromotionDirectDiscount();
        result = result * PRIME + ($isStaticPromotionDirectDiscount == null ? 43 : $isStaticPromotionDirectDiscount.hashCode());
        final Object $fareBasis = this.getFareBasis();
        result = result * PRIME + ($fareBasis == null ? 43 : $fareBasis.hashCode());
        final Object $isGiftInsurance = this.getIsGiftInsurance();
        result = result * PRIME + ($isGiftInsurance == null ? 43 : $isGiftInsurance.hashCode());
        final Object $giftInsuranceName = this.getGiftInsuranceName();
        result = result * PRIME + ($giftInsuranceName == null ? 43 : $giftInsuranceName.hashCode());
        final Object $isGiftCoupon = this.getIsGiftCoupon();
        result = result * PRIME + ($isGiftCoupon == null ? 43 : $isGiftCoupon.hashCode());
        final Object $isMemberExclusiveDirectDiscount = this.getIsMemberExclusiveDirectDiscount();
        result = result * PRIME + ($isMemberExclusiveDirectDiscount == null ? 43 : $isMemberExclusiveDirectDiscount.hashCode());
        final Object $isRealNameExclusiveDirectDiscount = this.getIsRealNameExclusiveDirectDiscount();
        result = result * PRIME + ($isRealNameExclusiveDirectDiscount == null ? 43 : $isRealNameExclusiveDirectDiscount.hashCode());
        final Object $airline = this.getAirline();
        result = result * PRIME + ($airline == null ? 43 : $airline.hashCode());
        final Object $mileageDeductionAmount = this.getMileageDeductionAmount();
        result = result * PRIME + ($mileageDeductionAmount == null ? 43 : $mileageDeductionAmount.hashCode());
        final Object $etlCreateTime = this.getEtlCreateTime();
        result = result * PRIME + ($etlCreateTime == null ? 43 : $etlCreateTime.hashCode());
        final Object $etlUpdateTime = this.getEtlUpdateTime();
        result = result * PRIME + ($etlUpdateTime == null ? 43 : $etlUpdateTime.hashCode());
        final Object $etlDate = this.getEtlDate();
        result = result * PRIME + ($etlDate == null ? 43 : $etlDate.hashCode());
        return result;
    }

    public String toString() {
        return "TOdsScSaleDetail(id=" + this.getId() + ", orderNumber=" + this.getOrderNumber() + ", bankOrderNumber=" + this.getBankOrderNumber() + ", pnr=" + this.getPnr() + ", ticketNumber=" + this.getTicketNumber() + ", flightNumber=" + this.getFlightNumber() + ", segmentThreeCode=" + this.getSegmentThreeCode() + ", segment=" + this.getSegment() + ", cabin=" + this.getCabin() + ", departureTime=" + this.getDepartureTime() + ", passengerName=" + this.getPassengerName() + ", priceBeforeDiscount=" + this.getPriceBeforeDiscount() + ", priceAfterDiscount=" + this.getPriceAfterDiscount() + ", airportConstructionFee=" + this.getAirportConstructionFee() + ", fuelSurcharge=" + this.getFuelSurcharge() + ", otherTax=" + this.getOtherTax() + ", insuranceFee=" + this.getInsuranceFee() + ", amountPayable=" + this.getAmountPayable() + ", productName=" + this.getProductName() + ", bookingDate=" + this.getBookingDate() + ", ticketingDate=" + this.getTicketingDate() + ", bookingStatus=" + this.getBookingStatus() + ", orderSource=" + this.getOrderSource() + ", orderChannel=" + this.getOrderChannel() + ", bookingUsername=" + this.getBookingUsername() + ", registeredUserName=" + this.getRegisteredUserName() + ", bookingUserPhone=" + this.getBookingUserPhone() + ", bookingUserEmail=" + this.getBookingUserEmail() + ", bankName=" + this.getBankName() + ", actualCarrier=" + this.getActualCarrier() + ", bigCustomerNumber=" + this.getBigCustomerNumber() + ", frequentFlyerCardNo=" + this.getFrequentFlyerCardNo() + ", paymentTime=" + this.getPaymentTime() + ", purchaseProcess=" + this.getPurchaseProcess() + ", passengerType=" + this.getPassengerType() + ", idType=" + this.getIdType() + ", idNumber=" + this.getIdNumber() + ", itineraryType=" + this.getItineraryType() + ", ticketNature=" + this.getTicketNature() + ", isDirectDiscount=" + this.getIsDirectDiscount() + ", contactName=" + this.getContactName() + ", contactPhone=" + this.getContactPhone() + ", site=" + this.getSite() + ", language=" + this.getLanguage() + ", currency=" + this.getCurrency() + ", couponName=" + this.getCouponName() + ", couponAmount=" + this.getCouponAmount() + ", couponCode=" + this.getCouponCode() + ", voucherCode=" + this.getVoucherCode() + ", directDiscountAmount=" + this.getDirectDiscountAmount() + ", staticPromotionDirectDiscountAmount=" + this.getStaticPromotionDirectDiscountAmount() + ", isStaticPromotionDirectDiscount=" + this.getIsStaticPromotionDirectDiscount() + ", fareBasis=" + this.getFareBasis() + ", isGiftInsurance=" + this.getIsGiftInsurance() + ", giftInsuranceName=" + this.getGiftInsuranceName() + ", isGiftCoupon=" + this.getIsGiftCoupon() + ", isMemberExclusiveDirectDiscount=" + this.getIsMemberExclusiveDirectDiscount() + ", isRealNameExclusiveDirectDiscount=" + this.getIsRealNameExclusiveDirectDiscount() + ", airline=" + this.getAirline() + ", mileageDeductionAmount=" + this.getMileageDeductionAmount() + ", etlCreateTime=" + this.getEtlCreateTime() + ", etlUpdateTime=" + this.getEtlUpdateTime() + ", etlDate=" + this.getEtlDate() + ")";
    }
}


