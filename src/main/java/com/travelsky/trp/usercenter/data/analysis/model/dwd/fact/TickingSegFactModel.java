package com.travelsky.trp.usercenter.data.analysis.model.dwd.fact;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 机票出票航段级事实表实体类-T_DWD_TICKING_SEG_FACT
 *
 * @author TS.SHA.fuhuazhang
 * @date 2025/7/20  14:27
 */
public class TickingSegFactModel {
    /**
     * 主键ID
     */
    @JsonProperty("PK_ID")
    private String pkId;

    /**
     * PNR编号
     */
    @JsonProperty("AK_PNR_NUMBER")
    private String akPnrNumber;

    /**
     * 出票类型
     */
    @JsonProperty("AK_TICKET_TYPE")
    private String akTicketType;

    /**
     * 票号
     */
    @JsonProperty("AK_TICKET_NUMBER")
    private String akTicketNumber;

    /**
     * 出票日期
     */
    @JsonProperty("FK_ISSUE_DATE")
    private String fkIssueDate;

    /**
     * 出票时间
     */
    @JsonProperty("FK_ISSUE_TIME")
    private String fkIssueTime;

    /**
     * 出票office号
     */
    @JsonProperty("AK_BOOKING_OFFICE_NUMBER")
    private String akBookingOfficeNumber;

    /**
     * 乘机人类型（成人等）
     */
    @JsonProperty("PASSENGER_TYPE")
    private String passengerType;

    /**
     * 乘机人英文姓
     */
    @JsonProperty("EN_LAST_NAME")
    private String enLastName;

    /**
     * 乘机人英文名
     */
    @JsonProperty("EN_FIRST_NAME")
    private String enFirstName;

    /**
     * 乘机人中文姓名
     */
    @JsonProperty("CN_NAME")
    private String cnName;

    /**
     * 乘机人证件类型
     */
    @JsonProperty("CERT_TYPE")
    private String certType;

    /**
     * 乘机人证件号码
     */
    @JsonProperty("CERT_NUMBER")
    private String certNumber;

    /**
     * 修正后乘机人证件类型
     */
    //@JsonProperty("MODIFIED_CERT_TYPE")
    //private String modifiedCertType;

    /**
     * 乘机人年龄
     */
    @JsonProperty("PASSENGER_AGE")
    private Integer passengerAge;

    /**
     * DOCS的证件类型
     */
    @JsonProperty("DOCS_CERT_TYPE")
    private String docsCertType;

    /**
     * 乘机人TID
     */
    @JsonProperty("FK_PASSENGER_USER_TID")
    private String fkPassengerUserTid;

    /**
     * 常客卡号
     */
    @JsonProperty("FFRF")
    private String ffrf;

    /**
     * 飞行时是否使用常客卡
     * */
    @JsonProperty("IS_USED_EFR")
    private Boolean isUsedFfr;

    /**
     * 常客等级
     */
    @JsonProperty("FF_LEVEL")
    private String ffLevel;

    /**
     * 常客卡航司
     */
    @JsonProperty("FF_AIRLINE")
    private String ffAirline;

    /**
     * 常客联盟卡级别
     */
    @JsonProperty("FF_ALLIANCE_LEVEL")
    private String ffAllianceLevel;

    /**
     * 国内国际标识
     */
    @JsonProperty("AK_DIMARK")
    private String akDimark;

    /**
     * 是否联票
     */
    @JsonProperty("AK_CONJUCTION")
    private Boolean akConjuction;

    /**
     * 联票上一张票票号
     */
    @JsonProperty("PRIOR_TICKET")
    private String priorTicket;

    /**
     * 联票下一张票票号
     */
    @JsonProperty("NEXT_TICKET")
    private String nextTicket;

    /**
     * 运价基础FB
     */
    @JsonProperty("AK_FAREBASIS")
    private String akFarebasis;

    /**
     * 是否为无陪儿童票
     */
    @JsonProperty("UNACCOMPANIED_MINOR")
    private Boolean unaccompaniedMinor;

    /**
     * 是否政府采购票
     */
    @JsonProperty("IS_GOVERNMENT_PURCHASE")
    private Boolean isGovernmentPurchase;

    /**
     * 大客户号
     */
    @JsonProperty("AK_KEY_ACCOUNT_CODE")
    private String akKeyAccountCode;

    /**
     * 是否为大客户订单
     */
    @JsonProperty("IS_KEY_ACCOUNT")
    private Boolean isKeyAccount;

    /**
     * 是否是团队票
     */
    @JsonProperty("IS_TEAM_TICKET")
    private Boolean isTeamTicket;
    /**
     * 同行人数
     */
    @JsonProperty("AK_PEER_NUMBER")
    private Integer akPeerNumber;

    /**
     * 是否单人出行
     */
    @JsonProperty("SINGLE_TRAVELER")
    private Boolean singleTraveler;

    /**
     * 是否与儿童同行
     */
    @JsonProperty("AK_PEER_CHD")
    private Boolean akPeerChd;

    /**
     * 是否与老人同行
     */
    @JsonProperty("PEER_SENIOR")
    private Boolean peerSenior;

    /**
     * 是否与婴儿同行
     */
    //@JsonProperty("PEER_INFANT")
    //private Boolean peerInfant;

    /**
     * 是否为自己订票
     */
    @JsonProperty("SELF_BOOKING")
    private Boolean selfBooking;

    /**
     * 是否首次出票
     */
    @JsonProperty("AK_FIRST_ISSUE")
    private Boolean akFirstIssue;

    /**
     * 预订人是否在乘机人列表
     */
    @JsonProperty("AK_BOOKING_PASSENGER")
    private Boolean akBookingPassenger;

    /**
     * 销售币种
     */
    @JsonProperty("AK_CURRENCY")
    private String akCurrency;


    /**
     * 航段销售票面价格销售币种
     */
    @JsonProperty("AK_SALE_PRICE_CURRENCY")
    private BigDecimal akSalePriceCurrency;

    /**
     * 航段销售基建税销售币种
     */
    @JsonProperty("AK_SALE_TAX_CURRENCY")
    private BigDecimal akSaleTaxCurrency;

    /**
     * 航段销售燃油税销售币种
     */
    @JsonProperty("AK_SALE_FUEL_TAX_CURRENCY")
    private BigDecimal akSaleFuelTaxCurrency;

    /**
     * 航段销售票面价格CNY
     */
    @JsonProperty("AK_SALE_PRICE_CNY")
    private BigDecimal akSalePriceCny;

    /**
     * 航段销售基建税CNY
     */
    @JsonProperty("AK_SALE_TAX_CNY")
    private BigDecimal akSaleTaxCny;

    /**
     * 航段销售燃油税CNY
     */
    @JsonProperty("AK_SALE_FUEL_TAX_CNY")
    private BigDecimal akSaleFuelTaxCny;

    /**
     * 航段销售总税费CNY
     */
    @JsonProperty("AK_SALE_TAX_ALL_CNY")
    private BigDecimal akSaleTaxAllCny;

    /**
     * 销售航段总票价CNY
     */
    @JsonProperty("AK_SALE_PRICE_ALL_CNY")
    private BigDecimal akSalePriceAllCny;

    /**
     * 折扣
     */
    @JsonProperty("AK_DISCOUNT")
    private BigDecimal akDiscount;

    /**
     * Y舱标准价
     */
    @JsonProperty("Y_CABIN_NORMAL_PRICE")
    private BigDecimal yCabinNormalPrice;

    /**
     * B2B OFFICE
     */
    @JsonProperty("B2B_OFFICE")
    private String b2bOffice;

    /**
     * B2B IATANO
     */
    @JsonProperty("B2B_IATANO")
    private String b2bIatan;

    /**
     * B2B代理费
     */
    @JsonProperty("B2B_AGENCY_FEE")
    private BigDecimal b2bAgencyFee;

    /**
     * 起飞机场
     */
    @JsonProperty("FK_DEPAIRPORT")
    private String fkDepairport;

    /**
     * 到达机场
     */
    @JsonProperty("FK_ARRIAIRPORT")
    private String fkArriairport;

    /**
     * 航程
     */
    @JsonProperty("FK_SEG_SEGMENT")
    private String fkSegSegment;

    /**
     * 航班起飞日期
     */
    @JsonProperty("FK_SEG_DATE")
    private String fkSegDate;

    /**
     * 航班起飞时间
     */
    @JsonProperty("FK_SEG_TIME")
    private String fkSegTime;

    /**
     * 舱等
     */
    @JsonProperty("AK_CABIN")
    private String akCabin;

    /**
     * 航位
     */
    @JsonProperty("AK_SEGCABIN")
    private String akSegcabin;

    /**
     * 航段状态
     */
    @JsonProperty("SEGMENT_STATUS")
    private String segmentStatus;

    /**
     * IRR标识
     */
    @JsonProperty("AK_IRRFLAG")
    private String akIrrflag;

    /**
     * 特殊餐食属性（仅限免费的）
     */
    @JsonProperty("AK_SPCA_ATT")
    private String akSpcaAtt;

    /**
     * 航段有效期起始日期
     */
    @JsonProperty("TIK_SEGSTARTDATE")
    private String tikSegstartdate;

    /**
     * 航段有效期截止日期
     */
    @JsonProperty("TIK_SEGENDDATE")
    private String tikSegenddate;

    /**
     * 是否发生过换开
     */
    @JsonProperty("IS_RESCHEDULED")
    private Boolean isRescheduled;

    /**
     * 提前出票天数
     */
    @JsonProperty("AK_ADVBOOK_DAY")
    private Integer akAdvbookDay;

    /**
     * 预选免费座位号
     */
    @JsonProperty("SEAT_NUMBER")
    private String seatNumber;

    /**
     * 是否为返乡段
     */
    @JsonProperty("HOMECOMING_SEG")
    private Boolean homecomingSeg;

    /**
     * 航段序号
     */
    @JsonProperty("SEGMENT_SEQ")
    private String segmentSeq;

    /**
     * 停留地
     */
    @JsonProperty("STOPOVER")
    private String stopover;

    /**
     * 停留天数
     */
    @JsonProperty("STOPOVER_DAY")
    private Integer stopoverDay;


    /**
     * 票价类型（全价票、折扣票、特价票）
     */
    @JsonProperty("PRICE_TYPE")
    private String priceType;

    /**
     * 出票航段计数
     */
    @JsonProperty("TIK_SEGCOUNT")
    private Integer tikSegcount = 1;

    /**
     * 预选免费座位渠道
     */
    @JsonProperty("SEAT_CHANNEL")
    private String seatChannel;

    /**
     * 目的地是否为停留地
     */
    @JsonProperty("IS_DESTINATION_STOP")
    private Boolean isDestinationStop;

    /**
     * 渠道订单号
     */
    @JsonProperty("CHANNEL_ORDERNO")
    private String channelOrderno;

    /**
     * 预订渠道
     */
    @JsonProperty("AK_CHANNEL")
    private String akChannel;

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
     * 是否使用优惠券
     */
    @JsonProperty("AK_COUPON")
    private Boolean akCoupon;

    /**
     * 优惠券产品编码
     */
    @JsonProperty("AK_COUPONCODE")
    private String akCouponcode;

    /**
     * 优惠券优惠金额
     */
    @JsonProperty("COUPON_AMOUNT")
    private BigDecimal couponAmount;

    /**
     * 优惠券张数
     */
    @JsonProperty("COUPON_COUNT")
    private Integer couponCount;

    /**
     * 优惠券券码
     */
    @JsonProperty("COUPONNO")
    private String couponno;

    /**
     * 优惠券名称
     */
    @JsonProperty("COUPON_NAME")
    private String couponName;

    /**
     * 联系人手机号
     */
    @JsonProperty("CONTACT_MOBILE_NUMBER")
    private String contactMobileNumber;

    /**
     * 联系人姓名
     */
    @JsonProperty("CONTACT_NAME")
    private String contactName;

    /**
     * 联系人固定电话
     */
    @JsonProperty("CONTACT_LANDLINE_MOBILE_NUMBER")
    private String contactLandlineMobileNumber;

    /**
     * 数据是否启用
     */
    @JsonProperty("DATA_ACTIVE")
    private Boolean dataActive;

    /**
     * 数据启用时间
     */
    @JsonProperty("DATA_ACTIVE_TIME")
    private String dataActiveTime;

    /**
     * 源系统最后更新时间（时间戳）
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

    public String getPkId() {
        return pkId;
    }

    public void setPkId(String pkId) {
        this.pkId = pkId;
    }

    public String getAkPnrNumber() {
        return akPnrNumber;
    }

    public void setAkPnrNumber(String akPnrNumber) {
        this.akPnrNumber = akPnrNumber;
    }

    public String getAkTicketType() {
        return akTicketType;
    }

    public void setAkTicketType(String akTicketType) {
        this.akTicketType = akTicketType;
    }

    public String getAkTicketNumber() {
        return akTicketNumber;
    }

    public void setAkTicketNumber(String akTicketNumber) {
        this.akTicketNumber = akTicketNumber;
    }

    public String getFkIssueDate() {
        return fkIssueDate;
    }

    public void setFkIssueDate(String fkIssueDate) {
        this.fkIssueDate = fkIssueDate;
    }

    public String getFkIssueTime() {
        return fkIssueTime;
    }

    public void setFkIssueTime(String fkIssueTime) {
        this.fkIssueTime = fkIssueTime;
    }

    public String getAkBookingOfficeNumber() {
        return akBookingOfficeNumber;
    }

    public void setAkBookingOfficeNumber(String akBookingOfficeNumber) {
        this.akBookingOfficeNumber = akBookingOfficeNumber;
    }

    public String getPassengerType() {
        return passengerType;
    }

    public void setPassengerType(String passengerType) {
        this.passengerType = passengerType;
    }

    public String getEnLastName() {
        return enLastName;
    }

    public void setEnLastName(String enLastName) {
        this.enLastName = enLastName;
    }

    public String getEnFirstName() {
        return enFirstName;
    }

    public void setEnFirstName(String enFirstName) {
        this.enFirstName = enFirstName;
    }

    public String getCnName() {
        return cnName;
    }

    public void setCnName(String cnName) {
        this.cnName = cnName;
    }

    public String getCertType() {
        return certType;
    }

    public void setCertType(String certType) {
        this.certType = certType;
    }

    public String getCertNumber() {
        return certNumber;
    }

    public void setCertNumber(String certNumber) {
        this.certNumber = certNumber;
    }

    //public String getModifiedCertType() {
    //    return modifiedCertType;
    //}
    //
    //public void setModifiedCertType(String modifiedCertType) {
    //    this.modifiedCertType = modifiedCertType;
    //}

    public Integer getPassengerAge() {
        return passengerAge;
    }

    public void setPassengerAge(Integer passengerAge) {
        this.passengerAge = passengerAge;
    }

    public String getDocsCertType() {
        return docsCertType;
    }

    public void setDocsCertType(String docsCertType) {
        this.docsCertType = docsCertType;
    }

    public String getFkPassengerUserTid() {
        return fkPassengerUserTid;
    }

    public void setFkPassengerUserTid(String fkPassengerUserTid) {
        this.fkPassengerUserTid = fkPassengerUserTid;
    }

    public String getFfrf() {
        return ffrf;
    }

    public void setFfrf(String ffrf) {
        this.ffrf = ffrf;
    }

    public Boolean getUsedFfr() {
        return isUsedFfr;
    }

    public void setUsedFfr(Boolean usedFfr) {
        isUsedFfr = usedFfr;
    }

    public String getFfLevel() {
        return ffLevel;
    }

    public void setFfLevel(String ffLevel) {
        this.ffLevel = ffLevel;
    }

    public String getFfAirline() {
        return ffAirline;
    }

    public void setFfAirline(String ffAirline) {
        this.ffAirline = ffAirline;
    }

    public String getFfAllianceLevel() {
        return ffAllianceLevel;
    }

    public void setFfAllianceLevel(String ffAllianceLevel) {
        this.ffAllianceLevel = ffAllianceLevel;
    }

    public String getAkDimark() {
        return akDimark;
    }

    public void setAkDimark(String akDimark) {
        this.akDimark = akDimark;
    }

    public Boolean getAkConjuction() {
        return akConjuction;
    }

    public void setAkConjuction(Boolean akConjuction) {
        this.akConjuction = akConjuction;
    }

    public String getPriorTicket() {
        return priorTicket;
    }

    public void setPriorTicket(String priorTicket) {
        this.priorTicket = priorTicket;
    }

    public String getNextTicket() {
        return nextTicket;
    }

    public void setNextTicket(String nextTicket) {
        this.nextTicket = nextTicket;
    }

    public String getAkFarebasis() {
        return akFarebasis;
    }

    public void setAkFarebasis(String akFarebasis) {
        this.akFarebasis = akFarebasis;
    }

    public Boolean getUnaccompaniedMinor() {
        return unaccompaniedMinor;
    }

    public void setUnaccompaniedMinor(Boolean unaccompaniedMinor) {
        this.unaccompaniedMinor = unaccompaniedMinor;
    }

    public Boolean getGovernmentPurchase() {
        return isGovernmentPurchase;
    }

    public void setGovernmentPurchase(Boolean governmentPurchase) {
        isGovernmentPurchase = governmentPurchase;
    }

    public String getAkKeyAccountCode() {
        return akKeyAccountCode;
    }

    public void setAkKeyAccountCode(String akKeyAccountCode) {
        this.akKeyAccountCode = akKeyAccountCode;
    }

    public Boolean getKeyAccount() {
        return isKeyAccount;
    }

    public void setKeyAccount(Boolean keyAccount) {
        isKeyAccount = keyAccount;
    }

    public Boolean getTeamTicket() {
        return isTeamTicket;
    }

    public void setTeamTicket(Boolean teamTicket) {
        isTeamTicket = teamTicket;
    }

    public Integer getAkPeerNumber() {
        return akPeerNumber;
    }

    public void setAkPeerNumber(Integer akPeerNumber) {
        this.akPeerNumber = akPeerNumber;
    }

    public Boolean getSingleTraveler() {
        return singleTraveler;
    }

    public void setSingleTraveler(Boolean singleTraveler) {
        this.singleTraveler = singleTraveler;
    }

    public Boolean getAkPeerChd() {
        return akPeerChd;
    }

    public void setAkPeerChd(Boolean akPeerChd) {
        this.akPeerChd = akPeerChd;
    }

    public Boolean getPeerSenior() {
        return peerSenior;
    }

    public void setPeerSenior(Boolean peerSenior) {
        this.peerSenior = peerSenior;
    }

    //public Boolean getPeerInfant() {
    //    return peerInfant;
    //}
    //
    //public void setPeerInfant(Boolean peerInfant) {
    //    this.peerInfant = peerInfant;
    //}

    public Boolean getSelfBooking() {
        return selfBooking;
    }

    public void setSelfBooking(Boolean selfBooking) {
        this.selfBooking = selfBooking;
    }

    public Boolean getAkFirstIssue() {
        return akFirstIssue;
    }

    public void setAkFirstIssue(Boolean akFirstIssue) {
        this.akFirstIssue = akFirstIssue;
    }

    public Boolean getAkBookingPassenger() {
        return akBookingPassenger;
    }

    public void setAkBookingPassenger(Boolean akBookingPassenger) {
        this.akBookingPassenger = akBookingPassenger;
    }

    public String getAkCurrency() {
        return akCurrency;
    }

    public void setAkCurrency(String akCurrency) {
        this.akCurrency = akCurrency;
    }

    public BigDecimal getAkSalePriceCurrency() {
        return akSalePriceCurrency;
    }

    public void setAkSalePriceCurrency(BigDecimal akSalePriceCurrency) {
        this.akSalePriceCurrency = akSalePriceCurrency;
    }

    public BigDecimal getAkSaleTaxCurrency() {
        return akSaleTaxCurrency;
    }

    public void setAkSaleTaxCurrency(BigDecimal akSaleTaxCurrency) {
        this.akSaleTaxCurrency = akSaleTaxCurrency;
    }

    public BigDecimal getAkSaleFuelTaxCurrency() {
        return akSaleFuelTaxCurrency;
    }

    public void setAkSaleFuelTaxCurrency(BigDecimal akSaleFuelTaxCurrency) {
        this.akSaleFuelTaxCurrency = akSaleFuelTaxCurrency;
    }

    public BigDecimal getAkSalePriceCny() {
        return akSalePriceCny;
    }

    public void setAkSalePriceCny(BigDecimal akSalePriceCny) {
        this.akSalePriceCny = akSalePriceCny;
    }

    public BigDecimal getAkSaleTaxCny() {
        return akSaleTaxCny;
    }

    public void setAkSaleTaxCny(BigDecimal akSaleTaxCny) {
        this.akSaleTaxCny = akSaleTaxCny;
    }

    public BigDecimal getAkSaleFuelTaxCny() {
        return akSaleFuelTaxCny;
    }

    public void setAkSaleFuelTaxCny(BigDecimal akSaleFuelTaxCny) {
        this.akSaleFuelTaxCny = akSaleFuelTaxCny;
    }

    public BigDecimal getAkSaleTaxAllCny() {
        return akSaleTaxAllCny;
    }

    public void setAkSaleTaxAllCny(BigDecimal akSaleTaxAllCny) {
        this.akSaleTaxAllCny = akSaleTaxAllCny;
    }

    public BigDecimal getAkSalePriceAllCny() {
        return akSalePriceAllCny;
    }

    public void setAkSalePriceAllCny(BigDecimal akSalePriceAllCny) {
        this.akSalePriceAllCny = akSalePriceAllCny;
    }

    public BigDecimal getAkDiscount() {
        return akDiscount;
    }

    public void setAkDiscount(BigDecimal akDiscount) {
        this.akDiscount = akDiscount;
    }

    public BigDecimal getyCabinNormalPrice() {
        return yCabinNormalPrice;
    }

    public void setyCabinNormalPrice(BigDecimal yCabinNormalPrice) {
        this.yCabinNormalPrice = yCabinNormalPrice;
    }

    public String getB2bOffice() {
        return b2bOffice;
    }

    public void setB2bOffice(String b2bOffice) {
        this.b2bOffice = b2bOffice;
    }

    public String getB2bIatan() {
        return b2bIatan;
    }

    public void setB2bIatan(String b2bIatan) {
        this.b2bIatan = b2bIatan;
    }

    public BigDecimal getB2bAgencyFee() {
        return b2bAgencyFee;
    }

    public void setB2bAgencyFee(BigDecimal b2bAgencyFee) {
        this.b2bAgencyFee = b2bAgencyFee;
    }

    public String getFkDepairport() {
        return fkDepairport;
    }

    public void setFkDepairport(String fkDepairport) {
        this.fkDepairport = fkDepairport;
    }

    public String getFkArriairport() {
        return fkArriairport;
    }

    public void setFkArriairport(String fkArriairport) {
        this.fkArriairport = fkArriairport;
    }

    public String getFkSegSegment() {
        return fkSegSegment;
    }

    public void setFkSegSegment(String fkSegSegment) {
        this.fkSegSegment = fkSegSegment;
    }

    public String getFkSegDate() {
        return fkSegDate;
    }

    public void setFkSegDate(String fkSegDate) {
        this.fkSegDate = fkSegDate;
    }

    public String getFkSegTime() {
        return fkSegTime;
    }

    public void setFkSegTime(String fkSegTime) {
        this.fkSegTime = fkSegTime;
    }

    public String getAkCabin() {
        return akCabin;
    }

    public void setAkCabin(String akCabin) {
        this.akCabin = akCabin;
    }

    public String getAkSegcabin() {
        return akSegcabin;
    }

    public void setAkSegcabin(String akSegcabin) {
        this.akSegcabin = akSegcabin;
    }

    public String getSegmentStatus() {
        return segmentStatus;
    }

    public void setSegmentStatus(String segmentStatus) {
        this.segmentStatus = segmentStatus;
    }

    public String getAkIrrflag() {
        return akIrrflag;
    }

    public void setAkIrrflag(String akIrrflag) {
        this.akIrrflag = akIrrflag;
    }

    public String getAkSpcaAtt() {
        return akSpcaAtt;
    }

    public void setAkSpcaAtt(String akSpcaAtt) {
        this.akSpcaAtt = akSpcaAtt;
    }

    public String getTikSegstartdate() {
        return tikSegstartdate;
    }

    public void setTikSegstartdate(String tikSegstartdate) {
        this.tikSegstartdate = tikSegstartdate;
    }

    public String getTikSegenddate() {
        return tikSegenddate;
    }

    public void setTikSegenddate(String tikSegenddate) {
        this.tikSegenddate = tikSegenddate;
    }

    public Boolean getRescheduled() {
        return isRescheduled;
    }

    public void setRescheduled(Boolean rescheduled) {
        isRescheduled = rescheduled;
    }

    public Integer getAkAdvbookDay() {
        return akAdvbookDay;
    }

    public void setAkAdvbookDay(Integer akAdvbookDay) {
        this.akAdvbookDay = akAdvbookDay;
    }

    public String getSeatNumber() {
        return seatNumber;
    }

    public void setSeatNumber(String seatNumber) {
        this.seatNumber = seatNumber;
    }

    public Boolean getHomecomingSeg() {
        return homecomingSeg;
    }

    public void setHomecomingSeg(Boolean homecomingSeg) {
        this.homecomingSeg = homecomingSeg;
    }

    public String getSegmentSeq() {
        return segmentSeq;
    }

    public void setSegmentSeq(String segmentSeq) {
        this.segmentSeq = segmentSeq;
    }

    public String getStopover() {
        return stopover;
    }

    public void setStopover(String stopover) {
        this.stopover = stopover;
    }

    public Integer getStopoverDay() {
        return stopoverDay;
    }

    public void setStopoverDay(Integer stopoverDay) {
        this.stopoverDay = stopoverDay;
    }

    public String getPriceType() {
        return priceType;
    }

    public void setPriceType(String priceType) {
        this.priceType = priceType;
    }

    public Integer getTikSegcount() {
        return tikSegcount;
    }

    public void setTikSegcount(Integer tikSegcount) {
        this.tikSegcount = tikSegcount;
    }

    public String getSeatChannel() {
        return seatChannel;
    }

    public void setSeatChannel(String seatChannel) {
        this.seatChannel = seatChannel;
    }

    public Boolean getDestinationStop() {
        return isDestinationStop;
    }

    public void setDestinationStop(Boolean destinationStop) {
        isDestinationStop = destinationStop;
    }

    public String getChannelOrderno() {
        return channelOrderno;
    }

    public void setChannelOrderno(String channelOrderno) {
        this.channelOrderno = channelOrderno;
    }

    public String getAkChannel() {
        return akChannel;
    }

    public void setAkChannel(String akChannel) {
        this.akChannel = akChannel;
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

    public Boolean getAkCoupon() {
        return akCoupon;
    }

    public void setAkCoupon(Boolean akCoupon) {
        this.akCoupon = akCoupon;
    }

    public String getAkCouponcode() {
        return akCouponcode;
    }

    public void setAkCouponcode(String akCouponcode) {
        this.akCouponcode = akCouponcode;
    }

    public BigDecimal getCouponAmount() {
        return couponAmount;
    }

    public void setCouponAmount(BigDecimal couponAmount) {
        this.couponAmount = couponAmount;
    }

    public Integer getCouponCount() {
        return couponCount;
    }

    public void setCouponCount(Integer couponCount) {
        this.couponCount = couponCount;
    }

    public String getCouponno() {
        return couponno;
    }

    public void setCouponno(String couponno) {
        this.couponno = couponno;
    }

    public String getCouponName() {
        return couponName;
    }

    public void setCouponName(String couponName) {
        this.couponName = couponName;
    }

    public String getContactMobileNumber() {
        return contactMobileNumber;
    }

    public void setContactMobileNumber(String contactMobileNumber) {
        this.contactMobileNumber = contactMobileNumber;
    }

    public String getContactName() {
        return contactName;
    }

    public void setContactName(String contactName) {
        this.contactName = contactName;
    }

    public String getContactLandlineMobileNumber() {
        return contactLandlineMobileNumber;
    }

    public void setContactLandlineMobileNumber(String contactLandlineMobileNumber) {
        this.contactLandlineMobileNumber = contactLandlineMobileNumber;
    }

    public Boolean getDataActive() {
        return dataActive;
    }

    public void setDataActive(Boolean dataActive) {
        this.dataActive = dataActive;
    }

    public String getDataActiveTime() {
        return dataActiveTime;
    }

    public void setDataActiveTime(String dataActiveTime) {
        this.dataActiveTime = dataActiveTime;
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
        return "TickingSegFactModel{" +
                "pkId='" + pkId + '\'' +
                ", akPnrNumber='" + akPnrNumber + '\'' +
                ", akTicketType='" + akTicketType + '\'' +
                ", akTicketNumber='" + akTicketNumber + '\'' +
                ", fkIssueDate='" + fkIssueDate + '\'' +
                ", fkIssueTime='" + fkIssueTime + '\'' +
                ", akBookingOfficeNumber='" + akBookingOfficeNumber + '\'' +
                ", passengerType='" + passengerType + '\'' +
                ", enLastName='" + enLastName + '\'' +
                ", enFirstName='" + enFirstName + '\'' +
                ", cnName='" + cnName + '\'' +
                ", certType='" + certType + '\'' +
                ", certNumber='" + certNumber + '\'' +
                //", modifiedCertType='" + modifiedCertType + '\'' +
                ", passengerAge=" + passengerAge +
                ", docsCertType='" + docsCertType + '\'' +
                ", fkPassengerUserTid='" + fkPassengerUserTid + '\'' +
                ", ffrf='" + ffrf + '\'' +
                ", isUsedFfr=" + isUsedFfr +
                ", ffLevel='" + ffLevel + '\'' +
                ", ffAirline='" + ffAirline + '\'' +
                ", ffAllianceLevel='" + ffAllianceLevel + '\'' +
                ", akDimark='" + akDimark + '\'' +
                ", akConjuction=" + akConjuction +
                ", priorTicket='" + priorTicket + '\'' +
                ", nextTicket='" + nextTicket + '\'' +
                ", akFarebasis='" + akFarebasis + '\'' +
                ", unaccompaniedMinor=" + unaccompaniedMinor +
                ", isGovernmentPurchase=" + isGovernmentPurchase +
                ", akKeyAccountCode='" + akKeyAccountCode + '\'' +
                ", isKeyAccount=" + isKeyAccount +
                ", isTeamTicket=" + isTeamTicket +
                ", akPeerNumber=" + akPeerNumber +
                ", singleTraveler=" + singleTraveler +
                ", akPeerChd=" + akPeerChd +
                ", peerSenior=" + peerSenior +
                //", peerInfant=" + peerInfant +
                ", selfBooking=" + selfBooking +
                ", akFirstIssue=" + akFirstIssue +
                ", akBookingPassenger=" + akBookingPassenger +
                ", akCurrency='" + akCurrency + '\'' +
                ", akSalePriceCurrency=" + akSalePriceCurrency +
                ", akSaleTaxCurrency=" + akSaleTaxCurrency +
                ", akSaleFuelTaxCurrency=" + akSaleFuelTaxCurrency +
                ", akSalePriceCny=" + akSalePriceCny +
                ", akSaleTaxCny=" + akSaleTaxCny +
                ", akSaleFuelTaxCny=" + akSaleFuelTaxCny +
                ", akSaleTaxAllCny=" + akSaleTaxAllCny +
                ", akSalePriceAllCny=" + akSalePriceAllCny +
                ", akDiscount=" + akDiscount +
                ", yCabinNormalPrice=" + yCabinNormalPrice +
                ", b2bOffice='" + b2bOffice + '\'' +
                ", b2bIatan='" + b2bIatan + '\'' +
                ", b2bAgencyFee=" + b2bAgencyFee +
                ", fkDepairport='" + fkDepairport + '\'' +
                ", fkArriairport='" + fkArriairport + '\'' +
                ", fkSegSegment='" + fkSegSegment + '\'' +
                ", fkSegDate='" + fkSegDate + '\'' +
                ", fkSegTime='" + fkSegTime + '\'' +
                ", akCabin='" + akCabin + '\'' +
                ", akSegcabin='" + akSegcabin + '\'' +
                ", segmentStatus='" + segmentStatus + '\'' +
                ", akIrrflag='" + akIrrflag + '\'' +
                ", akSpcaAtt='" + akSpcaAtt + '\'' +
                ", tikSegstartdate='" + tikSegstartdate + '\'' +
                ", tikSegenddate='" + tikSegenddate + '\'' +
                ", isRescheduled=" + isRescheduled +
                ", akAdvbookDay=" + akAdvbookDay +
                ", seatNumber='" + seatNumber + '\'' +
                ", homecomingSeg=" + homecomingSeg +
                ", segmentSeq='" + segmentSeq + '\'' +
                ", stopover='" + stopover + '\'' +
                ", stopoverDay=" + stopoverDay +
                ", priceType='" + priceType + '\'' +
                ", tikSegcount=" + tikSegcount +
                ", seatChannel='" + seatChannel + '\'' +
                ", isDestinationStop=" + isDestinationStop +
                ", channelOrderno='" + channelOrderno + '\'' +
                ", akChannel='" + akChannel + '\'' +
                ", fkBookingUserTid='" + fkBookingUserTid + '\'' +
                ", fkBookingUserOriginId='" + fkBookingUserOriginId + '\'' +
                ", akCoupon=" + akCoupon +
                ", akCouponcode='" + akCouponcode + '\'' +
                ", couponAmount=" + couponAmount +
                ", couponCount=" + couponCount +
                ", couponno='" + couponno + '\'' +
                ", couponName='" + couponName + '\'' +
                ", contactMobileNumber='" + contactMobileNumber + '\'' +
                ", contactName='" + contactName + '\'' +
                ", contactLandlineMobileNumber='" + contactLandlineMobileNumber + '\'' +
                ", dataActive=" + dataActive +
                ", dataActiveTime='" + dataActiveTime + '\'' +
                ", sourceLastUpdatetime='" + sourceLastUpdatetime + '\'' +
                ", systemCreatetime='" + systemCreatetime + '\'' +
                ", systemLastUpdatetime='" + systemLastUpdatetime + '\'' +
                '}';
    }
}
