package com.travelsky.trp.usercenter.data.analysis.model.dwd.fact;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * 服务预约-业务级事实表
 */
public class ServAppointmentFactModal {

    /**
     * 主键
     */
    @JsonProperty("PK_ID")
    private String pkId;

    /**
     * 预订日期
     */
    @JsonProperty("FK_BOOKING_DATE")
    private String fkBookingDate;

    /**
     * 预订时间
     */
    @JsonProperty("FK_BOOKING_TIME")
    private String fkBookingTime;

    /**
     * 预订服务次数
     */
    @JsonProperty("BOOKING_COUNT")
    private Integer bookingCount = 1;

    /**
     * 订单号
     */
    @JsonProperty("ORDER_NO")
    private String orderNo;

    /**
     * 订单状态
     */
    @JsonProperty("ORDER_STATUS")
    private String orderStatus;

    /**
     * 乘机人姓名
     */
    @JsonProperty("PASSENGER_NAME")
    private String passengerName;

    /**
     * 乘机人证件类型
     */
    @JsonProperty("CERT_TYPE")
    private String certType;

    /**
     * 乘机人证件号
     */
    @JsonProperty("CERT_NUMBER")
    private String certNumber;

    /**
     * 乘机人TID
     */
    @JsonProperty("PASSENGER_USER_TID")
    private String passengerUserTid;

    /**
     * 预订渠道
     */
    @JsonProperty("BOOKING_CHANNEL")
    private String bookingChannel;

    /**
     * 服务内容
     */
    @JsonProperty("SERVICE_ITEM")
    private String serviceItem;

    /**
     * 出票OFFICE
     */
    @JsonProperty("ISSUE_OFFICE")
    private String issueOffice;

    /**
     * 订票OFFICE
     */
    @JsonProperty("BOOK_OFFICE")
    private String bookOffice;

    /**
     * 票号
     */
    @JsonProperty("TICKET_NUMBER")
    private String ticketNumber;

    /**
     * 起飞机场
     */
    @JsonProperty("DEPAIRPORT")
    private String depairport;

    /**
     * 到达机场
     */
    @JsonProperty("ARRIAIRPORT")
    private String arriairport;

    /**
     * 航班号
     */
    @JsonProperty("FLIGHT_NUMBER")
    private String flightNumber;

    /**
     * 航班日期
     */
    @JsonProperty("FLIGHT_DATE")
    private String flightDate;

    /**
     * 起飞时间
     */
    @JsonProperty("DEPARTURE_TIME")
    private String departureTime;

    /**
     * 到达时间
     */
    @JsonProperty("ARRIVAL_TIME")
    private String arrivalTime;

    /**
     * 舱位
     */
    @JsonProperty("CABIN")
    private String cabin;

    /**
     * 航段价
     */
    @JsonProperty("SEGMENT_PRICE")
    private BigDecimal segmentPrice;

    /**
     * 评价状态
     */
    @JsonProperty("REVIEW_STATUS")
    private String reviewStatus;

    /**
     * 评价内容
     */
    @JsonProperty("REVIEW_CONTENT")
    private String reviewContent;

    /**
     * 评价级别
     */
    @JsonProperty("REVIEW_LEVEL")
    private Integer reviewLevel;

    /**
     * 评价时间
     */
    @JsonProperty("REVIEW_TIME")
    private String reviewTime;

    /**
     * 客票标识
     */
    @JsonProperty("TICKET_FLAG")
    private String ticketFlag;

    /**
     * 原订单号
     */
    @JsonProperty("ORIGINAL_ORDERNO")
    private String originalOrderno;

    /**
     * 预订人鲁雁行卡号 LY_CARD_NUMBER
     */
    @JsonProperty("LY_CARD_NUMBER")
    private String lyCardNumber;


    /**
     * 预订人TID
     */
    @JsonProperty("FK_BOOKING_USER_TID")
    private String fkBookingUserTid;
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

    public String getFkBookingDate() {
        return fkBookingDate;
    }

    public void setFkBookingDate(String fkBookingDate) {
        this.fkBookingDate = fkBookingDate;
    }

    public String getFkBookingTime() {
        return fkBookingTime;
    }

    public void setFkBookingTime(String fkBookingTime) {
        this.fkBookingTime = fkBookingTime;
    }

    public Integer getBookingCount() {
        return bookingCount;
    }

    public void setBookingCount(Integer bookingCount) {
        this.bookingCount = bookingCount;
    }

    public String getOrderNo() {
        return orderNo;
    }

    public void setOrderNo(String orderNo) {
        this.orderNo = orderNo;
    }

    public String getOrderStatus() {
        return orderStatus;
    }

    public void setOrderStatus(String orderStatus) {
        this.orderStatus = orderStatus;
    }

    public String getPassengerName() {
        return passengerName;
    }

    public void setPassengerName(String passengerName) {
        this.passengerName = passengerName;
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

    public String getPassengerUserTid() {
        return passengerUserTid;
    }

    public void setPassengerUserTid(String passengerUserTid) {
        this.passengerUserTid = passengerUserTid;
    }

    public String getBookingChannel() {
        return bookingChannel;
    }

    public void setBookingChannel(String bookingChannel) {
        this.bookingChannel = bookingChannel;
    }

    public String getServiceItem() {
        return serviceItem;
    }

    public void setServiceItem(String serviceItem) {
        this.serviceItem = serviceItem;
    }

    public String getIssueOffice() {
        return issueOffice;
    }

    public void setIssueOffice(String issueOffice) {
        this.issueOffice = issueOffice;
    }

    public String getBookOffice() {
        return bookOffice;
    }

    public void setBookOffice(String bookOffice) {
        this.bookOffice = bookOffice;
    }

    public String getTicketNumber() {
        return ticketNumber;
    }

    public void setTicketNumber(String ticketNumber) {
        this.ticketNumber = ticketNumber;
    }

    public String getDepairport() {
        return depairport;
    }

    public void setDepairport(String depairport) {
        this.depairport = depairport;
    }

    public String getArriairport() {
        return arriairport;
    }

    public void setArriairport(String arriairport) {
        this.arriairport = arriairport;
    }

    public String getFlightNumber() {
        return flightNumber;
    }

    public void setFlightNumber(String flightNumber) {
        this.flightNumber = flightNumber;
    }

    public String getFlightDate() {
        return flightDate;
    }

    public void setFlightDate(String flightDate) {
        this.flightDate = flightDate;
    }

    public String getDepartureTime() {
        return departureTime;
    }

    public void setDepartureTime(String departureTime) {
        this.departureTime = departureTime;
    }

    public String getArrivalTime() {
        return arrivalTime;
    }

    public void setArrivalTime(String arrivalTime) {
        this.arrivalTime = arrivalTime;
    }

    public String getCabin() {
        return cabin;
    }

    public void setCabin(String cabin) {
        this.cabin = cabin;
    }

    public BigDecimal getSegmentPrice() {
        return segmentPrice;
    }

    public void setSegmentPrice(BigDecimal segmentPrice) {
        this.segmentPrice = segmentPrice;
    }

    public String getReviewStatus() {
        return reviewStatus;
    }

    public void setReviewStatus(String reviewStatus) {
        this.reviewStatus = reviewStatus;
    }

    public String getReviewContent() {
        return reviewContent;
    }

    public void setReviewContent(String reviewContent) {
        this.reviewContent = reviewContent;
    }

    public Integer getReviewLevel() {
        return reviewLevel;
    }

    public void setReviewLevel(Integer reviewLevel) {
        this.reviewLevel = reviewLevel;
    }

    public String getReviewTime() {
        return reviewTime;
    }

    public void setReviewTime(String reviewTime) {
        this.reviewTime = reviewTime;
    }

    public String getTicketFlag() {
        return ticketFlag;
    }

    public void setTicketFlag(String ticketFlag) {
        this.ticketFlag = ticketFlag;
    }

    public String getOriginalOrderno() {
        return originalOrderno;
    }

    public void setOriginalOrderno(String originalOrderno) {
        this.originalOrderno = originalOrderno;
    }

    public String getSourceLastUpdatetime() {
        return sourceLastUpdatetime;
    }

    public void setSourceLastUpdatetime(String sourceLastUpdatetime) {
        this.sourceLastUpdatetime = sourceLastUpdatetime;
    }



    public String getSystemLastUpdatetime() {
        return systemLastUpdatetime;
    }

    public void setSystemLastUpdatetime(String systemLastUpdatetime) {
        this.systemLastUpdatetime = systemLastUpdatetime;
    }

    public String getLyCardNumber() {
        return lyCardNumber;
    }

    public void setLyCardNumber(String lyCardNumber) {
        this.lyCardNumber = lyCardNumber;
    }

    public String getFkBookingUserTid() {
        return fkBookingUserTid;
    }

    public void setFkBookingUserTid(String fkBookingUserTid) {
        this.fkBookingUserTid = fkBookingUserTid;
    }

    public String getSystemCreatetime() {
        return systemCreatetime;
    }

    public void setSystemCreatetime(String systemCreatetime) {
        this.systemCreatetime = systemCreatetime;
    }

    @Override
    public String toString() {
        return "ServAppointmentFactModal{" +
                "pkId='" + pkId + '\'' +
                ", fkBookingDate='" + fkBookingDate + '\'' +
                ", fkBookingTime='" + fkBookingTime + '\'' +
                ", bookingCount=" + bookingCount +
                ", orderNo='" + orderNo + '\'' +
                ", orderStatus='" + orderStatus + '\'' +
                ", passengerName='" + passengerName + '\'' +
                ", certType='" + certType + '\'' +
                ", certNumber='" + certNumber + '\'' +
                ", passengerUserTid='" + passengerUserTid + '\'' +
                ", bookingChannel='" + bookingChannel + '\'' +
                ", serviceItem='" + serviceItem + '\'' +
                ", issueOffice='" + issueOffice + '\'' +
                ", bookOffice='" + bookOffice + '\'' +
                ", ticketNumber='" + ticketNumber + '\'' +
                ", depairport='" + depairport + '\'' +
                ", arriairport='" + arriairport + '\'' +
                ", flightNumber='" + flightNumber + '\'' +
                ", flightDate='" + flightDate + '\'' +
                ", departureTime='" + departureTime + '\'' +
                ", arrivalTime='" + arrivalTime + '\'' +
                ", cabin='" + cabin + '\'' +
                ", segmentPrice=" + segmentPrice +
                ", reviewStatus='" + reviewStatus + '\'' +
                ", reviewContent='" + reviewContent + '\'' +
                ", reviewLevel=" + reviewLevel +
                ", reviewTime='" + reviewTime + '\'' +
                ", ticketFlag='" + ticketFlag + '\'' +
                ", originalOrderno='" + originalOrderno + '\'' +
                ", lyCardNumber='" + lyCardNumber + '\'' +
                ", fkBookingUserTid='" + fkBookingUserTid + '\'' +
                ", sourceLastUpdatetime='" + sourceLastUpdatetime + '\'' +
                ", systemCreatetime='" + systemCreatetime + '\'' +
                ", systemLastUpdatetime='" + systemLastUpdatetime + '\'' +
                '}';
    }
}
