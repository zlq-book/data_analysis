package com.travelsky.trp.usercenter.data.analysis.model.dwd.fact;


import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;


public class AuisSegFactModel {
  /**
   * 主键ID
   */
  @JsonProperty("PK_ID")
  private String pkId;
  /**
   * 关联订单号
   */
  @JsonProperty("AK_ORDERNUM")
  private String akOrdernum;
  /**
   * 关联票号
   */
  @JsonProperty("AK_TIKNUM")
  private String akTiknum;
  /**
   * 保险预订日期
   */
  @JsonProperty("FK_BKAUIS_DATE")
  private String fkBkauisDate;
  /**
   * 保险预订时间
   */
  @JsonProperty("FK_BKAUIS_TIME")
  private String fkBkauisTime;
  /**
   * 乘机人姓名
   */
  @JsonProperty("CN_NAME")
  private String cnName;
  /**
   * 乘机人证件类型
   */
  @JsonProperty("AK_CERT_TYPE")
  private String akCertType;
  /**
   * 乘机人证件号
   */
  @JsonProperty("CERT_NUMBER")
  private String certNumber;
  /**
   * 乘机人TID
   */
  @JsonProperty("FK_PASSENGER_USER_TID")
  private String fkPassengerUserTid;
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
   * 航班日期
   */
  @JsonProperty("FK_SEG_DATE")
  private String fkSegDate;
  /**
   * 航班时间
   */
  @JsonProperty("FK_SEG_TIME")
  private String fkSegTime;
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
   * 保险类型
   */
  @JsonProperty("AK_INSURANCE_TYPE")
  private String akInsuranceType;
  /**
   * 保险公司
   */
  @JsonProperty("INSURANCE_COMPANY")
  private String insuranceCompany;
  /**
   * 保险公司保险单号
   */
  @JsonProperty("INSURANCE_POLICY_NO")
  private String insurancePolicyNo;
  /**
   * 保险预订渠道
   */
  @JsonProperty("AK_CHANNEL")
  private String akChannel;
  /**
   * 保险状态
   */
  @JsonProperty("AK_INSURSTATUS")
  private String akInsurstatus;
  /**
   * 保险金额
   */
  @JsonProperty("INSURANCE_AMT")
  private Double insuranceAmt;
  /**
   * 保险计数
   */
  @JsonProperty("INSURANCE_COUNT")
  private Long insuranceCount=1L;
  /**
   * 源系统最后更新时间
   */
  @JsonProperty("UPDATE_TIME")
  private String updateTime;
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

  public String getAkOrdernum() {
    return akOrdernum;
  }

  public void setAkOrdernum(String akOrdernum) {
    this.akOrdernum = akOrdernum;
  }

  public String getAkTiknum() {
    return akTiknum;
  }

  public void setAkTiknum(String akTiknum) {
    this.akTiknum = akTiknum;
  }

  public String getFkBkauisDate() {
    return fkBkauisDate;
  }

  public void setFkBkauisDate(String fkBkauisDate) {
    this.fkBkauisDate = fkBkauisDate;
  }

  public String getFkBkauisTime() {
    return fkBkauisTime;
  }

  public void setFkBkauisTime(String fkBkauisTime) {
    this.fkBkauisTime = fkBkauisTime;
  }

  public String getCnName() {
    return cnName;
  }

  public void setCnName(String cnName) {
    this.cnName = cnName;
  }

  public String getAkCertType() {
    return akCertType;
  }

  public void setAkCertType(String akCertType) {
    this.akCertType = akCertType;
  }

  public String getCertNumber() {
    return certNumber;
  }

  public void setCertNumber(String certNumber) {
    this.certNumber = certNumber;
  }

  public String getFkPassengerUserTid() {
    return fkPassengerUserTid;
  }

  public void setFkPassengerUserTid(String fkPassengerUserTid) {
    this.fkPassengerUserTid = fkPassengerUserTid;
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

  public String getAkInsuranceType() {
    return akInsuranceType;
  }

  public void setAkInsuranceType(String akInsuranceType) {
    this.akInsuranceType = akInsuranceType;
  }

  public String getInsuranceCompany() {
    return insuranceCompany;
  }

  public void setInsuranceCompany(String insuranceCompany) {
    this.insuranceCompany = insuranceCompany;
  }


  public String getAkChannel() {
    return akChannel;
  }

  public void setAkChannel(String akChannel) {
    this.akChannel = akChannel;
  }

  public String getAkInsurstatus() {
    return akInsurstatus;
  }

  public void setAkInsurstatus(String akInsurstatus) {
    this.akInsurstatus = akInsurstatus;
  }

  public Double getInsuranceAmt() {
    return insuranceAmt;
  }

  public void setInsuranceAmt(Double insuranceAmt) {
    this.insuranceAmt = insuranceAmt;
  }

  public Long getInsuranceCount() {
    return insuranceCount;
  }

  public void setInsuranceCount(Long insuranceCount) {
    this.insuranceCount = insuranceCount;
  }

  public String getUpdateTime() {
    return updateTime;
  }

  public void setUpdateTime(String updateTime) {
    this.updateTime = updateTime;
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

  public String getInsurancePolicyNo() {
    return insurancePolicyNo;
  }

  public void setInsurancePolicyNo(String insurancePolicyNo) {
    this.insurancePolicyNo = insurancePolicyNo;
  }

  @Override
  public String toString() {
    return "AuisSegFactModel{" +
            "pkId='" + pkId + '\'' +
            ", akOrdernum='" + akOrdernum + '\'' +
            ", akTiknum='" + akTiknum + '\'' +
            ", fkBkauisDate='" + fkBkauisDate + '\'' +
            ", fkBkauisTime='" + fkBkauisTime + '\'' +
            ", cnName='" + cnName + '\'' +
            ", akCertType='" + akCertType + '\'' +
            ", certNumber='" + certNumber + '\'' +
            ", fkPassengerUserTid='" + fkPassengerUserTid + '\'' +
            ", fkBookingUserTid='" + fkBookingUserTid + '\'' +
            ", fkBookingUserOriginId='" + fkBookingUserOriginId + '\'' +
            ", fkSegDate='" + fkSegDate + '\'' +
            ", fkSegTime='" + fkSegTime + '\'' +
            ", fkDepairport='" + fkDepairport + '\'' +
            ", fkArriairport='" + fkArriairport + '\'' +
            ", akInsuranceType='" + akInsuranceType + '\'' +
            ", insuranceCompany='" + insuranceCompany + '\'' +
            ", insurancePolicyNo='" + insurancePolicyNo + '\'' +
            ", akChannel='" + akChannel + '\'' +
            ", akInsurstatus='" + akInsurstatus + '\'' +
            ", insuranceAmt=" + insuranceAmt +
            ", insuranceCount=" + insuranceCount +
            ", updateTime='" + updateTime + '\'' +
            ", systemCreatetime='" + systemCreatetime + '\'' +
            ", systemLastUpdatetime='" + systemLastUpdatetime + '\'' +
            '}';
  }
}
