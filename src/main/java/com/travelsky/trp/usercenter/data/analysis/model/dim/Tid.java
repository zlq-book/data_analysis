package com.travelsky.trp.usercenter.data.analysis.model.dim;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * @author kuangaihua
 * @date 2025/7/23 9:11
 */
public class Tid {
    //    用户的唯一标识符
    private String tid;
    //    直销CUSTOMER_ID
    @JsonProperty("CRM_CUSTOMER_ID")
    private String crmCustomerId;
    //    鲁雁行卡号
    @JsonProperty("LY_CARD_NUMBER")
    private String lyCardNumber;
    //证件号map(证件类型,证件号)
    private Map<String, String> certification;
    //    手机号
    @JsonProperty("MOBILE_PHONE")
    private String mobilePhone;
    //    常客卡号
    @JsonProperty("FREQUENT_TRAVELER_CARDNO")
    private String frequentTravelerCardno;

    public Tid() {
    }

    @Override
    public String toString() {
        return "Tid{" +
                "tid='" + tid + '\'' +
                ", crmCustomerId='" + crmCustomerId + '\'' +
                ", lyCardNumber='" + lyCardNumber + '\'' +
                ", certification=" + certification +
                ", mobilePhone='" + mobilePhone + '\'' +
                ", frequentTravelerCardno='" + frequentTravelerCardno + '\'' +
                '}';
    }

    public String getTid() {
        return tid;
    }

    public void setTid(String tid) {
        this.tid = tid;
    }

    public String getCrmCustomerId() {
        return crmCustomerId;
    }

    public void setCrmCustomerId(String crmCustomerId) {
        this.crmCustomerId = crmCustomerId;
    }

    public String getLyCardNumber() {
        return lyCardNumber;
    }

    public void setLyCardNumber(String lyCardNumber) {
        this.lyCardNumber = lyCardNumber;
    }

    public Map<String, String> getCertification() {
        return certification;
    }

    public void setCertification(Map<String, String> certification) {
        this.certification = certification;
    }

    public String getMobilePhone() {
        return mobilePhone;
    }

    public void setMobilePhone(String mobilePhone) {
        this.mobilePhone = mobilePhone;
    }

    public String getFrequentTravelerCardno() {
        return frequentTravelerCardno;
    }

    public void setFrequentTravelerCardno(String frequentTravelerCardno) {
        this.frequentTravelerCardno = frequentTravelerCardno;
    }
}
