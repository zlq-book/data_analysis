package com.travelsky.trp.usercenter.data.analysis.model.dim;

import com.alibaba.excel.annotation.ExcelProperty;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 机场维表
 */
public class AddressDimModel {

    /** 主键 */
    @JsonProperty("ADDRESS_KEY")
    @ExcelProperty("三字码")
    private String addressKey;

    /** 国 */
    @JsonProperty("COUNTRY")
    @ExcelProperty("国家/地区")
    private String country;

    /** 省 */
    @JsonProperty("PROVINCE")
    @ExcelProperty("省/区")
    private String province;

    /** 市 */
    @JsonProperty("CITY")
    @ExcelProperty("城市")
    private String city;

    /** 机场三字码 */
    @JsonProperty("AIRPORTKEY")
    @ExcelProperty("三字码")
    private String airportkey;

    /** 机场 */
    @JsonProperty("AIRPORT")
    @ExcelProperty("名称")
    private String airport;

    /** 洲 */
    @JsonProperty("ADDRESS_STATE")
    @ExcelProperty("洲")
    private String addressState;

    public String getAddressKey() {
        return addressKey;
    }

    public void setAddressKey(String addressKey) {
        this.addressKey = addressKey;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getAirportkey() {
        return airportkey;
    }

    public void setAirportkey(String airportkey) {
        this.airportkey = airportkey;
    }

    public String getAirport() {
        return airport;
    }

    public void setAirport(String airport) {
        this.airport = airport;
    }

    public String getAddressState() {
        return addressState;
    }

    public void setAddressState(String addressState) {
        this.addressState = addressState;
    }

    @Override
    public String toString() {
        return "AddressDimModel{" +
                "addressKey='" + addressKey + '\'' +
                ", country='" + country + '\'' +
                ", province='" + province + '\'' +
                ", city='" + city + '\'' +
                ", airportkey='" + airportkey + '\'' +
                ", airport='" + airport + '\'' +
                ", addressState='" + addressState + '\'' +
                '}';
    }
}