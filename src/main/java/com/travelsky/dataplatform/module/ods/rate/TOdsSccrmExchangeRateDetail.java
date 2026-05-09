package com.travelsky.dataplatform.module.ods.rate;

import com.alibaba.excel.annotation.ExcelIgnore;
import com.alibaba.excel.annotation.ExcelProperty;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * 汇率表 实体
 */
public class TOdsSccrmExchangeRateDetail implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 主键ID
     */
    @ExcelIgnore
    private String id;

    /**
     * 比价月
     */
    @ExcelProperty("比价月")
    private String exchangeMonth;

    /**
     * 货币代号
     */
    @ExcelProperty("货币代号")
    private String currencyCode;

    /**
     * 本币5天比价
     */
    @ExcelProperty("本币5天比价")
    private BigDecimal fiveDayRate;

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

    public TOdsSccrmExchangeRateDetail() {
    }

    public String getId() {
        return this.id;
    }

    public String getExchangeMonth() {
        return this.exchangeMonth;
    }

    public String getCurrencyCode() {
        return this.currencyCode;
    }

    public BigDecimal getFiveDayRate() {
        return this.fiveDayRate;
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

    public void setId(String id) {
        this.id = id;
    }

    public void setExchangeMonth(String exchangeMonth) {
        this.exchangeMonth = exchangeMonth;
    }

    public void setCurrencyCode(String currencyCode) {
        this.currencyCode = currencyCode;
    }

    public void setFiveDayRate(BigDecimal fiveDayRate) {
        this.fiveDayRate = fiveDayRate;
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
        if (!(o instanceof TOdsSccrmExchangeRateDetail)) return false;
        final TOdsSccrmExchangeRateDetail other = (TOdsSccrmExchangeRateDetail) o;
        if (!other.canEqual((Object) this)) return false;
        final Object this$id = this.getId();
        final Object other$id = other.getId();
        if (this$id == null ? other$id != null : !this$id.equals(other$id)) return false;
        final Object this$exchangeMonth = this.getExchangeMonth();
        final Object other$exchangeMonth = other.getExchangeMonth();
        if (this$exchangeMonth == null ? other$exchangeMonth != null : !this$exchangeMonth.equals(other$exchangeMonth))
            return false;
        final Object this$currencyCode = this.getCurrencyCode();
        final Object other$currencyCode = other.getCurrencyCode();
        if (this$currencyCode == null ? other$currencyCode != null : !this$currencyCode.equals(other$currencyCode))
            return false;
        final Object this$fiveDayRate = this.getFiveDayRate();
        final Object other$fiveDayRate = other.getFiveDayRate();
        if (this$fiveDayRate == null ? other$fiveDayRate != null : !this$fiveDayRate.equals(other$fiveDayRate))
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
        return other instanceof TOdsSccrmExchangeRateDetail;
    }

    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        final Object $id = this.getId();
        result = result * PRIME + ($id == null ? 43 : $id.hashCode());
        final Object $exchangeMonth = this.getExchangeMonth();
        result = result * PRIME + ($exchangeMonth == null ? 43 : $exchangeMonth.hashCode());
        final Object $currencyCode = this.getCurrencyCode();
        result = result * PRIME + ($currencyCode == null ? 43 : $currencyCode.hashCode());
        final Object $fiveDayRate = this.getFiveDayRate();
        result = result * PRIME + ($fiveDayRate == null ? 43 : $fiveDayRate.hashCode());
        final Object $etlCreateTime = this.getEtlCreateTime();
        result = result * PRIME + ($etlCreateTime == null ? 43 : $etlCreateTime.hashCode());
        final Object $etlUpdateTime = this.getEtlUpdateTime();
        result = result * PRIME + ($etlUpdateTime == null ? 43 : $etlUpdateTime.hashCode());
        final Object $etlDate = this.getEtlDate();
        result = result * PRIME + ($etlDate == null ? 43 : $etlDate.hashCode());
        return result;
    }

    public String toString() {
        return "TOdsSccrmExchangeRateDetail(id=" + this.getId() + ", exchangeMonth=" + this.getExchangeMonth() + ", currencyCode=" + this.getCurrencyCode() + ", fiveDayRate=" + this.getFiveDayRate() + ", etlCreateTime=" + this.getEtlCreateTime() + ", etlUpdateTime=" + this.getEtlUpdateTime() + ", etlDate=" + this.getEtlDate() + ")";
    }
}