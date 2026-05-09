package com.travelsky.dataplatform.module.ods.clk;

import com.alibaba.excel.annotation.ExcelIgnore;
import com.alibaba.excel.annotation.ExcelProperty;

import java.math.BigDecimal;

/**
 * 国航系会员发展量 实体类
 * 用于读取和写入国航系会员发展量报表Excel文件
 */
public class TOdsClkMemberDevelopment implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 主键ID
     */
    @ExcelIgnore
    private Long id;

    /**
     * 日期
     */
    @ExcelProperty("日期")
    private String date;

    /**
     * 国
     */
    @ExcelProperty("国")
    private BigDecimal china;

    /**
     * 深
     */
    @ExcelProperty("深")
    private BigDecimal shenzhen;

    /**
     * 山
     */
    @ExcelProperty("山")
    private BigDecimal shandong;

    /**
     * 澳
     */
    @ExcelProperty("澳")
    private BigDecimal macao;

    /**
     * 总发展量
     */
    @ExcelProperty("总发展量")
    private BigDecimal totalDevelopment;

    /**
     * 当日会员总量
     */
    @ExcelProperty("当日会员总量")
    private BigDecimal dailyMemberTotal;

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

    public TOdsClkMemberDevelopment() {
    }

    public Long getId() {
        return this.id;
    }

    public String getDate() {
        return this.date;
    }

    public BigDecimal getChina() {
        return this.china;
    }

    public BigDecimal getShenzhen() {
        return this.shenzhen;
    }

    public BigDecimal getShandong() {
        return this.shandong;
    }

    public BigDecimal getMacao() {
        return this.macao;
    }

    public BigDecimal getTotalDevelopment() {
        return this.totalDevelopment;
    }

    public BigDecimal getDailyMemberTotal() {
        return this.dailyMemberTotal;
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

    public void setDate(String date) {
        this.date = date;
    }

    public void setChina(BigDecimal china) {
        this.china = china;
    }

    public void setShenzhen(BigDecimal shenzhen) {
        this.shenzhen = shenzhen;
    }

    public void setShandong(BigDecimal shandong) {
        this.shandong = shandong;
    }

    public void setMacao(BigDecimal macao) {
        this.macao = macao;
    }

    public void setTotalDevelopment(BigDecimal totalDevelopment) {
        this.totalDevelopment = totalDevelopment;
    }

    public void setDailyMemberTotal(BigDecimal dailyMemberTotal) {
        this.dailyMemberTotal = dailyMemberTotal;
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
        if (!(o instanceof TOdsClkMemberDevelopment)) return false;
        final TOdsClkMemberDevelopment other = (TOdsClkMemberDevelopment) o;
        if (!other.canEqual((Object) this)) return false;
        final Object this$id = this.getId();
        final Object other$id = other.getId();
        if (this$id == null ? other$id != null : !this$id.equals(other$id)) return false;
        final Object this$date = this.getDate();
        final Object other$date = other.getDate();
        if (this$date == null ? other$date != null : !this$date.equals(other$date)) return false;
        final Object this$china = this.getChina();
        final Object other$china = other.getChina();
        if (this$china == null ? other$china != null : !this$china.equals(other$china)) return false;
        final Object this$shenzhen = this.getShenzhen();
        final Object other$shenzhen = other.getShenzhen();
        if (this$shenzhen == null ? other$shenzhen != null : !this$shenzhen.equals(other$shenzhen)) return false;
        final Object this$shandong = this.getShandong();
        final Object other$shandong = other.getShandong();
        if (this$shandong == null ? other$shandong != null : !this$shandong.equals(other$shandong)) return false;
        final Object this$macao = this.getMacao();
        final Object other$macao = other.getMacao();
        if (this$macao == null ? other$macao != null : !this$macao.equals(other$macao)) return false;
        final Object this$totalDevelopment = this.getTotalDevelopment();
        final Object other$totalDevelopment = other.getTotalDevelopment();
        if (this$totalDevelopment == null ? other$totalDevelopment != null : !this$totalDevelopment.equals(other$totalDevelopment))
            return false;
        final Object this$dailyMemberTotal = this.getDailyMemberTotal();
        final Object other$dailyMemberTotal = other.getDailyMemberTotal();
        if (this$dailyMemberTotal == null ? other$dailyMemberTotal != null : !this$dailyMemberTotal.equals(other$dailyMemberTotal))
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
        return other instanceof TOdsClkMemberDevelopment;
    }

    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        final Object $id = this.getId();
        result = result * PRIME + ($id == null ? 43 : $id.hashCode());
        final Object $date = this.getDate();
        result = result * PRIME + ($date == null ? 43 : $date.hashCode());
        final Object $china = this.getChina();
        result = result * PRIME + ($china == null ? 43 : $china.hashCode());
        final Object $shenzhen = this.getShenzhen();
        result = result * PRIME + ($shenzhen == null ? 43 : $shenzhen.hashCode());
        final Object $shandong = this.getShandong();
        result = result * PRIME + ($shandong == null ? 43 : $shandong.hashCode());
        final Object $macao = this.getMacao();
        result = result * PRIME + ($macao == null ? 43 : $macao.hashCode());
        final Object $totalDevelopment = this.getTotalDevelopment();
        result = result * PRIME + ($totalDevelopment == null ? 43 : $totalDevelopment.hashCode());
        final Object $dailyMemberTotal = this.getDailyMemberTotal();
        result = result * PRIME + ($dailyMemberTotal == null ? 43 : $dailyMemberTotal.hashCode());
        final Object $etlCreateTime = this.getEtlCreateTime();
        result = result * PRIME + ($etlCreateTime == null ? 43 : $etlCreateTime.hashCode());
        final Object $etlUpdateTime = this.getEtlUpdateTime();
        result = result * PRIME + ($etlUpdateTime == null ? 43 : $etlUpdateTime.hashCode());
        final Object $etlDate = this.getEtlDate();
        result = result * PRIME + ($etlDate == null ? 43 : $etlDate.hashCode());
        return result;
    }

    public String toString() {
        return "TOdsClkMemberDevelopment(id=" + this.getId() + ", date=" + this.getDate() + ", china=" + this.getChina() + ", shenzhen=" + this.getShenzhen() + ", shandong=" + this.getShandong() + ", macao=" + this.getMacao() + ", totalDevelopment=" + this.getTotalDevelopment() + ", dailyMemberTotal=" + this.getDailyMemberTotal() + ", etlCreateTime=" + this.getEtlCreateTime() + ", etlUpdateTime=" + this.getEtlUpdateTime() + ", etlDate=" + this.getEtlDate() + ")";
    }
}