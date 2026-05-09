package com.travelsky.dataplatform.module.ods.clk;

import com.alibaba.excel.annotation.ExcelIgnore;
import com.alibaba.excel.annotation.ExcelProperty;

import java.io.Serializable;

/**
 * 达标人群（无需报名） 实体类
 * 用于读取和写入达标人群无需报名Excel文件
 */
public class TOdsClkQualifiedNoRegistration implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 主键ID
     */
    @ExcelIgnore
    private Long id;


    /**
     * 活动中文名称
     */
    @ExcelProperty("活动中文名称")
    private String activityChineseName;

    /**
     * 活动code
     */
    @ExcelProperty("活动code")
    private String activityCode;

    /**
     * 常客卡号
     */
    @ExcelProperty("常客卡号")
    private String memberCardNumber;

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

    public TOdsClkQualifiedNoRegistration() {
    }

    public Long getId() {
        return this.id;
    }

    public String getActivityChineseName() {
        return this.activityChineseName;
    }

    public String getActivityCode() {
        return this.activityCode;
    }

    public String getMemberCardNumber() {
        return this.memberCardNumber;
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

    public void setActivityChineseName(String activityChineseName) {
        this.activityChineseName = activityChineseName;
    }

    public void setActivityCode(String activityCode) {
        this.activityCode = activityCode;
    }

    public void setMemberCardNumber(String memberCardNumber) {
        this.memberCardNumber = memberCardNumber;
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
        if (!(o instanceof TOdsClkQualifiedNoRegistration)) return false;
        final TOdsClkQualifiedNoRegistration other = (TOdsClkQualifiedNoRegistration) o;
        if (!other.canEqual((Object) this)) return false;
        final Object this$id = this.getId();
        final Object other$id = other.getId();
        if (this$id == null ? other$id != null : !this$id.equals(other$id)) return false;
        final Object this$activityChineseName = this.getActivityChineseName();
        final Object other$activityChineseName = other.getActivityChineseName();
        if (this$activityChineseName == null ? other$activityChineseName != null : !this$activityChineseName.equals(other$activityChineseName))
            return false;
        final Object this$activityCode = this.getActivityCode();
        final Object other$activityCode = other.getActivityCode();
        if (this$activityCode == null ? other$activityCode != null : !this$activityCode.equals(other$activityCode))
            return false;
        final Object this$memberCardNumber = this.getMemberCardNumber();
        final Object other$memberCardNumber = other.getMemberCardNumber();
        if (this$memberCardNumber == null ? other$memberCardNumber != null : !this$memberCardNumber.equals(other$memberCardNumber))
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
        return other instanceof TOdsClkQualifiedNoRegistration;
    }

    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        final Object $id = this.getId();
        result = result * PRIME + ($id == null ? 43 : $id.hashCode());
        final Object $activityChineseName = this.getActivityChineseName();
        result = result * PRIME + ($activityChineseName == null ? 43 : $activityChineseName.hashCode());
        final Object $activityCode = this.getActivityCode();
        result = result * PRIME + ($activityCode == null ? 43 : $activityCode.hashCode());
        final Object $memberCardNumber = this.getMemberCardNumber();
        result = result * PRIME + ($memberCardNumber == null ? 43 : $memberCardNumber.hashCode());
        final Object $etlCreateTime = this.getEtlCreateTime();
        result = result * PRIME + ($etlCreateTime == null ? 43 : $etlCreateTime.hashCode());
        final Object $etlUpdateTime = this.getEtlUpdateTime();
        result = result * PRIME + ($etlUpdateTime == null ? 43 : $etlUpdateTime.hashCode());
        final Object $etlDate = this.getEtlDate();
        result = result * PRIME + ($etlDate == null ? 43 : $etlDate.hashCode());
        return result;
    }

    public String toString() {
        return "TOdsClkQualifiedNoRegistration(id=" + this.getId() + ", activityChineseName=" + this.getActivityChineseName() + ", activityCode=" + this.getActivityCode() + ", memberCardNumber=" + this.getMemberCardNumber() + ", etlCreateTime=" + this.getEtlCreateTime() + ", etlUpdateTime=" + this.getEtlUpdateTime() + ", etlDate=" + this.getEtlDate() + ")";
    }
}