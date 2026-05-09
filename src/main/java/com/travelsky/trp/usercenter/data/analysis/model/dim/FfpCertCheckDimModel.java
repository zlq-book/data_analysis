package com.travelsky.trp.usercenter.data.analysis.model.dim;
import com.fasterxml.jackson.annotation.JsonProperty;
/**
 * T_DIM_FFP_CERT_CHECK_DIM 实体类
 * 对应 Doris 表 T_DIM_FFP_CERT_CHECK_DIM
 * 用户维表-常客卡号证件信息校验表*
 */
public class FfpCertCheckDimModel {

    /**
     * 主键
     */
    @JsonProperty("SYSTEM_KEY")
    private String systemKey;

    /**
     * 常客卡号
     */
    @JsonProperty("MEMBERNUMBER")
    private String memberNumber;

    /**
     * 常客卡级别
     */
    @JsonProperty("MEMBERNUMBER_TYPE")
    private String memberNumberType;

    /**
     * 中文姓名
     */
    @JsonProperty("NAME_CN")
    private String nameCn;

    /**
     * 英文姓名
     */
    @JsonProperty("NAME_EN")
    private String nameEn;

    /**
     * 主要手机号
     */
    @JsonProperty("MOBILE")
    private String mobile;

    /**
     * 主地址
     */
    @JsonProperty("ADDRESS")
    private String address;

    /**
     * 主邮箱
     */
    @JsonProperty("EMAIL")
    private String email;

    /**
     * 证件类型
     */
    @JsonProperty("CREDENTIALTYPE")
    private String credentialType;

    /**
     * 证件号
     */
    @JsonProperty("CREDENTIALNUM")
    private String credentialNum;

    /**
     * 父母常客卡号
     */
    @JsonProperty("PARENTMEMBERNUM")
    private String parentMemberNum;

    /**
     * 更新时间
     */
    @JsonProperty("UPDATE_TIME")
    private String updateTime;

    /**
     * 创建时间
     */
    @JsonProperty("CREATE_TIME")
    private String createTime;

    public String getSystemKey() {
        return systemKey;
    }

    public void setSystemKey(String systemKey) {
        this.systemKey = systemKey;
    }

    public String getMemberNumber() {
        return memberNumber;
    }

    public void setMemberNumber(String memberNumber) {
        this.memberNumber = memberNumber;
    }

    public String getMemberNumberType() {
        return memberNumberType;
    }

    public void setMemberNumberType(String memberNumberType) {
        this.memberNumberType = memberNumberType;
    }

    public String getNameCn() {
        return nameCn;
    }

    public void setNameCn(String nameCn) {
        this.nameCn = nameCn;
    }

    public String getNameEn() {
        return nameEn;
    }

    public void setNameEn(String nameEn) {
        this.nameEn = nameEn;
    }

    public String getMobile() {
        return mobile;
    }

    public void setMobile(String mobile) {
        this.mobile = mobile;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getCredentialType() {
        return credentialType;
    }

    public void setCredentialType(String credentialType) {
        this.credentialType = credentialType;
    }

    public String getCredentialNum() {
        return credentialNum;
    }

    public void setCredentialNum(String credentialNum) {
        this.credentialNum = credentialNum;
    }

    public String getParentMemberNum() {
        return parentMemberNum;
    }

    public void setParentMemberNum(String parentMemberNum) {
        this.parentMemberNum = parentMemberNum;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    @Override
    public String toString() {
        return "TDimFfpCertCheckDim{" +
                "systemKey='" + systemKey + '\'' +
                ", memberNumber='" + memberNumber + '\'' +
                ", memberNumberType='" + memberNumberType + '\'' +
                ", nameCn='" + nameCn + '\'' +
                ", nameEn='" + nameEn + '\'' +
                ", mobile='" + mobile + '\'' +
                ", address='" + address + '\'' +
                ", email='" + email + '\'' +
                ", credentialType='" + credentialType + '\'' +
                ", credentialNum='" + credentialNum + '\'' +
                ", parentMemberNum='" + parentMemberNum + '\'' +
                ", updateTime='" + updateTime + '\'' +
                ", createTime='" + createTime + '\'' +
                '}';
    }
}
