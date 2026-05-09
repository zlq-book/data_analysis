
package com.travelsky.trp.usercenter.data.analysis.cascrmclient.memberinfoquerynew;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>member complex type的 Java 类。
 * 
 * <p>以下模式片段指定包含在此类中的预期内容。
 * 
 * <pre>
 * &lt;complexType name="member">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="airRedeemQual" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="attribute1" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="caLifetimePoints" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="cardName" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="cardStatus" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="cnFirstName" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="cnLastName" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="credentialList" type="{http://service.memberInfoQueryNew.crm_interface.sda.com/}credentialList" minOccurs="0"/>
 *         &lt;element name="crmMemberId" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="dateofBirth" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="firstName" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="frozenPoints" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="gender" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="joinDate" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="kyLifetimePoints" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="lastName" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="lifetimePoints" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="lifetimeTier" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="lifetimeTierEndDT" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="lifttimeTierCHSDesc" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="lifttimeTierENUDesc" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="memberBrand" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="memberNumber" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="memberType" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="mm" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="nationality" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="nonAirRedeemQual" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="nxLifetimePoints" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="parentMemberId" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="parentMemberNum" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="pinActiveFlg" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="primaryTierName" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="scLifetimePoints" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="securityQuestion" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="spFirstName" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="spLastName" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="status" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="title" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="tsFirstName" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="tsLastName" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="tvLifetimePoints" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="zhLifetimePoints" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "member", propOrder = {
    "airRedeemQual",
    "attribute1",
    "caLifetimePoints",
    "cardName",
    "cardStatus",
    "cnFirstName",
    "cnLastName",
    "credentialList",
    "crmMemberId",
    "dateofBirth",
    "firstName",
    "frozenPoints",
    "gender",
    "joinDate",
    "kyLifetimePoints",
    "lastName",
    "lifetimePoints",
    "lifetimeTier",
    "lifetimeTierEndDT",
    "lifttimeTierCHSDesc",
    "lifttimeTierENUDesc",
    "memberBrand",
    "memberNumber",
    "memberType",
    "mm",
    "nationality",
    "nonAirRedeemQual",
    "nxLifetimePoints",
    "parentMemberId",
    "parentMemberNum",
    "pinActiveFlg",
    "primaryTierName",
    "scLifetimePoints",
    "securityQuestion",
    "spFirstName",
    "spLastName",
    "status",
    "title",
    "tsFirstName",
    "tsLastName",
    "tvLifetimePoints",
    "zhLifetimePoints"
})
public class Member {

    protected String airRedeemQual;
    protected String attribute1;
    protected String caLifetimePoints;
    protected String cardName;
    protected String cardStatus;
    protected String cnFirstName;
    protected String cnLastName;
    protected CredentialList credentialList;
    protected String crmMemberId;
    protected String dateofBirth;
    protected String firstName;
    protected String frozenPoints;
    protected String gender;
    protected String joinDate;
    protected String kyLifetimePoints;
    protected String lastName;
    protected String lifetimePoints;
    protected String lifetimeTier;
    protected String lifetimeTierEndDT;
    protected String lifttimeTierCHSDesc;
    protected String lifttimeTierENUDesc;
    protected String memberBrand;
    protected String memberNumber;
    protected String memberType;
    protected String mm;
    protected String nationality;
    protected String nonAirRedeemQual;
    protected String nxLifetimePoints;
    protected String parentMemberId;
    protected String parentMemberNum;
    protected String pinActiveFlg;
    protected String primaryTierName;
    protected String scLifetimePoints;
    protected String securityQuestion;
    protected String spFirstName;
    protected String spLastName;
    protected String status;
    protected String title;
    protected String tsFirstName;
    protected String tsLastName;
    protected String tvLifetimePoints;
    protected String zhLifetimePoints;

    /**
     * 获取airRedeemQual属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getAirRedeemQual() {
        return airRedeemQual;
    }

    /**
     * 设置airRedeemQual属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setAirRedeemQual(String value) {
        this.airRedeemQual = value;
    }

    /**
     * 获取attribute1属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getAttribute1() {
        return attribute1;
    }

    /**
     * 设置attribute1属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setAttribute1(String value) {
        this.attribute1 = value;
    }

    /**
     * 获取caLifetimePoints属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getCaLifetimePoints() {
        return caLifetimePoints;
    }

    /**
     * 设置caLifetimePoints属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setCaLifetimePoints(String value) {
        this.caLifetimePoints = value;
    }

    /**
     * 获取cardName属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getCardName() {
        return cardName;
    }

    /**
     * 设置cardName属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setCardName(String value) {
        this.cardName = value;
    }

    /**
     * 获取cardStatus属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getCardStatus() {
        return cardStatus;
    }

    /**
     * 设置cardStatus属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setCardStatus(String value) {
        this.cardStatus = value;
    }

    /**
     * 获取cnFirstName属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getCnFirstName() {
        return cnFirstName;
    }

    /**
     * 设置cnFirstName属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setCnFirstName(String value) {
        this.cnFirstName = value;
    }

    /**
     * 获取cnLastName属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getCnLastName() {
        return cnLastName;
    }

    /**
     * 设置cnLastName属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setCnLastName(String value) {
        this.cnLastName = value;
    }

    /**
     * 获取credentialList属性的值。
     * 
     * @return
     *     possible object is
     *     {@link CredentialList }
     *     
     */
    public CredentialList getCredentialList() {
        return credentialList;
    }

    /**
     * 设置credentialList属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link CredentialList }
     *     
     */
    public void setCredentialList(CredentialList value) {
        this.credentialList = value;
    }

    /**
     * 获取crmMemberId属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getCrmMemberId() {
        return crmMemberId;
    }

    /**
     * 设置crmMemberId属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setCrmMemberId(String value) {
        this.crmMemberId = value;
    }

    /**
     * 获取dateofBirth属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getDateofBirth() {
        return dateofBirth;
    }

    /**
     * 设置dateofBirth属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setDateofBirth(String value) {
        this.dateofBirth = value;
    }

    /**
     * 获取firstName属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getFirstName() {
        return firstName;
    }

    /**
     * 设置firstName属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setFirstName(String value) {
        this.firstName = value;
    }

    /**
     * 获取frozenPoints属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getFrozenPoints() {
        return frozenPoints;
    }

    /**
     * 设置frozenPoints属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setFrozenPoints(String value) {
        this.frozenPoints = value;
    }

    /**
     * 获取gender属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getGender() {
        return gender;
    }

    /**
     * 设置gender属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setGender(String value) {
        this.gender = value;
    }

    /**
     * 获取joinDate属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getJoinDate() {
        return joinDate;
    }

    /**
     * 设置joinDate属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setJoinDate(String value) {
        this.joinDate = value;
    }

    /**
     * 获取kyLifetimePoints属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getKyLifetimePoints() {
        return kyLifetimePoints;
    }

    /**
     * 设置kyLifetimePoints属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setKyLifetimePoints(String value) {
        this.kyLifetimePoints = value;
    }

    /**
     * 获取lastName属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getLastName() {
        return lastName;
    }

    /**
     * 设置lastName属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setLastName(String value) {
        this.lastName = value;
    }

    /**
     * 获取lifetimePoints属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getLifetimePoints() {
        return lifetimePoints;
    }

    /**
     * 设置lifetimePoints属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setLifetimePoints(String value) {
        this.lifetimePoints = value;
    }

    /**
     * 获取lifetimeTier属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getLifetimeTier() {
        return lifetimeTier;
    }

    /**
     * 设置lifetimeTier属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setLifetimeTier(String value) {
        this.lifetimeTier = value;
    }

    /**
     * 获取lifetimeTierEndDT属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getLifetimeTierEndDT() {
        return lifetimeTierEndDT;
    }

    /**
     * 设置lifetimeTierEndDT属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setLifetimeTierEndDT(String value) {
        this.lifetimeTierEndDT = value;
    }

    /**
     * 获取lifttimeTierCHSDesc属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getLifttimeTierCHSDesc() {
        return lifttimeTierCHSDesc;
    }

    /**
     * 设置lifttimeTierCHSDesc属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setLifttimeTierCHSDesc(String value) {
        this.lifttimeTierCHSDesc = value;
    }

    /**
     * 获取lifttimeTierENUDesc属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getLifttimeTierENUDesc() {
        return lifttimeTierENUDesc;
    }

    /**
     * 设置lifttimeTierENUDesc属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setLifttimeTierENUDesc(String value) {
        this.lifttimeTierENUDesc = value;
    }

    /**
     * 获取memberBrand属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getMemberBrand() {
        return memberBrand;
    }

    /**
     * 设置memberBrand属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setMemberBrand(String value) {
        this.memberBrand = value;
    }

    /**
     * 获取memberNumber属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getMemberNumber() {
        return memberNumber;
    }

    /**
     * 设置memberNumber属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setMemberNumber(String value) {
        this.memberNumber = value;
    }

    /**
     * 获取memberType属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getMemberType() {
        return memberType;
    }

    /**
     * 设置memberType属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setMemberType(String value) {
        this.memberType = value;
    }

    /**
     * 获取mm属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getMm() {
        return mm;
    }

    /**
     * 设置mm属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setMm(String value) {
        this.mm = value;
    }

    /**
     * 获取nationality属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getNationality() {
        return nationality;
    }

    /**
     * 设置nationality属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setNationality(String value) {
        this.nationality = value;
    }

    /**
     * 获取nonAirRedeemQual属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getNonAirRedeemQual() {
        return nonAirRedeemQual;
    }

    /**
     * 设置nonAirRedeemQual属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setNonAirRedeemQual(String value) {
        this.nonAirRedeemQual = value;
    }

    /**
     * 获取nxLifetimePoints属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getNxLifetimePoints() {
        return nxLifetimePoints;
    }

    /**
     * 设置nxLifetimePoints属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setNxLifetimePoints(String value) {
        this.nxLifetimePoints = value;
    }

    /**
     * 获取parentMemberId属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getParentMemberId() {
        return parentMemberId;
    }

    /**
     * 设置parentMemberId属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setParentMemberId(String value) {
        this.parentMemberId = value;
    }

    /**
     * 获取parentMemberNum属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getParentMemberNum() {
        return parentMemberNum;
    }

    /**
     * 设置parentMemberNum属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setParentMemberNum(String value) {
        this.parentMemberNum = value;
    }

    /**
     * 获取pinActiveFlg属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getPinActiveFlg() {
        return pinActiveFlg;
    }

    /**
     * 设置pinActiveFlg属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setPinActiveFlg(String value) {
        this.pinActiveFlg = value;
    }

    /**
     * 获取primaryTierName属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getPrimaryTierName() {
        return primaryTierName;
    }

    /**
     * 设置primaryTierName属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setPrimaryTierName(String value) {
        this.primaryTierName = value;
    }

    /**
     * 获取scLifetimePoints属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getScLifetimePoints() {
        return scLifetimePoints;
    }

    /**
     * 设置scLifetimePoints属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setScLifetimePoints(String value) {
        this.scLifetimePoints = value;
    }

    /**
     * 获取securityQuestion属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getSecurityQuestion() {
        return securityQuestion;
    }

    /**
     * 设置securityQuestion属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setSecurityQuestion(String value) {
        this.securityQuestion = value;
    }

    /**
     * 获取spFirstName属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getSpFirstName() {
        return spFirstName;
    }

    /**
     * 设置spFirstName属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setSpFirstName(String value) {
        this.spFirstName = value;
    }

    /**
     * 获取spLastName属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getSpLastName() {
        return spLastName;
    }

    /**
     * 设置spLastName属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setSpLastName(String value) {
        this.spLastName = value;
    }

    /**
     * 获取status属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getStatus() {
        return status;
    }

    /**
     * 设置status属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setStatus(String value) {
        this.status = value;
    }

    /**
     * 获取title属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getTitle() {
        return title;
    }

    /**
     * 设置title属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setTitle(String value) {
        this.title = value;
    }

    /**
     * 获取tsFirstName属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getTsFirstName() {
        return tsFirstName;
    }

    /**
     * 设置tsFirstName属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setTsFirstName(String value) {
        this.tsFirstName = value;
    }

    /**
     * 获取tsLastName属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getTsLastName() {
        return tsLastName;
    }

    /**
     * 设置tsLastName属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setTsLastName(String value) {
        this.tsLastName = value;
    }

    /**
     * 获取tvLifetimePoints属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getTvLifetimePoints() {
        return tvLifetimePoints;
    }

    /**
     * 设置tvLifetimePoints属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setTvLifetimePoints(String value) {
        this.tvLifetimePoints = value;
    }

    /**
     * 获取zhLifetimePoints属性的值。
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getZhLifetimePoints() {
        return zhLifetimePoints;
    }

    /**
     * 设置zhLifetimePoints属性的值。
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setZhLifetimePoints(String value) {
        this.zhLifetimePoints = value;
    }

}
