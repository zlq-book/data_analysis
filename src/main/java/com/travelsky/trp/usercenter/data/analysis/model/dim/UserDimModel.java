package com.travelsky.trp.usercenter.data.analysis.model.dim;

import java.time.LocalDate;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.travelsky.dataplatform.utils.DateTimeUtils;

/**
 * 用户维表 DateTime --> String
 */
public class UserDimModel {

    /**
     * 用户T_ID
     */
    @JsonProperty("PK_ID")
    private String pkId;

    /**
     * 直销系统用户ID
     */
    @JsonProperty("CRM_CUSTOMER_ID")
    private String crmCustomerId;

    /**
     * 鲁雁行用户ID
     */
    @JsonProperty("LY_MEMBER_ID")
    private String lyMemberId;

    /**
     * 鲁雁管家高端旅客用户ID
     */
    @JsonProperty("LY_VIP_ID")
    private String lyVipId;

    /**
     * 会员天地用户id
     */
    @JsonProperty("SYS_REGISTER_ID")
    private String sysRegisterId;

    /**
     * 机上注册常客用户id
     */
    @JsonProperty("FFP_REGISTER_ID")
    private String ffpRegisterId;

    /**
     * 2023年12月31日前常客用户id
     */
    @JsonProperty("HISTORICAL_FFP_ID")
    private String historicalFfpId;

    /**
     * 外部账号-支付宝
     */
    @JsonProperty("ALIPAY_ID")
    private String alipayId;

    /**
     * 外部账号-微信
     */
    @JsonProperty("WECHAT_ID")
    private String wechatId;

    /**
     * 外部账号-抖音
     */
    @JsonProperty("DOUYIN_ID")
    private String douyinId;

    /**
     * 中文姓名
     */
    @JsonProperty("CN_NAME")
    private String cnName;

    /**
     * 英文姓名
     */
    @JsonProperty("EN_NAME")
    private String enName;

    /**
     * 性别
     */
    @JsonProperty("SEX")
    private String sex;

    /**
     * 用户类型
     */
    @JsonProperty("USER_TYPE")
    private String userType;

    /**
     * 生日
     */
    @JsonProperty("BIRTHDAY")
    private String birthday;

    /**
     * 省份
     */
    @JsonProperty("PROVINCE")
    private String province;

    /**
     * 城市（户籍）
     */
    @JsonProperty("CITY")
    private String city;

    /**
     * 国籍
     */
    @JsonProperty("NATIONALITY")
    private String nationality;

    /**
     * 民族
     */
    @JsonProperty("ETHNICITY")
    private String ethnicity;

    /**
     * 工作单位
     */
    @JsonProperty("EMPLOYER")
    private String employer;

    /**
     * 主手机号
     */
    @JsonProperty("MOBILE_PHONE")
    private String mobilePhone;

    /**
     * 主手机号可信度
     */
    @JsonProperty("MOBILE_NUMBER_RELIABILITY")
    private String mobileNumberReliability;

    /**
     * 邮箱
     */
    @JsonProperty("EMAIL")
    private String email;

    /**
     * 身份证
     */
    @JsonProperty("ID_CARD")
    private String idCard;

    /**
     * 护照
     */
    @JsonProperty("PASSPORT")
    private String passport;

    /**
     * 海员证
     */
    @JsonProperty("SEAMAN_ID")
    private String seamanId;

    /**
     * 外国人出入境证
     */
    @JsonProperty("ALIEN_PERMIT")
    private String alienPermit;

    /**
     * 外交部签发的驻华外交人员证
     */
    @JsonProperty("DIPLOMATIC_STAFF_CERTIFICATE")
    private String diplomaticStaffCertificate;

    /**
     * 外国人永久居留证
     */
    @JsonProperty("PERMANENT_RESIDENT_ID")
    private String permanentResidentId;

    /**
     * 文职人员证
     */
    @JsonProperty("CIVILIAN_STAFF_ID")
    private String civilianStaffId;

    /**
     * 职工证
     */
    @JsonProperty("STAFF_ID")
    private String staffId;

    /**
     * 军官证
     */
    @JsonProperty("OFFICER_ID_CARD")
    private String officerIdCard;

    /**
     * 武警警官证
     */
    @JsonProperty("ARMED_POLICE_OFFICER")
    private String armedPoliceOfficer;

    /**
     * 武警士兵证
     */
    @JsonProperty("ARMED_POLICE_SOLDIER")
    private String armedPoliceSoldier;

    /**
     * 文职干部证
     */
    @JsonProperty("CIVILIAN_OFFICIAL_ID")
    private String civilianOfficialId;

    /**
     * 义务兵证
     */
    @JsonProperty("CONSCRIPT_SOLDIER_ID")
    private String conscriptSoldierId;

    /**
     * 士官证
     */
    @JsonProperty("NON_COMMISSIONED_OFFICER_ID")
    private String nonCommissionedOfficerId;

    /**
     * 港澳居民来往内地通行证
     */
    @JsonProperty("HK_MACAO_RESIDENT_PERMIT")
    private String hkMacaoResidentPermit;

    /**
     * 台湾居民来往大陆通行证
     */
    @JsonProperty("TAIWAN_RESIDENT_TRAVEL_PERMIT")
    private String taiwanResidentTravelPermit;

    /**
     * 盲人旅客标识
     */
    @JsonProperty("BLIND_PASSENGER")
    private Boolean blindPassenger;

    /**
     * 失聪旅客标识
     */
    @JsonProperty("DEAF_PASSENGER")
    private Boolean deafPassenger;

    /**
     * 是否直销用户
     */
    @JsonProperty("IS_DIRECT_USER")
    private Boolean isDirectUser;

    /**
     * 是否官网非注册用户
     */
    @JsonProperty("IS_OFFICIAL_WEBSITE_NON_REGISTERED")
    private Boolean isOfficialWebsiteNonRegistered;

    /**
     * 是否抖音次卡购买人
     */
    @JsonProperty("IS_DOUYIN_CARD_PURCHASER")
    private Boolean isDouyinCardPurchaser;

    /**
     * 直销用户状态
     */
    @JsonProperty("DIRECT_USER_STATUS")
    private String directUserStatus;

    /**
     * 最后登录时间
     */
    @JsonProperty("LAST_LOGIN_TIME")
    private String lastLoginTime;

    /**
     * 直销用户注册日期
     */
    @JsonProperty("DIRECT_REGISTER_DATE")
    private String directRegisterDate;

    /**
     * 直销最后一次登录日期
     */
    @JsonProperty("DIRECT_LASTLOGIN_DATE")
    private String directLastloginDate;

    /**
     * 直销实名认证标识
     */
    @JsonProperty("DIRECT_VERIFIED_FLAG")
    private Boolean directVerifiedFlag;

    /**
     * 直销实名认证日期
     */
    @JsonProperty("DIRECT_VERIFY_DATE")
    private String directVerifyDate;

    /**
     * 是否直销黑名单用户
     */
    @JsonProperty("IS_BLACKLIST_USER")
    private Boolean isBlacklistUser;

    /**
     * 学生标识
     */
    @JsonProperty("STUDENT_FLAG")
    private Boolean studentFlag;

    /**
     * 教师标识
     */
    @JsonProperty("TEACHER_FLAG")
    private Boolean teacherFlag;

    /**
     * 代理人标识
     */
    @JsonProperty("AGENT_FLAG")
    private Boolean agentFlag;

    /**
     * 是否大客户
     */
    @JsonProperty("IS_KEY_ACCOUNT")
    private Boolean isKeyAccount;

    /**
     * 大客户号
     */
    @JsonProperty("KEY_ACCOUNT_NUMBER")
    private String keyAccountNumber;

    /**
     * 是否常旅客
     */
    @JsonProperty("IS_FREQUENT_TRAVELER")
    private Boolean isFrequentTraveler;

    /**
     * 常旅客卡号
     */
    @JsonProperty("FREQUENT_TRAVELER_CARDNO")
    private String frequentTravelerCardno;

    /**
     * 常旅客等级
     */
    @JsonProperty("FREQUENT_TRAVELER_LEVEL")
    private String frequentTravelerLevel;

    /**
     * 援疆卡卡号
     */
    @JsonProperty("YJ_CARD_NUMBER")
    private String yjCardNumber;

    /**
     * 援疆卡有效截止期
     */
    @JsonProperty("YJ_CARD_EXPIREDATE")
    private String yjCardExpiredate;

    /**
     * 常旅客发展渠道（一级）
     */
    @JsonProperty("DEV_CHANNEL_ONE")
    private String devChannelOne;

    /**
     * 常旅客发展渠道（二级）
     */
    @JsonProperty("DEV_CHANNEL_TWO")
    private String devChannelTwo;

    /**
     * 常旅客发展渠道（三级）
     */
    @JsonProperty("DEV_CHANNEL_THREE")
    private String devChannelThree;

    /**
     * 常旅客发展渠道（四级）
     */
    @JsonProperty("DEV_CHANNEL_FOUR")
    private String devChannelFour;

    /**
     * 注册常旅客时间
     */
    @JsonProperty("FT_REGISTER_TIME")
    private String ftRegisterTime;

    /**
     * 是否接受短信营销
     */
    @JsonProperty("ACCEPT_SMS_MARKETING")
    private Boolean acceptSmsMarketing;

    /**
     * 是否接受邮件营销
     */
    @JsonProperty("ACCEPT_EMAIL_MARKETING")
    private Boolean acceptEmailMarketing;

    /**
     * 父母常客卡号
     */
    @JsonProperty("PARENT_FT_CARDNO")
    private String parentFtCardno;

    /**
     * 是否鲁雁行用户
     */
    @JsonProperty("IS_LY_USER")
    private Boolean isLyUser;

    /**
     * 鲁雁行注册时间
     */
    @JsonProperty("LY_REGISTER_TIME")
    private String lyRegisterTime;

    /**
     * 鲁雁行卡号
     */
    @JsonProperty("LY_CARD_NUMBER")
    private String lyCardNumber;

    /**
     * 鲁雁行用户等级
     */
    @JsonProperty("LY_USER_LEVEL")
    private String lyUserLevel;

    /**
     * 鲁雁行用户状态
     */
    @JsonProperty("LY_USER_STATUS")
    private String lyUserStatus;

    /**
     * 鲁雁行注册状态
     */
    @JsonProperty("LY_REGISTER_STATUS")
    private String lyRegisterStatus;

    /**
     * 鲁雁行实名认证状态
     */
    @JsonProperty("LY_VERIFY_STATUS")
    private String lyVerifyStatus;

    /**
     * 生命周期鲁雁值
     */
    @JsonProperty("LY_LIFETIME_POINTS")
    private Integer lyLifetimePoints;

    /**
     * 可用鲁雁值
     */
    @JsonProperty("LY_AVAILABLE_POINTS")
    private Integer lyAvailablePoints;

    /**
     * 是否高端旅客
     */
    @JsonProperty("IS_HIGH_TRAVELER")
    private Boolean isHighTraveler;

    /**
     * 高端旅客类型
     */
    @JsonProperty("HIGH_TRAVELER_TYPE")
    private String highTravelerType;

    /**
     * 高端旅客分层类型
     */
    @JsonProperty("HIGH_TRAVELER_TIER")
    private String highTravelerTier;

    /**
     * 至尊身份分层有效期
     */
    @JsonProperty("TIER_PRESTIGE_EXPIREDATE")
    private String tierPrestigeExpiredate;

    /**
     * 荣耀身份分层有效期
     */
    @JsonProperty("TIER_HONOR_EXPIREDATE")
    private String tierHonorExpiredate;

    /**
     * 高端旅客数据来源
     */
    @JsonProperty("HIGH_TRAVELER_DS")
    private String highTravelerDs;

    /**
     * 是否援疆人员
     */
    @JsonProperty("IS_YJ_PERSONNEL")
    private Boolean isYjPersonnel;

    /**
     * VIP标识
     */
    @JsonProperty("VIP_FLAG")
    private Boolean vipFlag;

    /**
     * CIP标识
     */
    @JsonProperty("CIP_FLAG")
    private Boolean cipFlag;

    /**
     * VVIP标识
     */
    @JsonProperty("VVIP_FLAG")
    private Boolean vvipFlag;

    /**
     * 餐食喜好
     */
    @JsonProperty("FOOD_PREFERENCE")
    private String foodPreference;

    /**
     * 座位喜好
     */
    @JsonProperty("SEAT_PREFERENCE")
    private String seatPreference;

    /**
     * 临时餐食喜好
     */
    @JsonProperty("TEMP_FOOD_PREFERENCE")
    private String tempFoodPreference;

    /**
     * 临时座位喜好
     */
    @JsonProperty("TEMP_SEAT_PREFERENCE")
    private String tempSeatPreference;

    /**
     * 饮品喜好
     */
    @JsonProperty("BEVERAGE_PREFERENCE")
    private String beveragePreference;

    /**
     * 长期座位喜好
     */
    @JsonProperty("LONG_TERM_SEAT_PREFERENCE")
    private String longTermSeatPreference;

    /**
     * 头等舱休息室喜好
     */
    @JsonProperty("FIRST_CLASS_LOUNGE_PREFERENCE")
    private String firstClassLoungePreference;

    /**
     * 合并至用户ID
     */
    @JsonProperty("MERGED_TO_USERID")
    private String mergedToUserid;

    /**
     * 是否有效用户
     */
    @JsonProperty("IS_VALID_USER")
    private int isValidUser=0;

    /**
     * 常客卡号是否经过验证
     */
    @JsonProperty("IS_FREQUENT_FLYER_NUMBER_VERIFIED")
    private Boolean isFrequentFlyerNumberVerified;

    /**
     * 创建时间
     */
    @JsonProperty("CREATE_TIME")
    private String createTime = DateTimeUtils.getCurrentDateTime();

    /**
     * 更新时间
     */
    @JsonProperty("UPDATE_TIME")
    private String updateTime = DateTimeUtils.getCurrentDateTime();


    /**
     * 港澳通行证
     */
    @JsonProperty("HK_MACAO_TRAVEL_PERMIT")
    private String hkMacaoTravelPermit;
    /**
     * 大陆居民往来台湾通行证
     */
    @JsonProperty("MAINLAND_TO_TAIWAN_TRAVEL_PERMIT")
    private String mainlandToTaiwanTravelPermit;
    /**
     * 港澳台居民居住证
     */
    @JsonProperty("HK_MACAO_TAIWAN_RESIDENCE_PERMIT")
    private String hkMacaoTaiwanIdCard;
    /**
     * 港澳台居民身份证
     */
    @JsonProperty("HK_MACAO_TAIWAN_ID_CARD")
    private String hkMacaoTaiwanResidencePermit;
    /**
     * 是否军人大客户
     */
    @JsonProperty("IS_CORP_TRAVEL_MILITARY_CARD")
    private Boolean isCorpTravelMilitaryCard;

    public String getPkId() {
        return pkId;
    }

    public void setPkId(String pkId) {
        this.pkId = pkId;
    }

    public String getCrmCustomerId() {
        return crmCustomerId;
    }

    public void setCrmCustomerId(String crmCustomerId) {
        this.crmCustomerId = crmCustomerId;
    }

    public String getLyMemberId() {
        return lyMemberId;
    }

    public void setLyMemberId(String lyMemberId) {
        this.lyMemberId = lyMemberId;
    }

    public String getLyVipId() {
        return lyVipId;
    }

    public void setLyVipId(String lyVipId) {
        this.lyVipId = lyVipId;
    }

    public String getSysRegisterId() {
        return sysRegisterId;
    }

    public void setSysRegisterId(String sysRegisterId) {
        this.sysRegisterId = sysRegisterId;
    }

    public String getFfpRegisterId() {
        return ffpRegisterId;
    }

    public void setFfpRegisterId(String ffpRegisterId) {
        this.ffpRegisterId = ffpRegisterId;
    }

    public String getHistoricalFfpId() {
        return historicalFfpId;
    }

    public void setHistoricalFfpId(String historicalFfpId) {
        this.historicalFfpId = historicalFfpId;
    }

    public String getAlipayId() {
        return alipayId;
    }

    public void setAlipayId(String alipayId) {
        this.alipayId = alipayId;
    }

    public String getWechatId() {
        return wechatId;
    }

    public void setWechatId(String wechatId) {
        this.wechatId = wechatId;
    }

    public String getDouyinId() {
        return douyinId;
    }

    public void setDouyinId(String douyinId) {
        this.douyinId = douyinId;
    }

    public String getCnName() {
        return cnName;
    }

    public void setCnName(String cnName) {
        this.cnName = cnName;
    }

    public String getEnName() {
        return enName;
    }

    public void setEnName(String enName) {
        this.enName = enName;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public String getUserType() {
        return userType;
    }

    public void setUserType(String userType) {
        this.userType = userType;
    }

    public String getBirthday() {
        return birthday;
    }

    public void setBirthday(String birthday) {
        this.birthday = birthday;
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

    public String getNationality() {
        return nationality;
    }

    public void setNationality(String nationality) {
        this.nationality = nationality;
    }

    public String getEthnicity() {
        return ethnicity;
    }

    public void setEthnicity(String ethnicity) {
        this.ethnicity = ethnicity;
    }

    public String getEmployer() {
        return employer;
    }

    public void setEmployer(String employer) {
        this.employer = employer;
    }

    public String getMobilePhone() {
        return mobilePhone;
    }

    public void setMobilePhone(String mobilePhone) {
        this.mobilePhone = mobilePhone;
    }

    public String getMobileNumberReliability() {
        return mobileNumberReliability;
    }

    public void setMobileNumberReliability(String mobileNumberReliability) {
        this.mobileNumberReliability = mobileNumberReliability;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getIdCard() {
        return idCard;
    }

    public void setIdCard(String idCard) {
        this.idCard = idCard;
    }

    public String getPassport() {
        return passport;
    }

    public void setPassport(String passport) {
        this.passport = passport;
    }

    public String getSeamanId() {
        return seamanId;
    }

    public void setSeamanId(String seamanId) {
        this.seamanId = seamanId;
    }

    public String getAlienPermit() {
        return alienPermit;
    }

    public void setAlienPermit(String alienPermit) {
        this.alienPermit = alienPermit;
    }

    public String getDiplomaticStaffCertificate() {
        return diplomaticStaffCertificate;
    }

    public void setDiplomaticStaffCertificate(String diplomaticStaffCertificate) {
        this.diplomaticStaffCertificate = diplomaticStaffCertificate;
    }

    public String getPermanentResidentId() {
        return permanentResidentId;
    }

    public void setPermanentResidentId(String permanentResidentId) {
        this.permanentResidentId = permanentResidentId;
    }

    public String getCivilianStaffId() {
        return civilianStaffId;
    }

    public void setCivilianStaffId(String civilianStaffId) {
        this.civilianStaffId = civilianStaffId;
    }

    public String getStaffId() {
        return staffId;
    }

    public void setStaffId(String staffId) {
        this.staffId = staffId;
    }

    public String getOfficerIdCard() {
        return officerIdCard;
    }

    public void setOfficerIdCard(String officerIdCard) {
        this.officerIdCard = officerIdCard;
    }

    public String getArmedPoliceOfficer() {
        return armedPoliceOfficer;
    }

    public void setArmedPoliceOfficer(String armedPoliceOfficer) {
        this.armedPoliceOfficer = armedPoliceOfficer;
    }

    public String getArmedPoliceSoldier() {
        return armedPoliceSoldier;
    }

    public void setArmedPoliceSoldier(String armedPoliceSoldier) {
        this.armedPoliceSoldier = armedPoliceSoldier;
    }

    public String getCivilianOfficialId() {
        return civilianOfficialId;
    }

    public void setCivilianOfficialId(String civilianOfficialId) {
        this.civilianOfficialId = civilianOfficialId;
    }

    public String getConscriptSoldierId() {
        return conscriptSoldierId;
    }

    public void setConscriptSoldierId(String conscriptSoldierId) {
        this.conscriptSoldierId = conscriptSoldierId;
    }

    public String getNonCommissionedOfficerId() {
        return nonCommissionedOfficerId;
    }

    public void setNonCommissionedOfficerId(String nonCommissionedOfficerId) {
        this.nonCommissionedOfficerId = nonCommissionedOfficerId;
    }

    public String getHkMacaoResidentPermit() {
        return hkMacaoResidentPermit;
    }

    public void setHkMacaoResidentPermit(String hkMacaoResidentPermit) {
        this.hkMacaoResidentPermit = hkMacaoResidentPermit;
    }

    public String getTaiwanResidentTravelPermit() {
        return taiwanResidentTravelPermit;
    }

    public void setTaiwanResidentTravelPermit(String taiwanResidentTravelPermit) {
        this.taiwanResidentTravelPermit = taiwanResidentTravelPermit;
    }

    public Boolean getBlindPassenger() {
        return blindPassenger;
    }

    public void setBlindPassenger(Boolean blindPassenger) {
        this.blindPassenger = blindPassenger;
    }

    public Boolean getDeafPassenger() {
        return deafPassenger;
    }

    public void setDeafPassenger(Boolean deafPassenger) {
        this.deafPassenger = deafPassenger;
    }

    public Boolean getDirectUser() {
        return isDirectUser;
    }

    public void setDirectUser(Boolean directUser) {
        isDirectUser = directUser;
    }

    public Boolean getOfficialWebsiteNonRegistered() {
        return isOfficialWebsiteNonRegistered;
    }

    public void setOfficialWebsiteNonRegistered(Boolean officialWebsiteNonRegistered) {
        isOfficialWebsiteNonRegistered = officialWebsiteNonRegistered;
    }

    public Boolean getDouyinCardPurchaser() {
        return isDouyinCardPurchaser;
    }

    public void setDouyinCardPurchaser(Boolean douyinCardPurchaser) {
        isDouyinCardPurchaser = douyinCardPurchaser;
    }

    public String getDirectUserStatus() {
        return directUserStatus;
    }

    public void setDirectUserStatus(String directUserStatus) {
        this.directUserStatus = directUserStatus;
    }

    public String getLastLoginTime() {
        return lastLoginTime;
    }

    public void setLastLoginTime(String lastLoginTime) {
        this.lastLoginTime = lastLoginTime;
    }

    public String getDirectRegisterDate() {
        return directRegisterDate;
    }

    public void setDirectRegisterDate(String directRegisterDate) {
        this.directRegisterDate = directRegisterDate;
    }

    public String getDirectLastloginDate() {
        return directLastloginDate;
    }

    public void setDirectLastloginDate(String directLastloginDate) {
        this.directLastloginDate = directLastloginDate;
    }

    public Boolean getDirectVerifiedFlag() {
        return directVerifiedFlag;
    }

    public void setDirectVerifiedFlag(Boolean directVerifiedFlag) {
        this.directVerifiedFlag = directVerifiedFlag;
    }

    public String getDirectVerifyDate() {
        return directVerifyDate;
    }

    public void setDirectVerifyDate(String directVerifyDate) {
        this.directVerifyDate = directVerifyDate;
    }

    public Boolean getBlacklistUser() {
        return isBlacklistUser;
    }

    public void setBlacklistUser(Boolean blacklistUser) {
        isBlacklistUser = blacklistUser;
    }

    public Boolean getStudentFlag() {
        return studentFlag;
    }

    public void setStudentFlag(Boolean studentFlag) {
        this.studentFlag = studentFlag;
    }

    public Boolean getTeacherFlag() {
        return teacherFlag;
    }

    public void setTeacherFlag(Boolean teacherFlag) {
        this.teacherFlag = teacherFlag;
    }

    public Boolean getAgentFlag() {
        return agentFlag;
    }

    public void setAgentFlag(Boolean agentFlag) {
        this.agentFlag = agentFlag;
    }

    public Boolean getKeyAccount() {
        return isKeyAccount;
    }

    public void setKeyAccount(Boolean keyAccount) {
        isKeyAccount = keyAccount;
    }

    public String getKeyAccountNumber() {
        return keyAccountNumber;
    }

    public void setKeyAccountNumber(String keyAccountNumber) {
        this.keyAccountNumber = keyAccountNumber;
    }

    public Boolean getFrequentTraveler() {
        return isFrequentTraveler;
    }

    public void setFrequentTraveler(Boolean frequentTraveler) {
        isFrequentTraveler = frequentTraveler;
    }

    public String getFrequentTravelerCardno() {
        return frequentTravelerCardno;
    }

    public void setFrequentTravelerCardno(String frequentTravelerCardno) {
        this.frequentTravelerCardno = frequentTravelerCardno;
    }

    public String getFrequentTravelerLevel() {
        return frequentTravelerLevel;
    }

    public void setFrequentTravelerLevel(String frequentTravelerLevel) {
        this.frequentTravelerLevel = frequentTravelerLevel;
    }

    public String getYjCardNumber() {
        return yjCardNumber;
    }

    public void setYjCardNumber(String yjCardNumber) {
        this.yjCardNumber = yjCardNumber;
    }

    public String getYjCardExpiredate() {
        return yjCardExpiredate;
    }

    public void setYjCardExpiredate(String yjCardExpiredate) {
        this.yjCardExpiredate = yjCardExpiredate;
    }

    public String getDevChannelOne() {
        return devChannelOne;
    }

    public void setDevChannelOne(String devChannelOne) {
        this.devChannelOne = devChannelOne;
    }

    public String getDevChannelTwo() {
        return devChannelTwo;
    }

    public void setDevChannelTwo(String devChannelTwo) {
        this.devChannelTwo = devChannelTwo;
    }

    public String getDevChannelThree() {
        return devChannelThree;
    }

    public void setDevChannelThree(String devChannelThree) {
        this.devChannelThree = devChannelThree;
    }

    public String getDevChannelFour() {
        return devChannelFour;
    }

    public void setDevChannelFour(String devChannelFour) {
        this.devChannelFour = devChannelFour;
    }

    public String getFtRegisterTime() {
        return ftRegisterTime;
    }

    public void setFtRegisterTime(String ftRegisterTime) {
        this.ftRegisterTime = ftRegisterTime;
    }

    public Boolean getAcceptSmsMarketing() {
        return acceptSmsMarketing;
    }

    public void setAcceptSmsMarketing(Boolean acceptSmsMarketing) {
        this.acceptSmsMarketing = acceptSmsMarketing;
    }

    public Boolean getAcceptEmailMarketing() {
        return acceptEmailMarketing;
    }

    public void setAcceptEmailMarketing(Boolean acceptEmailMarketing) {
        this.acceptEmailMarketing = acceptEmailMarketing;
    }

    public String getParentFtCardno() {
        return parentFtCardno;
    }

    public void setParentFtCardno(String parentFtCardno) {
        this.parentFtCardno = parentFtCardno;
    }

    public Boolean getLyUser() {
        return isLyUser;
    }

    public void setLyUser(Boolean lyUser) {
        isLyUser = lyUser;
    }

    public String getLyRegisterTime() {
        return lyRegisterTime;
    }

    public void setLyRegisterTime(String lyRegisterTime) {
        this.lyRegisterTime = lyRegisterTime;
    }

    public String getLyCardNumber() {
        return lyCardNumber;
    }

    public void setLyCardNumber(String lyCardNumber) {
        this.lyCardNumber = lyCardNumber;
    }

    public String getLyUserLevel() {
        return lyUserLevel;
    }

    public void setLyUserLevel(String lyUserLevel) {
        this.lyUserLevel = lyUserLevel;
    }

    public String getLyUserStatus() {
        return lyUserStatus;
    }

    public void setLyUserStatus(String lyUserStatus) {
        this.lyUserStatus = lyUserStatus;
    }

    public String getLyRegisterStatus() {
        return lyRegisterStatus;
    }

    public void setLyRegisterStatus(String lyRegisterStatus) {
        this.lyRegisterStatus = lyRegisterStatus;
    }

    public String getLyVerifyStatus() {
        return lyVerifyStatus;
    }

    public void setLyVerifyStatus(String lyVerifyStatus) {
        this.lyVerifyStatus = lyVerifyStatus;
    }

    public Integer getLyLifetimePoints() {
        return lyLifetimePoints;
    }

    public void setLyLifetimePoints(Integer lyLifetimePoints) {
        this.lyLifetimePoints = lyLifetimePoints;
    }

    public Integer getLyAvailablePoints() {
        return lyAvailablePoints;
    }

    public void setLyAvailablePoints(Integer lyAvailablePoints) {
        this.lyAvailablePoints = lyAvailablePoints;
    }

    public Boolean getHighTraveler() {
        return isHighTraveler;
    }

    public void setHighTraveler(Boolean highTraveler) {
        isHighTraveler = highTraveler;
    }

    public String getHighTravelerType() {
        return highTravelerType;
    }

    public void setHighTravelerType(String highTravelerType) {
        this.highTravelerType = highTravelerType;
    }

    public String getHighTravelerTier() {
        return highTravelerTier;
    }

    public void setHighTravelerTier(String highTravelerTier) {
        this.highTravelerTier = highTravelerTier;
    }

    public String getTierPrestigeExpiredate() {
        return tierPrestigeExpiredate;
    }

    public void setTierPrestigeExpiredate(String tierPrestigeExpiredate) {
        this.tierPrestigeExpiredate = tierPrestigeExpiredate;
    }

    public String getTierHonorExpiredate() {
        return tierHonorExpiredate;
    }

    public void setTierHonorExpiredate(String tierHonorExpiredate) {
        this.tierHonorExpiredate = tierHonorExpiredate;
    }

    public String getHighTravelerDs() {
        return highTravelerDs;
    }

    public void setHighTravelerDs(String highTravelerDs) {
        this.highTravelerDs = highTravelerDs;
    }

    public Boolean getYjPersonnel() {
        return isYjPersonnel;
    }

    public void setYjPersonnel(Boolean yjPersonnel) {
        isYjPersonnel = yjPersonnel;
    }

    public Boolean getVipFlag() {
        return vipFlag;
    }

    public void setVipFlag(Boolean vipFlag) {
        this.vipFlag = vipFlag;
    }

    public Boolean getCipFlag() {
        return cipFlag;
    }

    public void setCipFlag(Boolean cipFlag) {
        this.cipFlag = cipFlag;
    }

    public Boolean getVvipFlag() {
        return vvipFlag;
    }

    public void setVvipFlag(Boolean vvipFlag) {
        this.vvipFlag = vvipFlag;
    }

    public String getFoodPreference() {
        return foodPreference;
    }

    public void setFoodPreference(String foodPreference) {
        this.foodPreference = foodPreference;
    }

    public String getSeatPreference() {
        return seatPreference;
    }

    public void setSeatPreference(String seatPreference) {
        this.seatPreference = seatPreference;
    }

    public String getTempFoodPreference() {
        return tempFoodPreference;
    }

    public void setTempFoodPreference(String tempFoodPreference) {
        this.tempFoodPreference = tempFoodPreference;
    }

    public String getTempSeatPreference() {
        return tempSeatPreference;
    }

    public void setTempSeatPreference(String tempSeatPreference) {
        this.tempSeatPreference = tempSeatPreference;
    }

    public String getBeveragePreference() {
        return beveragePreference;
    }

    public void setBeveragePreference(String beveragePreference) {
        this.beveragePreference = beveragePreference;
    }

    public String getLongTermSeatPreference() {
        return longTermSeatPreference;
    }

    public void setLongTermSeatPreference(String longTermSeatPreference) {
        this.longTermSeatPreference = longTermSeatPreference;
    }

    public String getFirstClassLoungePreference() {
        return firstClassLoungePreference;
    }

    public void setFirstClassLoungePreference(String firstClassLoungePreference) {
        this.firstClassLoungePreference = firstClassLoungePreference;
    }

    public String getMergedToUserid() {
        return mergedToUserid;
    }

    public void setMergedToUserid(String mergedToUserid) {
        this.mergedToUserid = mergedToUserid;
    }

    public int getValidUser() {
        return isValidUser;
    }

    public void setValidUser(int validUser) {
        isValidUser = validUser;
    }

    public Boolean getFrequentFlyerNumberVerified() {
        return isFrequentFlyerNumberVerified;
    }

    public void setFrequentFlyerNumberVerified(Boolean frequentFlyerNumberVerified) {
        isFrequentFlyerNumberVerified = frequentFlyerNumberVerified;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }

    public Boolean getCorpTravelMilitaryCard() {
        return isCorpTravelMilitaryCard;
    }

    public void setCorpTravelMilitaryCard(Boolean corpTravelMilitaryCard) {
        isCorpTravelMilitaryCard = corpTravelMilitaryCard;
    }

    public String getHkMacaoTaiwanResidencePermit() {
        return hkMacaoTaiwanResidencePermit;
    }

    public void setHkMacaoTaiwanResidencePermit(String hkMacaoTaiwanResidencePermit) {
        this.hkMacaoTaiwanResidencePermit = hkMacaoTaiwanResidencePermit;
    }

    public String getHkMacaoTaiwanIdCard() {
        return hkMacaoTaiwanIdCard;
    }

    public void setHkMacaoTaiwanIdCard(String hkMacaoTaiwanIdCard) {
        this.hkMacaoTaiwanIdCard = hkMacaoTaiwanIdCard;
    }

    public String getMainlandToTaiwanTravelPermit() {
        return mainlandToTaiwanTravelPermit;
    }

    public void setMainlandToTaiwanTravelPermit(String mainlandToTaiwanTravelPermit) {
        this.mainlandToTaiwanTravelPermit = mainlandToTaiwanTravelPermit;
    }

    public String getHkMacaoTravelPermit() {
        return hkMacaoTravelPermit;
    }

    public void setHkMacaoTravelPermit(String hkMacaoTravelPermit) {
        this.hkMacaoTravelPermit = hkMacaoTravelPermit;
    }

    @Override
    public String toString() {
        return "UserDimModel{" +
                "pkId='" + pkId + '\'' +
                ", crmCustomerId='" + crmCustomerId + '\'' +
                ", lyMemberId='" + lyMemberId + '\'' +
                ", lyVipId='" + lyVipId + '\'' +
                ", sysRegisterId='" + sysRegisterId + '\'' +
                ", ffpRegisterId='" + ffpRegisterId + '\'' +
                ", historicalFfpId='" + historicalFfpId + '\'' +
                ", alipayId='" + alipayId + '\'' +
                ", wechatId='" + wechatId + '\'' +
                ", douyinId='" + douyinId + '\'' +
                ", cnName='" + cnName + '\'' +
                ", enName='" + enName + '\'' +
                ", sex='" + sex + '\'' +
                ", userType='" + userType + '\'' +
                ", birthday='" + birthday + '\'' +
                ", province='" + province + '\'' +
                ", city='" + city + '\'' +
                ", nationality='" + nationality + '\'' +
                ", ethnicity='" + ethnicity + '\'' +
                ", employer='" + employer + '\'' +
                ", mobilePhone='" + mobilePhone + '\'' +
                ", mobileNumberReliability='" + mobileNumberReliability + '\'' +
                ", email='" + email + '\'' +
                ", idCard='" + idCard + '\'' +
                ", passport='" + passport + '\'' +
                ", seamanId='" + seamanId + '\'' +
                ", alienPermit='" + alienPermit + '\'' +
                ", diplomaticStaffCertificate='" + diplomaticStaffCertificate + '\'' +
                ", permanentResidentId='" + permanentResidentId + '\'' +
                ", civilianStaffId='" + civilianStaffId + '\'' +
                ", staffId='" + staffId + '\'' +
                ", officerIdCard='" + officerIdCard + '\'' +
                ", armedPoliceOfficer='" + armedPoliceOfficer + '\'' +
                ", armedPoliceSoldier='" + armedPoliceSoldier + '\'' +
                ", civilianOfficialId='" + civilianOfficialId + '\'' +
                ", conscriptSoldierId='" + conscriptSoldierId + '\'' +
                ", nonCommissionedOfficerId='" + nonCommissionedOfficerId + '\'' +
                ", hkMacaoResidentPermit='" + hkMacaoResidentPermit + '\'' +
                ", taiwanResidentTravelPermit='" + taiwanResidentTravelPermit + '\'' +
                ", blindPassenger=" + blindPassenger +
                ", deafPassenger=" + deafPassenger +
                ", isDirectUser=" + isDirectUser +
                ", isOfficialWebsiteNonRegistered=" + isOfficialWebsiteNonRegistered +
                ", isDouyinCardPurchaser=" + isDouyinCardPurchaser +
                ", directUserStatus='" + directUserStatus + '\'' +
                ", lastLoginTime='" + lastLoginTime + '\'' +
                ", directRegisterDate='" + directRegisterDate + '\'' +
                ", directLastloginDate='" + directLastloginDate + '\'' +
                ", directVerifiedFlag=" + directVerifiedFlag +
                ", directVerifyDate='" + directVerifyDate + '\'' +
                ", isBlacklistUser=" + isBlacklistUser +
                ", studentFlag=" + studentFlag +
                ", teacherFlag=" + teacherFlag +
                ", agentFlag=" + agentFlag +
                ", isKeyAccount=" + isKeyAccount +
                ", keyAccountNumber='" + keyAccountNumber + '\'' +
                ", isFrequentTraveler=" + isFrequentTraveler +
                ", frequentTravelerCardno='" + frequentTravelerCardno + '\'' +
                ", frequentTravelerLevel='" + frequentTravelerLevel + '\'' +
                ", yjCardNumber='" + yjCardNumber + '\'' +
                ", yjCardExpiredate='" + yjCardExpiredate + '\'' +
                ", devChannelOne='" + devChannelOne + '\'' +
                ", devChannelTwo='" + devChannelTwo + '\'' +
                ", devChannelThree='" + devChannelThree + '\'' +
                ", devChannelFour='" + devChannelFour + '\'' +
                ", ftRegisterTime='" + ftRegisterTime + '\'' +
                ", acceptSmsMarketing=" + acceptSmsMarketing +
                ", acceptEmailMarketing=" + acceptEmailMarketing +
                ", parentFtCardno='" + parentFtCardno + '\'' +
                ", isLyUser=" + isLyUser +
                ", lyRegisterTime='" + lyRegisterTime + '\'' +
                ", lyCardNumber='" + lyCardNumber + '\'' +
                ", lyUserLevel='" + lyUserLevel + '\'' +
                ", lyUserStatus='" + lyUserStatus + '\'' +
                ", lyRegisterStatus='" + lyRegisterStatus + '\'' +
                ", lyVerifyStatus='" + lyVerifyStatus + '\'' +
                ", lyLifetimePoints=" + lyLifetimePoints +
                ", lyAvailablePoints=" + lyAvailablePoints +
                ", isHighTraveler=" + isHighTraveler +
                ", highTravelerType=" + highTravelerType +
                ", highTravelerTier='" + highTravelerTier + '\'' +
                ", tierPrestigeExpiredate='" + tierPrestigeExpiredate + '\'' +
                ", tierHonorExpiredate='" + tierHonorExpiredate + '\'' +
                ", highTravelerDs='" + highTravelerDs + '\'' +
                ", isYjPersonnel=" + isYjPersonnel +
                ", vipFlag=" + vipFlag +
                ", cipFlag=" + cipFlag +
                ", vvipFlag=" + vvipFlag +
                ", foodPreference='" + foodPreference + '\'' +
                ", seatPreference='" + seatPreference + '\'' +
                ", tempFoodPreference='" + tempFoodPreference + '\'' +
                ", tempSeatPreference='" + tempSeatPreference + '\'' +
                ", beveragePreference='" + beveragePreference + '\'' +
                ", longTermSeatPreference='" + longTermSeatPreference + '\'' +
                ", firstClassLoungePreference='" + firstClassLoungePreference + '\'' +
                ", mergedToUserid='" + mergedToUserid + '\'' +
                ", isValidUser=" + isValidUser +
                ", isFrequentFlyerNumberVerified=" + isFrequentFlyerNumberVerified +
                ", createTime='" + createTime + '\'' +
                ", updateTime='" + updateTime + '\'' +
                ", hkMacaoTravelPermit='" + hkMacaoTravelPermit + '\'' +
                ", mainlandToTaiwanTravelPermit='" + mainlandToTaiwanTravelPermit + '\'' +
                ", hkMacaoTaiwanIdCard='" + hkMacaoTaiwanIdCard + '\'' +
                ", hkMacaoTaiwanResidencePermit='" + hkMacaoTaiwanResidencePermit + '\'' +
                ", isCorpTravelMilitaryCard=" + isCorpTravelMilitaryCard +
                '}';
    }
}
