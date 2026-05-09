package com.travelsky.trp.usercenter.data.analysis.model.dim;

import com.fasterxml.jackson.annotation.JsonProperty;


/**
 * 用户维表数据来源表 DateTime -->  String
 */
public class UserDimSummaryModel {
    @JsonProperty("PK_ID")
    private String pkId;
    @JsonProperty("CRM_CUSTOMER_ID_SOURCE")
    private String crmCustomerIdSource;

    @JsonProperty("CRM_CUSTOMER_ID_UPDATETIME")
    private String crmCustomerIdUpdatetime;

    @JsonProperty("LY_VIP_ID_SOURCE")
    private String lyVipIdSource;
    @JsonProperty("LY_VIP_ID_UPDATETIME")
    private String lyVipIdUpdatetime;

    @JsonProperty("LY_MEMBER_ID_SOURCE")
    private String lyMemberIdSource;

    @JsonProperty("LY_MEMBER_ID_UPDATETIME")
    private String lyMemberIdUpdatetime;

    @JsonProperty("SYS_REGISTER_ID_SOURCE")
    private String sysRegisterIdSource;
    @JsonProperty("SYS_REGISTER_ID_UPDATETIME")
    private String sysRegisterIdUpdatetime;
    @JsonProperty("FFP_REGISTER_ID_SOURCE")
    private String ffpRegisterIdSource;
    @JsonProperty("FFP_REGISTER_ID_UPDATETIME")
    private String ffpRegisterIdUpdatetime;
    @JsonProperty("HISTORICAL_FFP_ID_SOURCE")
    private String historicalFfpIdSource;
    @JsonProperty("HISTORICAL_FFP_ID_UPDATETIME")
    private String historicalFfpIdUpdatetime;
    @JsonProperty("ALIPAY_ID_SOURCE")
    private String alipayIdSource;
    @JsonProperty("ALIPAY_ID_UPDATETIME")
    private String alipayIdUpdatetime;
    @JsonProperty("WECHAT_ID_SOURCE")
    private String wechatIdSource;
    @JsonProperty("WECHAT_ID_UPDATETIME")
    private String wechatIdUpdatetime;
    @JsonProperty("DOUYIN_ID_SOURCE")
    private String douyinIdSource;
    @JsonProperty("DOUYIN_ID_UPDATETIME")
    private String douyinIdUpdatetime;
    @JsonProperty("CN_NAME_SOURCE")
    private String cnNameSource;
    @JsonProperty("CN_NAME_UPDATETIME")
    private String cnNameUpdatetime;
    @JsonProperty("EN_NAME_SOURCE")
    private String enNameSource;
    @JsonProperty("EN_NAME_UPDATETIME")
    private String enNameUpdatetime;
    @JsonProperty("SEX_SOURCE")
    private String sexSource;
    @JsonProperty("SEX_UPDATETIME")
    private String sexUpdatetime;
    @JsonProperty("USER_TYPE_SOURCE")
    private String userTypeSource;
    @JsonProperty("USER_TYPE_UPDATETIME")
    private String userTypeUpdatetime;
    @JsonProperty("BIRTHDAY_SOURCE")
    private String birthdaySource;
    @JsonProperty("BIRTHDAY_UPDATETIME")
    private String birthdayUpdatetime;
    @JsonProperty("PROVINCE_SOURCE")
    private String provinceSource;
    @JsonProperty("PROVINCE_UPDATETIME")
    private String provinceUpdatetime;
    @JsonProperty("CITY_SOURCE")
    private String citySource;
    @JsonProperty("CITY_UPDATETIME")
    private String cityUpdatetime;
    @JsonProperty("NATIONALITY_SOURCE")
    private String nationalitySource;
    @JsonProperty("NATIONALITY_UPDATETIME")
    private String nationalityUpdatetime;
    @JsonProperty("ETHNICITY_SOURCE")
    private String ethnicitySource;
    @JsonProperty("ETHNICITY_UPDATETIME")
    private String ethnicityUpdatetime;
    @JsonProperty("EMPLOYER_SOURCE")
    private String employerSource;
    @JsonProperty("EMPLOYER_UPDATETIME")
    private String employerUpdatetime;
    @JsonProperty("MOBILE_PHONE_SOURCE")
    private String mobilePhoneSource;
    @JsonProperty("MOBILE_PHONE_UPDATETIME")
    private String mobilePhoneUpdatetime;
    @JsonProperty("MOBILE_NUMBER_RELIABILITY_SOURCE")
    private String mobileNumberReliabilitySource;
    @JsonProperty("MOBILE_NUMBER_RELIABILITY_UPDATETIME")
    private String mobileNumberReliabilityUpdatetime;
    @JsonProperty("EMAIL_SOURCE")
    private String emailSource;
    @JsonProperty("EMAIL_UPDATETIME")
    private String emailUpdatetime;
    @JsonProperty("ID_CARD_SOURCE")
    private String idCardSource;
    @JsonProperty("ID_CARD_UPDATETIME")
    private String idCardUpdatetime;
    @JsonProperty("PASSPORT_SOURCE")
    private String passportSource;
    @JsonProperty("PASSPORT_UPDATETIME")
    private String passportUpdatetime;
    @JsonProperty("SEAMAN_ID_SOURCE")
    private String seamanIdSource;
    @JsonProperty("SEAMAN_ID_UPDATETIME")
    private String seamanIdUpdatetime;
    @JsonProperty("ALIEN_PERMIT_SOURCE")
    private String alienPermitSource;
    @JsonProperty("ALIEN_PERMIT_UPDATETIME")
    private String alienPermitUpdatetime;
    @JsonProperty("DIPLOMATIC_STAFF_CERTIFICATE_SOURCE")
    private String diplomaticStaffCertificateSource;
    @JsonProperty("DIPLOMATIC_STAFF_CERTIFICATE_UPDATETIME")
    private String diplomaticStaffCertificateUpdatetime;
    @JsonProperty("PERMANENT_RESIDENT_ID_SOURCE")
    private String permanentResidentIdSource;
    @JsonProperty("PERMANENT_RESIDENT_ID_UPDATETIME")
    private String permanentResidentIdUpdatetime;
    @JsonProperty("CIVILIAN_STAFF_ID_SOURCE")
    private String civilianStaffIdSource;
    @JsonProperty("CIVILIAN_STAFF_ID_UPDATETIME")
    private String civilianStaffIdUpdatetime;
    @JsonProperty("STAFF_ID_SOURCE")
    private String staffIdSource;
    @JsonProperty("STAFF_ID_UPDATETIME")
    private String staffIdUpdatetime;
    @JsonProperty("OFFICER_ID_CARD_SOURCE")
    private String officerIdCardSource;
    @JsonProperty("OFFICER_ID_CARD_UPDATETIME")
    private String officerIdCardUpdatetime;
    @JsonProperty("ARMED_POLICE_OFFICER_SOURCE")
    private String armedPoliceOfficerSource;
    @JsonProperty("ARMED_POLICE_OFFICER_UPDATETIME")
    private String armedPoliceOfficerUpdatetime;
    @JsonProperty("ARMED_POLICE_SOLDIER_SOURCE")
    private String armedPoliceSoldierSource;
    @JsonProperty("ARMED_POLICE_SOLDIER_UPDATETIME")
    private String armedPoliceSoldierUpdatetime;
    @JsonProperty("CIVILIAN_OFFICIAL_ID_SOURCE")
    private String civilianOfficialIdSource;
    @JsonProperty("CIVILIAN_OFFICIAL_ID_UPDATETIME")
    private String civilianOfficialIdUpdatetime;
    @JsonProperty("CONSCRIPT_SOLDIER_ID_SOURCE")
    private String conscriptSoldierIdSource;
    @JsonProperty("CONSCRIPT_SOLDIER_ID_UPDATETIME")
    private String conscriptSoldierIdUpdatetime;
    @JsonProperty("NON_COMMISSIONED_OFFICER_ID_SOURCE")
    private String nonCommissionedOfficerIdSource;
    @JsonProperty("NON_COMMISSIONED_OFFICER_ID_UPDATETIME")
    private String nonCommissionedOfficerIdUpdatetime;
    @JsonProperty("HK_MACAO_RESIDENT_PERMIT_SOURCE")
    private String hkMacaoResidentPermitSource;
    @JsonProperty("HK_MACAO_RESIDENT_PERMIT_UPDATETIME")
    private String hkMacaoResidentPermitUpdatetime;
    @JsonProperty("TAIWAN_RESIDENT_TRAVEL_PERMIT_SOURCE")
    private String taiwanResidentTravelPermitSource;
    @JsonProperty("TAIWAN_RESIDENT_TRAVEL_PERMIT_UPDATETIME")
    private String taiwanResidentTravelPermitUpdatetime;
    @JsonProperty("BLIND_PASSENGER_SOURCE")
    private String blindPassengerSource;
    @JsonProperty("BLIND_PASSENGER_UPDATETIME")
    private String blindPassengerUpdatetime;
    @JsonProperty("DEAF_PASSENGER_SOURCE")
    private String deafPassengerSource;
    @JsonProperty("DEAF_PASSENGER_UPDATETIME")
    private String deafPassengerUpdatetime;
    @JsonProperty("IS_DIRECT_USER_SOURCE")
    private String isDirectUserSource;
    @JsonProperty("IS_DIRECT_USER_UPDATETIME")
    private String isDirectUserUpdatetime;
    @JsonProperty("IS_OFFICIAL_WEBSITE_NON_REGISTERED_SOURCE")
    private String isOfficialWebsiteNonRegisteredSource;
    @JsonProperty("IS_OFFICIAL_WEBSITE_NON_REGISTERED_UPDATETIME")
    private String isOfficialWebsiteNonRegisteredUpdatetime;
    @JsonProperty("IS_DOUYIN_CARD_PURCHASER_SOURCE")
    private String isDouyinCardPurchaserSource;
    @JsonProperty("IS_DOUYIN_CARD_PURCHASER_UPDATETIME")
    private String isDouyinCardPurchaserUpdatetime;
    @JsonProperty("DIRECT_USER_STATUS_SOURCE")
    private String directUserStatusSource;
    @JsonProperty("DIRECT_USER_STATUS_UPDATETIME")
    private String directUserStatusUpdatetime;
    @JsonProperty("DIRECT_REGISTER_DATE_SOURCE")
    private String directRegisterDateSource;
    @JsonProperty("DIRECT_REGISTER_DATE_UPDATETIME")
    private String directRegisterDateUpdatetime;
    @JsonProperty("DIRECT_LASTLOGIN_DATE_SOURCE")
    private String directLastLoginDateSource;
    @JsonProperty("DIRECT_LASTLOGIN_DATE_UPDATETIME")
    private String directLastLoginDateUpdatetime;
    @JsonProperty("DIRECT_VERIFIED_FLAG_SOURCE")
    private String directVerifiedFlagSource;
    @JsonProperty("DIRECT_VERIFIED_FLAG_UPDATETIME")
    private String directVerifiedFlagUpdatetime;
    @JsonProperty("DIRECT_VERIFY_DATE_SOURCE")
    private String directVerifyDateSource;
    @JsonProperty("DIRECT_VERIFY_DATE_UPDATETIME")
    private String directVerifyDateUpdatetime;
    @JsonProperty("IS_BLACKLIST_USER_SOURCE")
    private String isBlacklistUserSource;
    @JsonProperty("IS_BLACKLIST_USER_UPDATETIME")
    private String isBlacklistUserUpdatetime;
    @JsonProperty("STUDENT_FLAG_SOURCE")
    private String studentFlagSource;
    @JsonProperty("STUDENT_FLAG_UPDATETIME")
    private String studentFlagUpdatetime;
    @JsonProperty("TEACHER_FLAG_SOURCE")
    private String teacherFlagSource;
    @JsonProperty("TEACHER_FLAG_UPDATETIME")
    private String teacherFlagUpdatetime;
    @JsonProperty("AGENT_FLAG_SOURCE")
    private String agentFlagSource;
    @JsonProperty("AGENT_FLAG_UPDATETIME")
    private String agentFlagUpdatetime;
    @JsonProperty("IS_KEY_ACCOUNT_SOURCE")
    private String isKeyAccountSource;
    @JsonProperty("IS_KEY_ACCOUNT_UPDATETIME")
    private String isKeyAccountUpdatetime;
    @JsonProperty("KEY_ACCOUNT_NUMBER_SOURCE")
    private String keyAccountNumberSource;
    @JsonProperty("KEY_ACCOUNT_NUMBER_UPDATETIME")
    private String keyAccountNumberUpdatetime;
    @JsonProperty("IS_FREQUENT_TRAVELER_SOURCE")
    private String isFrequentTravelerSource;
    @JsonProperty("IS_FREQUENT_TRAVELER_UPDATETIME")
    private String isFrequentTravelerUpdatetime;
    @JsonProperty("FREQUENT_TRAVELER_CARDNO_SOURCE")
    private String frequentTravelerCardnoSource;
    @JsonProperty("FREQUENT_TRAVELER_CARDNO_UPDATETIME")
    private String frequentTravelerCardnoUpdatetime;
    @JsonProperty("FREQUENT_TRAVELER_LEVEL_SOURCE")
    private String frequentTravelerLevelSource;
    @JsonProperty("FREQUENT_TRAVELER_LEVEL_UPDATETIME")
    private String frequentTravelerLevelUpdatetime;
    @JsonProperty("YJ_CARD_NUMBER_SOURCE")
    private String yjCardNumberSource;
    @JsonProperty("YJ_CARD_NUMBER_UPDATETIME")
    private String yjCardNumberUpdatetime;
    @JsonProperty("YJ_CARD_EXPIREDATE_SOURCE")
    private String yjCardExpiredateSource;
    @JsonProperty("YJ_CARD_EXPIREDATE_UPDATETIME")
    private String yjCardExpiredateUpdatetime;
    @JsonProperty("DEV_CHANNEL_ONE_SOURCE")
    private String devChannelOneSource;
    @JsonProperty("DEV_CHANNEL_ONE_UPDATETIME")
    private String devChannelOneUpdatetime;
    @JsonProperty("DEV_CHANNEL_TWO_SOURCE")
    private String devChannelTwoSource;
    @JsonProperty("DEV_CHANNEL_TWO_UPDATETIME")
    private String devChannelTwoUpdatetime;
    @JsonProperty("DEV_CHANNEL_THREE_SOURCE")
    private String devChannelThreeSource;
    @JsonProperty("DEV_CHANNEL_THREE_UPDATETIME")
    private String devChannelThreeUpdatetime;
    @JsonProperty("DEV_CHANNEL_FOUR_SOURCE")
    private String devChannelFourSource;
    @JsonProperty("DEV_CHANNEL_FOUR_UPDATETIME")
    private String devChannelFourUpdatetime;
    @JsonProperty("FT_REGISTER_TIME_SOURCE")
    private String ftRegisterTimeSource;
    @JsonProperty("FT_REGISTER_TIME_UPDATETIME")
    private String ftRegisterTimeUpdatetime;
    @JsonProperty("ACCEPT_SMS_MARKETING_SOURCE")
    private String acceptSmsMarketingSource;
    @JsonProperty("ACCEPT_SMS_MARKETING_UPDATETIME")
    private String acceptSmsMarketingUpdatetime;
    @JsonProperty("ACCEPT_EMAIL_MARKETING_SOURCE")
    private String acceptEmailMarketingSource;
    @JsonProperty("ACCEPT_EMAIL_MARKETING_UPDATETIME")
    private String acceptEmailMarketingUpdatetime;
    @JsonProperty("PARENT_FT_CARDNO_SOURCE")
    private String parentFtCardnoSource;
    @JsonProperty("PARENT_FT_CARDNO_UPDATETIME")
    private String parentFtCardnoUpdatetime;
    @JsonProperty("IS_LY_USER_SOURCE")
    private String isLyUserSource;
    @JsonProperty("IS_LY_USER_UPDATETIME")
    private String isLyUserUpdatetime;
    @JsonProperty("LY_REGISTER_TIME_SOURCE")
    private String lyRegisterTimeSource;
    @JsonProperty("LY_REGISTER_TIME_UPDATETIME")
    private String lyRegisterTimeUpdatetime;
    @JsonProperty("LY_CARD_NUMBER_SOURCE")
    private String lyCardNumberSource;
    @JsonProperty("LY_CARD_NUMBER_UPDATETIME")
    private String lyCardNumberUpdatetime;
    @JsonProperty("LY_USER_LEVEL_SOURCE")
    private String lyUserLevelSource;
    @JsonProperty("LY_USER_LEVEL_UPDATETIME")
    private String lyUserLevelUpdatetime;
    @JsonProperty("LY_USER_STATUS_SOURCE")
    private String lyUserStatusSource;
    @JsonProperty("LY_USER_STATUS_UPDATETIME")
    private String lyUserStatusUpdatetime;
    @JsonProperty("LY_REGISTER_STATUS_SOURCE")
    private String lyRegisterStatusSource;
    @JsonProperty("LY_REGISTER_STATUS_UPDATETIME")
    private String lyRegisterStatusUpdatetime;
    @JsonProperty("LY_VERIFY_STATUS_SOURCE")
    private String lyVerifyStatusSource;
    @JsonProperty("LY_VERIFY_STATUS_UPDATETIME")
    private String lyVerifyStatusUpdatetime;
    @JsonProperty("LY_LIFETIME_POINTS_SOURCE")
    private String lyLifetimePointsSource;
    @JsonProperty("LY_LIFETIME_POINTS_UPDATETIME")
    private String lyLifetimePointsUpdatetime;
    @JsonProperty("LY_AVAILABLE_POINTS_SOURCE")
    private String lyAvailablePointsSource;
    @JsonProperty("LY_AVAILABLE_POINTS_UPDATETIME")
    private String lyAvailablePointsUpdatetime;
    @JsonProperty("IS_HIGH_TRAVELER_SOURCE")
    private String isHighTravelerSource;
    @JsonProperty("IS_HIGH_TRAVELER_UPDATETIME")
    private String isHighTravelerUpdatetime;
    @JsonProperty("HIGH_TRAVELER_TYPE_SOURCE")
    private String highTravelerTypeSource;
    @JsonProperty("HIGH_TRAVELER_TYPE_UPDATETIME")
    private String highTravelerTypeUpdatetime;
    @JsonProperty("HIGH_TRAVELER_TIER_SOURCE")
    private String highTravelerTierSource;
    @JsonProperty("HIGH_TRAVELER_TIER_UPDATETIME")
    private String highTravelerTierUpdatetime;
    @JsonProperty("TIER_PRESTIGE_EXPIREDATE_SOURCE")
    private String tierPrestigeExpiredateSource;
    @JsonProperty("TIER_PRESTIGE_EXPIREDATE_UPDATETIME")
    private String tierPrestigeExpiredateUpdatetime;
    @JsonProperty("TIER_HONOR_EXPIREDATE_SOURCE")
    private String tierHonorExpiredateSource;
    @JsonProperty("TIER_HONOR_EXPIREDATE_UPDATETIME")
    private String tierHonorExpiredateUpdatetime;
    @JsonProperty("HIGH_TRAVELER_DS_SOURCE")
    private String highTravelerDsSource;
    @JsonProperty("HIGH_TRAVELER_DS_UPDATETIME")
    private String highTravelerDsUpdatetime;
    @JsonProperty("IS_YJ_PERSONNEL_SOURCE")
    private String isYjPersonnelSource;
    @JsonProperty("IS_YJ_PERSONNEL_UPDATETIME")
    private String isYjPersonnelUpdatetime;
    @JsonProperty("VIP_FLAG_SOURCE")
    private String vipFlagSource;
    @JsonProperty("VIP_FLAG_UPDATETIME")
    private String vipFlagUpdatetime;
    @JsonProperty("CIP_FLAG_SOURCE")
    private String cipFlagSource;
    @JsonProperty("CIP_FLAG_UPDATETIME")
    private String cipFlagUpdatetime;
    @JsonProperty("VVIP_FLAG_SOURCE")
    private String vvipFlagSource;
    @JsonProperty("VVIP_FLAG_UPDATETIME")
    private String vvipFlagUpdatetime;
    @JsonProperty("FOOD_PREFERENCE_SOURCE")
    private String foodPreferenceSource;
    @JsonProperty("FOOD_PREFERENCE_UPDATETIME")
    private String foodPreferenceUpdatetime;
    @JsonProperty("SEAT_PREFERENCE_SOURCE")
    private String seatPreferenceSource;
    @JsonProperty("SEAT_PREFERENCE_UPDATETIME")
    private String seatPreferenceUpdatetime;
    @JsonProperty("TEMP_FOOD_PREFERENCE_SOURCE")
    private String tempFoodPreferenceSource;
    @JsonProperty("TEMP_FOOD_PREFERENCE_UPDATETIME")
    private String tempFoodPreferenceUpdatetime;
    @JsonProperty("TEMP_SEAT_PREFERENCE_SOURCE")
    private String tempSeatPreferenceSource;
    @JsonProperty("TEMP_SEAT_PREFERENCE_UPDATETIME")
    private String tempSeatPreferenceUpdatetime;
    @JsonProperty("BEVERAGE_PREFERENCE_SOURCE")
    private String beveragePreferenceSource;
    @JsonProperty("BEVERAGE_PREFERENCE_UPDATETIME")
    private String beveragePreferenceUpdatetime;
    @JsonProperty("LONG_TERM_SEAT_PREFERENCE_SOURCE")
    private String longTermSeatPreferenceSource;
    @JsonProperty("LONG_TERM_SEAT_PREFERENCE_UPDATETIME")
    private String longTermSeatPreferenceUpdatetime;
    @JsonProperty("FIRST_CLASS_LOUNGE_PREFERENCE_SOURCE")
    private String firstClassLoungePreferenceSource;
    @JsonProperty("FIRST_CLASS_LOUNGE_PREFERENCE_UPDATETIME")
    private String firstClassLoungePreferenceUpdatetime;
    @JsonProperty("MERGED_TO_USERID_SOURCE")
    private String mergedToUseridSource;
    @JsonProperty("MERGED_TO_USERID_UPDATETIME")
    private String mergedToUseridUpdatetime;
    @JsonProperty("IS_VALID_USER_SOURCE")
    private String isValidUserSource;
    @JsonProperty("IS_VALID_USER_UPDATETIME")
    private String isValidUserUpdatetime;
    @JsonProperty("IS_FREQUENT_FLYER_NUMBER_VERIFIED_SOURCE")
    private String isFrequentFlyerNumberVerifiedSource;
    @JsonProperty("IS_FREQUENT_FLYER_NUMBER_VERIFIED_UPDATETIME")
    private String isFrequentFlyerNumberVerifiedUpdatetime;

    @JsonProperty("HK_MACAO_TRAVEL_PERMIT_SOURCE")
    private String hkMacaoTravelPermitSource;
    @JsonProperty("HK_MACAO_TRAVEL_PERMIT_UPDATETIME")
    private String hkMacaoTravelPermitUpdatetime;
    @JsonProperty("MAINLAND_TO_TAIWAN_TRAVEL_PERMIT_SOURCE")
    private String mainlandToTaiwanTravelPermitSource;
    @JsonProperty("MAINLAND_TO_TAIWAN_TRAVEL_PERMIT_UPDATETIME")
    private String mainlandToTaiwanTravelPermitUpdatetime;
    @JsonProperty("HK_MACAO_TAIWAN_ID_CARD_SOURCE")
    private String hkMacaoTaiwanIdCardSource;
    @JsonProperty("HK_MACAO_TAIWAN_ID_CARD_UPDATETIME")
    private String hkMacaoTaiwanIdCardUpdatetime;
    @JsonProperty("HK_MACAO_TAIWAN_RESIDENCE_PERMIT_SOURCE")
    private String hkMacaoTaiwanResidencePermitSource;
    @JsonProperty("HK_MACAO_TAIWAN_RESIDENCE_PERMIT_UPDATETIME")
    private String hkMacaoTaiwanResidencePermitUpdatetime;

    public UserDimSummaryModel() {
    }

    public String getIsValidUserSource() {
        return isValidUserSource;
    }

    public void setIsValidUserSource(String isValidUserSource) {
        this.isValidUserSource = isValidUserSource;
    }

    public String getIsValidUserUpdatetime() {
        return isValidUserUpdatetime;
    }

    public void setIsValidUserUpdatetime(String isValidUserUpdatetime) {
        this.isValidUserUpdatetime = isValidUserUpdatetime;
    }

    public String getDirectLastLoginDateSource() {
        return directLastLoginDateSource;
    }

    public String getHighTravelerTypeSource() {
        return highTravelerTypeSource;
    }

    public void setHighTravelerTypeSource(String highTravelerTypeSource) {
        this.highTravelerTypeSource = highTravelerTypeSource;
    }

    public String getHighTravelerTypeUpdatetime() {
        return highTravelerTypeUpdatetime;
    }

    public void setHighTravelerTypeUpdatetime(String highTravelerTypeUpdatetime) {
        this.highTravelerTypeUpdatetime = highTravelerTypeUpdatetime;
    }

    public void setDirectLastLoginDateSource(String directLastLoginDateSource) {
        this.directLastLoginDateSource = directLastLoginDateSource;
    }

    public String getDirectLastLoginDateUpdatetime() {
        return directLastLoginDateUpdatetime;
    }

    public void setDirectLastLoginDateUpdatetime(String directLastLoginDateUpdatetime) {
        this.directLastLoginDateUpdatetime = directLastLoginDateUpdatetime;
    }

    public String getCrmCustomerIdSource() {
        return crmCustomerIdSource;
    }

    public void setCrmCustomerIdSource(String crmCustomerIdSource) {
        this.crmCustomerIdSource = crmCustomerIdSource;
    }

    public String getCrmCustomerIdUpdatetime() {
        return crmCustomerIdUpdatetime;
    }

    public void setCrmCustomerIdUpdatetime(String crmCustomerIdUpdatetime) {
        this.crmCustomerIdUpdatetime = crmCustomerIdUpdatetime;
    }

    public String getLyMemberIdSource() {
        return lyMemberIdSource;
    }

    public void setLyMemberIdSource(String lyMemberIdSource) {
        this.lyMemberIdSource = lyMemberIdSource;
    }

    public String getLyMemberIdUpdatetime() {
        return lyMemberIdUpdatetime;
    }

    public void setLyMemberIdUpdatetime(String lyMemberIdUpdatetime) {
        this.lyMemberIdUpdatetime = lyMemberIdUpdatetime;
    }

    public String getPkId() {
        return pkId;
    }

    public void setPkId(String pkId) {
        this.pkId = pkId;
    }

    public String getLyVipIdSource() {
        return lyVipIdSource;
    }

    public void setLyVipIdSource(String lyVipIdSource) {
        this.lyVipIdSource = lyVipIdSource;
    }

    public String getLyVipIdUpdatetime() {
        return lyVipIdUpdatetime;
    }

    public void setLyVipIdUpdatetime(String lyVipIdUpdatetime) {
        this.lyVipIdUpdatetime = lyVipIdUpdatetime;
    }

    public String getSysRegisterIdSource() {
        return sysRegisterIdSource;
    }

    public void setSysRegisterIdSource(String sysRegisterIdSource) {
        this.sysRegisterIdSource = sysRegisterIdSource;
    }

    public String getSysRegisterIdUpdatetime() {
        return sysRegisterIdUpdatetime;
    }

    public void setSysRegisterIdUpdatetime(String sysRegisterIdUpdatetime) {
        this.sysRegisterIdUpdatetime = sysRegisterIdUpdatetime;
    }

    public String getFfpRegisterIdSource() {
        return ffpRegisterIdSource;
    }

    public void setFfpRegisterIdSource(String ffpRegisterIdSource) {
        this.ffpRegisterIdSource = ffpRegisterIdSource;
    }

    public String getFfpRegisterIdUpdatetime() {
        return ffpRegisterIdUpdatetime;
    }

    public void setFfpRegisterIdUpdatetime(String ffpRegisterIdUpdatetime) {
        this.ffpRegisterIdUpdatetime = ffpRegisterIdUpdatetime;
    }

    public String getHistoricalFfpIdSource() {
        return historicalFfpIdSource;
    }

    public void setHistoricalFfpIdSource(String historicalFfpIdSource) {
        this.historicalFfpIdSource = historicalFfpIdSource;
    }

    public String getHistoricalFfpIdUpdatetime() {
        return historicalFfpIdUpdatetime;
    }

    public void setHistoricalFfpIdUpdatetime(String historicalFfpIdUpdatetime) {
        this.historicalFfpIdUpdatetime = historicalFfpIdUpdatetime;
    }

    public String getAlipayIdSource() {
        return alipayIdSource;
    }

    public void setAlipayIdSource(String alipayIdSource) {
        this.alipayIdSource = alipayIdSource;
    }

    public String getAlipayIdUpdatetime() {
        return alipayIdUpdatetime;
    }

    public void setAlipayIdUpdatetime(String alipayIdUpdatetime) {
        this.alipayIdUpdatetime = alipayIdUpdatetime;
    }

    public String getWechatIdSource() {
        return wechatIdSource;
    }

    public void setWechatIdSource(String wechatIdSource) {
        this.wechatIdSource = wechatIdSource;
    }

    public String getWechatIdUpdatetime() {
        return wechatIdUpdatetime;
    }

    public void setWechatIdUpdatetime(String wechatIdUpdatetime) {
        this.wechatIdUpdatetime = wechatIdUpdatetime;
    }

    public String getDouyinIdSource() {
        return douyinIdSource;
    }

    public void setDouyinIdSource(String douyinIdSource) {
        this.douyinIdSource = douyinIdSource;
    }

    public String getDouyinIdUpdatetime() {
        return douyinIdUpdatetime;
    }

    public void setDouyinIdUpdatetime(String douyinIdUpdatetime) {
        this.douyinIdUpdatetime = douyinIdUpdatetime;
    }

    public String getCnNameSource() {
        return cnNameSource;
    }

    public void setCnNameSource(String cnNameSource) {
        this.cnNameSource = cnNameSource;
    }

    public String getCnNameUpdatetime() {
        return cnNameUpdatetime;
    }

    public void setCnNameUpdatetime(String cnNameUpdatetime) {
        this.cnNameUpdatetime = cnNameUpdatetime;
    }

    public String getEnNameSource() {
        return enNameSource;
    }

    public void setEnNameSource(String enNameSource) {
        this.enNameSource = enNameSource;
    }

    public String getEnNameUpdatetime() {
        return enNameUpdatetime;
    }

    public void setEnNameUpdatetime(String enNameUpdatetime) {
        this.enNameUpdatetime = enNameUpdatetime;
    }

    public String getSexSource() {
        return sexSource;
    }

    public void setSexSource(String sexSource) {
        this.sexSource = sexSource;
    }

    public String getSexUpdatetime() {
        return sexUpdatetime;
    }

    public void setSexUpdatetime(String sexUpdatetime) {
        this.sexUpdatetime = sexUpdatetime;
    }

    public String getUserTypeSource() {
        return userTypeSource;
    }

    public void setUserTypeSource(String userTypeSource) {
        this.userTypeSource = userTypeSource;
    }

    public String getUserTypeUpdatetime() {
        return userTypeUpdatetime;
    }

    public void setUserTypeUpdatetime(String userTypeUpdatetime) {
        this.userTypeUpdatetime = userTypeUpdatetime;
    }

    public String getBirthdaySource() {
        return birthdaySource;
    }

    public void setBirthdaySource(String birthdaySource) {
        this.birthdaySource = birthdaySource;
    }

    public String getBirthdayUpdatetime() {
        return birthdayUpdatetime;
    }

    public void setBirthdayUpdatetime(String birthdayUpdatetime) {
        this.birthdayUpdatetime = birthdayUpdatetime;
    }

    public String getProvinceSource() {
        return provinceSource;
    }

    public void setProvinceSource(String provinceSource) {
        this.provinceSource = provinceSource;
    }

    public String getProvinceUpdatetime() {
        return provinceUpdatetime;
    }

    public void setProvinceUpdatetime(String provinceUpdatetime) {
        this.provinceUpdatetime = provinceUpdatetime;
    }

    public String getCitySource() {
        return citySource;
    }

    public void setCitySource(String citySource) {
        this.citySource = citySource;
    }

    public String getCityUpdatetime() {
        return cityUpdatetime;
    }

    public void setCityUpdatetime(String cityUpdatetime) {
        this.cityUpdatetime = cityUpdatetime;
    }

    public String getNationalitySource() {
        return nationalitySource;
    }

    public void setNationalitySource(String nationalitySource) {
        this.nationalitySource = nationalitySource;
    }

    public String getNationalityUpdatetime() {
        return nationalityUpdatetime;
    }

    public void setNationalityUpdatetime(String nationalityUpdatetime) {
        this.nationalityUpdatetime = nationalityUpdatetime;
    }

    public String getEthnicitySource() {
        return ethnicitySource;
    }

    public void setEthnicitySource(String ethnicitySource) {
        this.ethnicitySource = ethnicitySource;
    }

    public String getEthnicityUpdatetime() {
        return ethnicityUpdatetime;
    }

    public void setEthnicityUpdatetime(String ethnicityUpdatetime) {
        this.ethnicityUpdatetime = ethnicityUpdatetime;
    }

    public String getEmployerSource() {
        return employerSource;
    }

    public void setEmployerSource(String employerSource) {
        this.employerSource = employerSource;
    }

    public String getEmployerUpdatetime() {
        return employerUpdatetime;
    }

    public void setEmployerUpdatetime(String employerUpdatetime) {
        this.employerUpdatetime = employerUpdatetime;
    }

    public String getMobilePhoneSource() {
        return mobilePhoneSource;
    }

    public void setMobilePhoneSource(String mobilePhoneSource) {
        this.mobilePhoneSource = mobilePhoneSource;
    }

    public String getMobilePhoneUpdatetime() {
        return mobilePhoneUpdatetime;
    }

    public void setMobilePhoneUpdatetime(String mobilePhoneUpdatetime) {
        this.mobilePhoneUpdatetime = mobilePhoneUpdatetime;
    }

    public String getMobileNumberReliabilitySource() {
        return mobileNumberReliabilitySource;
    }

    public void setMobileNumberReliabilitySource(String mobileNumberReliabilitySource) {
        this.mobileNumberReliabilitySource = mobileNumberReliabilitySource;
    }

    public String getMobileNumberReliabilityUpdatetime() {
        return mobileNumberReliabilityUpdatetime;
    }

    public void setMobileNumberReliabilityUpdatetime(String mobileNumberReliabilityUpdatetime) {
        this.mobileNumberReliabilityUpdatetime = mobileNumberReliabilityUpdatetime;
    }

    public String getEmailSource() {
        return emailSource;
    }

    public void setEmailSource(String emailSource) {
        this.emailSource = emailSource;
    }

    public String getEmailUpdatetime() {
        return emailUpdatetime;
    }

    public void setEmailUpdatetime(String emailUpdatetime) {
        this.emailUpdatetime = emailUpdatetime;
    }

    public String getIdCardSource() {
        return idCardSource;
    }

    public void setIdCardSource(String idCardSource) {
        this.idCardSource = idCardSource;
    }

    public String getIdCardUpdatetime() {
        return idCardUpdatetime;
    }

    public void setIdCardUpdatetime(String idCardUpdatetime) {
        this.idCardUpdatetime = idCardUpdatetime;
    }

    public String getPassportSource() {
        return passportSource;
    }

    public void setPassportSource(String passportSource) {
        this.passportSource = passportSource;
    }

    public String getPassportUpdatetime() {
        return passportUpdatetime;
    }

    public void setPassportUpdatetime(String passportUpdatetime) {
        this.passportUpdatetime = passportUpdatetime;
    }

    public String getSeamanIdSource() {
        return seamanIdSource;
    }

    public void setSeamanIdSource(String seamanIdSource) {
        this.seamanIdSource = seamanIdSource;
    }

    public String getSeamanIdUpdatetime() {
        return seamanIdUpdatetime;
    }

    public void setSeamanIdUpdatetime(String seamanIdUpdatetime) {
        this.seamanIdUpdatetime = seamanIdUpdatetime;
    }

    public String getAlienPermitSource() {
        return alienPermitSource;
    }

    public void setAlienPermitSource(String alienPermitSource) {
        this.alienPermitSource = alienPermitSource;
    }

    public String getAlienPermitUpdatetime() {
        return alienPermitUpdatetime;
    }

    public void setAlienPermitUpdatetime(String alienPermitUpdatetime) {
        this.alienPermitUpdatetime = alienPermitUpdatetime;
    }

    public String getDiplomaticStaffCertificateSource() {
        return diplomaticStaffCertificateSource;
    }

    public void setDiplomaticStaffCertificateSource(String diplomaticStaffCertificateSource) {
        this.diplomaticStaffCertificateSource = diplomaticStaffCertificateSource;
    }

    public String getDiplomaticStaffCertificateUpdatetime() {
        return diplomaticStaffCertificateUpdatetime;
    }

    public void setDiplomaticStaffCertificateUpdatetime(String diplomaticStaffCertificateUpdatetime) {
        this.diplomaticStaffCertificateUpdatetime = diplomaticStaffCertificateUpdatetime;
    }

    public String getPermanentResidentIdSource() {
        return permanentResidentIdSource;
    }

    public void setPermanentResidentIdSource(String permanentResidentIdSource) {
        this.permanentResidentIdSource = permanentResidentIdSource;
    }

    public String getPermanentResidentIdUpdatetime() {
        return permanentResidentIdUpdatetime;
    }

    public void setPermanentResidentIdUpdatetime(String permanentResidentIdUpdatetime) {
        this.permanentResidentIdUpdatetime = permanentResidentIdUpdatetime;
    }

    public String getCivilianStaffIdSource() {
        return civilianStaffIdSource;
    }

    public void setCivilianStaffIdSource(String civilianStaffIdSource) {
        this.civilianStaffIdSource = civilianStaffIdSource;
    }

    public String getCivilianStaffIdUpdatetime() {
        return civilianStaffIdUpdatetime;
    }

    public void setCivilianStaffIdUpdatetime(String civilianStaffIdUpdatetime) {
        this.civilianStaffIdUpdatetime = civilianStaffIdUpdatetime;
    }

    public String getStaffIdSource() {
        return staffIdSource;
    }

    public void setStaffIdSource(String staffIdSource) {
        this.staffIdSource = staffIdSource;
    }

    public String getStaffIdUpdatetime() {
        return staffIdUpdatetime;
    }

    public void setStaffIdUpdatetime(String staffIdUpdatetime) {
        this.staffIdUpdatetime = staffIdUpdatetime;
    }

    public String getOfficerIdCardSource() {
        return officerIdCardSource;
    }

    public void setOfficerIdCardSource(String officerIdCardSource) {
        this.officerIdCardSource = officerIdCardSource;
    }

    public String getOfficerIdCardUpdatetime() {
        return officerIdCardUpdatetime;
    }

    public void setOfficerIdCardUpdatetime(String officerIdCardUpdatetime) {
        this.officerIdCardUpdatetime = officerIdCardUpdatetime;
    }

    public String getArmedPoliceOfficerSource() {
        return armedPoliceOfficerSource;
    }

    public void setArmedPoliceOfficerSource(String armedPoliceOfficerSource) {
        this.armedPoliceOfficerSource = armedPoliceOfficerSource;
    }

    public String getArmedPoliceOfficerUpdatetime() {
        return armedPoliceOfficerUpdatetime;
    }

    public void setArmedPoliceOfficerUpdatetime(String armedPoliceOfficerUpdatetime) {
        this.armedPoliceOfficerUpdatetime = armedPoliceOfficerUpdatetime;
    }

    public String getArmedPoliceSoldierSource() {
        return armedPoliceSoldierSource;
    }

    public void setArmedPoliceSoldierSource(String armedPoliceSoldierSource) {
        this.armedPoliceSoldierSource = armedPoliceSoldierSource;
    }

    public String getArmedPoliceSoldierUpdatetime() {
        return armedPoliceSoldierUpdatetime;
    }

    public void setArmedPoliceSoldierUpdatetime(String armedPoliceSoldierUpdatetime) {
        this.armedPoliceSoldierUpdatetime = armedPoliceSoldierUpdatetime;
    }

    public String getCivilianOfficialIdSource() {
        return civilianOfficialIdSource;
    }

    public void setCivilianOfficialIdSource(String civilianOfficialIdSource) {
        this.civilianOfficialIdSource = civilianOfficialIdSource;
    }

    public String getCivilianOfficialIdUpdatetime() {
        return civilianOfficialIdUpdatetime;
    }

    public void setCivilianOfficialIdUpdatetime(String civilianOfficialIdUpdatetime) {
        this.civilianOfficialIdUpdatetime = civilianOfficialIdUpdatetime;
    }

    public String getConscriptSoldierIdSource() {
        return conscriptSoldierIdSource;
    }

    public void setConscriptSoldierIdSource(String conscriptSoldierIdSource) {
        this.conscriptSoldierIdSource = conscriptSoldierIdSource;
    }

    public String getConscriptSoldierIdUpdatetime() {
        return conscriptSoldierIdUpdatetime;
    }

    public void setConscriptSoldierIdUpdatetime(String conscriptSoldierIdUpdatetime) {
        this.conscriptSoldierIdUpdatetime = conscriptSoldierIdUpdatetime;
    }

    public String getNonCommissionedOfficerIdSource() {
        return nonCommissionedOfficerIdSource;
    }

    public void setNonCommissionedOfficerIdSource(String nonCommissionedOfficerIdSource) {
        this.nonCommissionedOfficerIdSource = nonCommissionedOfficerIdSource;
    }

    public String getNonCommissionedOfficerIdUpdatetime() {
        return nonCommissionedOfficerIdUpdatetime;
    }

    public void setNonCommissionedOfficerIdUpdatetime(String nonCommissionedOfficerIdUpdatetime) {
        this.nonCommissionedOfficerIdUpdatetime = nonCommissionedOfficerIdUpdatetime;
    }

    public String getHkMacaoResidentPermitSource() {
        return hkMacaoResidentPermitSource;
    }

    public void setHkMacaoResidentPermitSource(String hkMacaoResidentPermitSource) {
        this.hkMacaoResidentPermitSource = hkMacaoResidentPermitSource;
    }

    public String getHkMacaoResidentPermitUpdatetime() {
        return hkMacaoResidentPermitUpdatetime;
    }

    public void setHkMacaoResidentPermitUpdatetime(String hkMacaoResidentPermitUpdatetime) {
        this.hkMacaoResidentPermitUpdatetime = hkMacaoResidentPermitUpdatetime;
    }

    public String getTaiwanResidentTravelPermitSource() {
        return taiwanResidentTravelPermitSource;
    }

    public void setTaiwanResidentTravelPermitSource(String taiwanResidentTravelPermitSource) {
        this.taiwanResidentTravelPermitSource = taiwanResidentTravelPermitSource;
    }

    public String getTaiwanResidentTravelPermitUpdatetime() {
        return taiwanResidentTravelPermitUpdatetime;
    }

    public void setTaiwanResidentTravelPermitUpdatetime(String taiwanResidentTravelPermitUpdatetime) {
        this.taiwanResidentTravelPermitUpdatetime = taiwanResidentTravelPermitUpdatetime;
    }

    public String getBlindPassengerSource() {
        return blindPassengerSource;
    }

    public void setBlindPassengerSource(String blindPassengerSource) {
        this.blindPassengerSource = blindPassengerSource;
    }

    public String getBlindPassengerUpdatetime() {
        return blindPassengerUpdatetime;
    }

    public void setBlindPassengerUpdatetime(String blindPassengerUpdatetime) {
        this.blindPassengerUpdatetime = blindPassengerUpdatetime;
    }

    public String getDeafPassengerSource() {
        return deafPassengerSource;
    }

    public void setDeafPassengerSource(String deafPassengerSource) {
        this.deafPassengerSource = deafPassengerSource;
    }

    public String getDeafPassengerUpdatetime() {
        return deafPassengerUpdatetime;
    }

    public void setDeafPassengerUpdatetime(String deafPassengerUpdatetime) {
        this.deafPassengerUpdatetime = deafPassengerUpdatetime;
    }

    public String getIsDirectUserSource() {
        return isDirectUserSource;
    }

    public void setIsDirectUserSource(String isDirectUserSource) {
        this.isDirectUserSource = isDirectUserSource;
    }

    public String getIsDirectUserUpdatetime() {
        return isDirectUserUpdatetime;
    }

    public void setIsDirectUserUpdatetime(String isDirectUserUpdatetime) {
        this.isDirectUserUpdatetime = isDirectUserUpdatetime;
    }

    public String getIsOfficialWebsiteNonRegisteredSource() {
        return isOfficialWebsiteNonRegisteredSource;
    }

    public void setIsOfficialWebsiteNonRegisteredSource(String isOfficialWebsiteNonRegisteredSource) {
        this.isOfficialWebsiteNonRegisteredSource = isOfficialWebsiteNonRegisteredSource;
    }

    public String getIsOfficialWebsiteNonRegisteredUpdatetime() {
        return isOfficialWebsiteNonRegisteredUpdatetime;
    }

    public void setIsOfficialWebsiteNonRegisteredUpdatetime(String isOfficialWebsiteNonRegisteredUpdatetime) {
        this.isOfficialWebsiteNonRegisteredUpdatetime = isOfficialWebsiteNonRegisteredUpdatetime;
    }

    public String getIsDouyinCardPurchaserSource() {
        return isDouyinCardPurchaserSource;
    }

    public void setIsDouyinCardPurchaserSource(String isDouyinCardPurchaserSource) {
        this.isDouyinCardPurchaserSource = isDouyinCardPurchaserSource;
    }

    public String getIsDouyinCardPurchaserUpdatetime() {
        return isDouyinCardPurchaserUpdatetime;
    }

    public void setIsDouyinCardPurchaserUpdatetime(String isDouyinCardPurchaserUpdatetime) {
        this.isDouyinCardPurchaserUpdatetime = isDouyinCardPurchaserUpdatetime;
    }

    public String getDirectUserStatusSource() {
        return directUserStatusSource;
    }

    public void setDirectUserStatusSource(String directUserStatusSource) {
        this.directUserStatusSource = directUserStatusSource;
    }

    public String getDirectUserStatusUpdatetime() {
        return directUserStatusUpdatetime;
    }

    public void setDirectUserStatusUpdatetime(String directUserStatusUpdatetime) {
        this.directUserStatusUpdatetime = directUserStatusUpdatetime;
    }

    public String getDirectRegisterDateSource() {
        return directRegisterDateSource;
    }

    public void setDirectRegisterDateSource(String directRegisterDateSource) {
        this.directRegisterDateSource = directRegisterDateSource;
    }

    public String getDirectRegisterDateUpdatetime() {
        return directRegisterDateUpdatetime;
    }

    public void setDirectRegisterDateUpdatetime(String directRegisterDateUpdatetime) {
        this.directRegisterDateUpdatetime = directRegisterDateUpdatetime;
    }

    public String getDirectVerifiedFlagSource() {
        return directVerifiedFlagSource;
    }

    public void setDirectVerifiedFlagSource(String directVerifiedFlagSource) {
        this.directVerifiedFlagSource = directVerifiedFlagSource;
    }

    public String getDirectVerifiedFlagUpdatetime() {
        return directVerifiedFlagUpdatetime;
    }

    public void setDirectVerifiedFlagUpdatetime(String directVerifiedFlagUpdatetime) {
        this.directVerifiedFlagUpdatetime = directVerifiedFlagUpdatetime;
    }

    public String getDirectVerifyDateSource() {
        return directVerifyDateSource;
    }

    public void setDirectVerifyDateSource(String directVerifyDateSource) {
        this.directVerifyDateSource = directVerifyDateSource;
    }

    public String getDirectVerifyDateUpdatetime() {
        return directVerifyDateUpdatetime;
    }

    public void setDirectVerifyDateUpdatetime(String directVerifyDateUpdatetime) {
        this.directVerifyDateUpdatetime = directVerifyDateUpdatetime;
    }

    public String getIsBlacklistUserSource() {
        return isBlacklistUserSource;
    }

    public void setIsBlacklistUserSource(String isBlacklistUserSource) {
        this.isBlacklistUserSource = isBlacklistUserSource;
    }

    public String getIsBlacklistUserUpdatetime() {
        return isBlacklistUserUpdatetime;
    }

    public void setIsBlacklistUserUpdatetime(String isBlacklistUserUpdatetime) {
        this.isBlacklistUserUpdatetime = isBlacklistUserUpdatetime;
    }

    public String getStudentFlagSource() {
        return studentFlagSource;
    }

    public void setStudentFlagSource(String studentFlagSource) {
        this.studentFlagSource = studentFlagSource;
    }

    public String getStudentFlagUpdatetime() {
        return studentFlagUpdatetime;
    }

    public void setStudentFlagUpdatetime(String studentFlagUpdatetime) {
        this.studentFlagUpdatetime = studentFlagUpdatetime;
    }

    public String getTeacherFlagSource() {
        return teacherFlagSource;
    }

    public void setTeacherFlagSource(String teacherFlagSource) {
        this.teacherFlagSource = teacherFlagSource;
    }

    public String getTeacherFlagUpdatetime() {
        return teacherFlagUpdatetime;
    }

    public void setTeacherFlagUpdatetime(String teacherFlagUpdatetime) {
        this.teacherFlagUpdatetime = teacherFlagUpdatetime;
    }

    public String getAgentFlagSource() {
        return agentFlagSource;
    }

    public void setAgentFlagSource(String agentFlagSource) {
        this.agentFlagSource = agentFlagSource;
    }

    public String getAgentFlagUpdatetime() {
        return agentFlagUpdatetime;
    }

    public void setAgentFlagUpdatetime(String agentFlagUpdatetime) {
        this.agentFlagUpdatetime = agentFlagUpdatetime;
    }

    public String getIsKeyAccountSource() {
        return isKeyAccountSource;
    }

    public void setIsKeyAccountSource(String isKeyAccountSource) {
        this.isKeyAccountSource = isKeyAccountSource;
    }

    public String getIsKeyAccountUpdatetime() {
        return isKeyAccountUpdatetime;
    }

    public void setIsKeyAccountUpdatetime(String isKeyAccountUpdatetime) {
        this.isKeyAccountUpdatetime = isKeyAccountUpdatetime;
    }

    public String getKeyAccountNumberSource() {
        return keyAccountNumberSource;
    }

    public void setKeyAccountNumberSource(String keyAccountNumberSource) {
        this.keyAccountNumberSource = keyAccountNumberSource;
    }

    public String getKeyAccountNumberUpdatetime() {
        return keyAccountNumberUpdatetime;
    }

    public void setKeyAccountNumberUpdatetime(String keyAccountNumberUpdatetime) {
        this.keyAccountNumberUpdatetime = keyAccountNumberUpdatetime;
    }

    public String getIsFrequentTravelerSource() {
        return isFrequentTravelerSource;
    }

    public void setIsFrequentTravelerSource(String isFrequentTravelerSource) {
        this.isFrequentTravelerSource = isFrequentTravelerSource;
    }

    public String getIsFrequentTravelerUpdatetime() {
        return isFrequentTravelerUpdatetime;
    }

    public void setIsFrequentTravelerUpdatetime(String isFrequentTravelerUpdatetime) {
        this.isFrequentTravelerUpdatetime = isFrequentTravelerUpdatetime;
    }

    public String getFrequentTravelerCardnoSource() {
        return frequentTravelerCardnoSource;
    }

    public void setFrequentTravelerCardnoSource(String frequentTravelerCardnoSource) {
        this.frequentTravelerCardnoSource = frequentTravelerCardnoSource;
    }

    public String getFrequentTravelerCardnoUpdatetime() {
        return frequentTravelerCardnoUpdatetime;
    }

    public void setFrequentTravelerCardnoUpdatetime(String frequentTravelerCardnoUpdatetime) {
        this.frequentTravelerCardnoUpdatetime = frequentTravelerCardnoUpdatetime;
    }

    public String getFrequentTravelerLevelSource() {
        return frequentTravelerLevelSource;
    }

    public void setFrequentTravelerLevelSource(String frequentTravelerLevelSource) {
        this.frequentTravelerLevelSource = frequentTravelerLevelSource;
    }

    public String getFrequentTravelerLevelUpdatetime() {
        return frequentTravelerLevelUpdatetime;
    }

    public void setFrequentTravelerLevelUpdatetime(String frequentTravelerLevelUpdatetime) {
        this.frequentTravelerLevelUpdatetime = frequentTravelerLevelUpdatetime;
    }

    public String getYjCardNumberSource() {
        return yjCardNumberSource;
    }

    public void setYjCardNumberSource(String yjCardNumberSource) {
        this.yjCardNumberSource = yjCardNumberSource;
    }

    public String getYjCardNumberUpdatetime() {
        return yjCardNumberUpdatetime;
    }

    public void setYjCardNumberUpdatetime(String yjCardNumberUpdatetime) {
        this.yjCardNumberUpdatetime = yjCardNumberUpdatetime;
    }

    public String getYjCardExpiredateSource() {
        return yjCardExpiredateSource;
    }

    public void setYjCardExpiredateSource(String yjCardExpiredateSource) {
        this.yjCardExpiredateSource = yjCardExpiredateSource;
    }

    public String getYjCardExpiredateUpdatetime() {
        return yjCardExpiredateUpdatetime;
    }

    public void setYjCardExpiredateUpdatetime(String yjCardExpiredateUpdatetime) {
        this.yjCardExpiredateUpdatetime = yjCardExpiredateUpdatetime;
    }

    public String getDevChannelOneSource() {
        return devChannelOneSource;
    }

    public void setDevChannelOneSource(String devChannelOneSource) {
        this.devChannelOneSource = devChannelOneSource;
    }

    public String getDevChannelOneUpdatetime() {
        return devChannelOneUpdatetime;
    }

    public void setDevChannelOneUpdatetime(String devChannelOneUpdatetime) {
        this.devChannelOneUpdatetime = devChannelOneUpdatetime;
    }

    public String getDevChannelTwoSource() {
        return devChannelTwoSource;
    }

    public void setDevChannelTwoSource(String devChannelTwoSource) {
        this.devChannelTwoSource = devChannelTwoSource;
    }

    public String getDevChannelTwoUpdatetime() {
        return devChannelTwoUpdatetime;
    }

    public void setDevChannelTwoUpdatetime(String devChannelTwoUpdatetime) {
        this.devChannelTwoUpdatetime = devChannelTwoUpdatetime;
    }

    public String getDevChannelThreeSource() {
        return devChannelThreeSource;
    }

    public void setDevChannelThreeSource(String devChannelThreeSource) {
        this.devChannelThreeSource = devChannelThreeSource;
    }

    public String getDevChannelThreeUpdatetime() {
        return devChannelThreeUpdatetime;
    }

    public void setDevChannelThreeUpdatetime(String devChannelThreeUpdatetime) {
        this.devChannelThreeUpdatetime = devChannelThreeUpdatetime;
    }

    public String getDevChannelFourSource() {
        return devChannelFourSource;
    }

    public void setDevChannelFourSource(String devChannelFourSource) {
        this.devChannelFourSource = devChannelFourSource;
    }

    public String getDevChannelFourUpdatetime() {
        return devChannelFourUpdatetime;
    }

    public void setDevChannelFourUpdatetime(String devChannelFourUpdatetime) {
        this.devChannelFourUpdatetime = devChannelFourUpdatetime;
    }

    public String getFtRegisterTimeSource() {
        return ftRegisterTimeSource;
    }

    public void setFtRegisterTimeSource(String ftRegisterTimeSource) {
        this.ftRegisterTimeSource = ftRegisterTimeSource;
    }

    public String getFtRegisterTimeUpdatetime() {
        return ftRegisterTimeUpdatetime;
    }

    public void setFtRegisterTimeUpdatetime(String ftRegisterTimeUpdatetime) {
        this.ftRegisterTimeUpdatetime = ftRegisterTimeUpdatetime;
    }

    public String getAcceptSmsMarketingSource() {
        return acceptSmsMarketingSource;
    }

    public void setAcceptSmsMarketingSource(String acceptSmsMarketingSource) {
        this.acceptSmsMarketingSource = acceptSmsMarketingSource;
    }

    public String getAcceptSmsMarketingUpdatetime() {
        return acceptSmsMarketingUpdatetime;
    }

    public void setAcceptSmsMarketingUpdatetime(String acceptSmsMarketingUpdatetime) {
        this.acceptSmsMarketingUpdatetime = acceptSmsMarketingUpdatetime;
    }

    public String getAcceptEmailMarketingSource() {
        return acceptEmailMarketingSource;
    }

    public void setAcceptEmailMarketingSource(String acceptEmailMarketingSource) {
        this.acceptEmailMarketingSource = acceptEmailMarketingSource;
    }

    public String getAcceptEmailMarketingUpdatetime() {
        return acceptEmailMarketingUpdatetime;
    }

    public void setAcceptEmailMarketingUpdatetime(String acceptEmailMarketingUpdatetime) {
        this.acceptEmailMarketingUpdatetime = acceptEmailMarketingUpdatetime;
    }

    public String getParentFtCardnoSource() {
        return parentFtCardnoSource;
    }

    public void setParentFtCardnoSource(String parentFtCardnoSource) {
        this.parentFtCardnoSource = parentFtCardnoSource;
    }

    public String getParentFtCardnoUpdatetime() {
        return parentFtCardnoUpdatetime;
    }

    public void setParentFtCardnoUpdatetime(String parentFtCardnoUpdatetime) {
        this.parentFtCardnoUpdatetime = parentFtCardnoUpdatetime;
    }

    public String getIsLyUserSource() {
        return isLyUserSource;
    }

    public void setIsLyUserSource(String isLyUserSource) {
        this.isLyUserSource = isLyUserSource;
    }

    public String getIsLyUserUpdatetime() {
        return isLyUserUpdatetime;
    }

    public void setIsLyUserUpdatetime(String isLyUserUpdatetime) {
        this.isLyUserUpdatetime = isLyUserUpdatetime;
    }

    public String getLyRegisterTimeSource() {
        return lyRegisterTimeSource;
    }

    public void setLyRegisterTimeSource(String lyRegisterTimeSource) {
        this.lyRegisterTimeSource = lyRegisterTimeSource;
    }

    public String getLyRegisterTimeUpdatetime() {
        return lyRegisterTimeUpdatetime;
    }

    public void setLyRegisterTimeUpdatetime(String lyRegisterTimeUpdatetime) {
        this.lyRegisterTimeUpdatetime = lyRegisterTimeUpdatetime;
    }

    public String getLyCardNumberSource() {
        return lyCardNumberSource;
    }

    public void setLyCardNumberSource(String lyCardNumberSource) {
        this.lyCardNumberSource = lyCardNumberSource;
    }

    public String getLyCardNumberUpdatetime() {
        return lyCardNumberUpdatetime;
    }

    public void setLyCardNumberUpdatetime(String lyCardNumberUpdatetime) {
        this.lyCardNumberUpdatetime = lyCardNumberUpdatetime;
    }

    public String getLyUserLevelSource() {
        return lyUserLevelSource;
    }

    public void setLyUserLevelSource(String lyUserLevelSource) {
        this.lyUserLevelSource = lyUserLevelSource;
    }

    public String getLyUserLevelUpdatetime() {
        return lyUserLevelUpdatetime;
    }

    public void setLyUserLevelUpdatetime(String lyUserLevelUpdatetime) {
        this.lyUserLevelUpdatetime = lyUserLevelUpdatetime;
    }

    public String getLyUserStatusSource() {
        return lyUserStatusSource;
    }

    public void setLyUserStatusSource(String lyUserStatusSource) {
        this.lyUserStatusSource = lyUserStatusSource;
    }

    public String getLyUserStatusUpdatetime() {
        return lyUserStatusUpdatetime;
    }

    public void setLyUserStatusUpdatetime(String lyUserStatusUpdatetime) {
        this.lyUserStatusUpdatetime = lyUserStatusUpdatetime;
    }

    public String getLyRegisterStatusSource() {
        return lyRegisterStatusSource;
    }

    public void setLyRegisterStatusSource(String lyRegisterStatusSource) {
        this.lyRegisterStatusSource = lyRegisterStatusSource;
    }

    public String getLyRegisterStatusUpdatetime() {
        return lyRegisterStatusUpdatetime;
    }

    public void setLyRegisterStatusUpdatetime(String lyRegisterStatusUpdatetime) {
        this.lyRegisterStatusUpdatetime = lyRegisterStatusUpdatetime;
    }

    public String getLyVerifyStatusSource() {
        return lyVerifyStatusSource;
    }

    public void setLyVerifyStatusSource(String lyVerifyStatusSource) {
        this.lyVerifyStatusSource = lyVerifyStatusSource;
    }

    public String getLyVerifyStatusUpdatetime() {
        return lyVerifyStatusUpdatetime;
    }

    public void setLyVerifyStatusUpdatetime(String lyVerifyStatusUpdatetime) {
        this.lyVerifyStatusUpdatetime = lyVerifyStatusUpdatetime;
    }

    public String getLyLifetimePointsSource() {
        return lyLifetimePointsSource;
    }

    public void setLyLifetimePointsSource(String lyLifetimePointsSource) {
        this.lyLifetimePointsSource = lyLifetimePointsSource;
    }

    public String getLyLifetimePointsUpdatetime() {
        return lyLifetimePointsUpdatetime;
    }

    public void setLyLifetimePointsUpdatetime(String lyLifetimePointsUpdatetime) {
        this.lyLifetimePointsUpdatetime = lyLifetimePointsUpdatetime;
    }

    public String getLyAvailablePointsSource() {
        return lyAvailablePointsSource;
    }

    public void setLyAvailablePointsSource(String lyAvailablePointsSource) {
        this.lyAvailablePointsSource = lyAvailablePointsSource;
    }

    public String getLyAvailablePointsUpdatetime() {
        return lyAvailablePointsUpdatetime;
    }

    public void setLyAvailablePointsUpdatetime(String lyAvailablePointsUpdatetime) {
        this.lyAvailablePointsUpdatetime = lyAvailablePointsUpdatetime;
    }

    public String getIsHighTravelerSource() {
        return isHighTravelerSource;
    }

    public void setIsHighTravelerSource(String isHighTravelerSource) {
        this.isHighTravelerSource = isHighTravelerSource;
    }

    public String getIsHighTravelerUpdatetime() {
        return isHighTravelerUpdatetime;
    }

    public void setIsHighTravelerUpdatetime(String isHighTravelerUpdatetime) {
        this.isHighTravelerUpdatetime = isHighTravelerUpdatetime;
    }

    public String getHighTravelerTierSource() {
        return highTravelerTierSource;
    }

    public void setHighTravelerTierSource(String highTravelerTierSource) {
        this.highTravelerTierSource = highTravelerTierSource;
    }

    public String getHighTravelerTierUpdatetime() {
        return highTravelerTierUpdatetime;
    }

    public void setHighTravelerTierUpdatetime(String highTravelerTierUpdatetime) {
        this.highTravelerTierUpdatetime = highTravelerTierUpdatetime;
    }

    public String getTierPrestigeExpiredateSource() {
        return tierPrestigeExpiredateSource;
    }

    public void setTierPrestigeExpiredateSource(String tierPrestigeExpiredateSource) {
        this.tierPrestigeExpiredateSource = tierPrestigeExpiredateSource;
    }

    public String getTierPrestigeExpiredateUpdatetime() {
        return tierPrestigeExpiredateUpdatetime;
    }

    public void setTierPrestigeExpiredateUpdatetime(String tierPrestigeExpiredateUpdatetime) {
        this.tierPrestigeExpiredateUpdatetime = tierPrestigeExpiredateUpdatetime;
    }

    public String getTierHonorExpiredateSource() {
        return tierHonorExpiredateSource;
    }

    public void setTierHonorExpiredateSource(String tierHonorExpiredateSource) {
        this.tierHonorExpiredateSource = tierHonorExpiredateSource;
    }

    public String getTierHonorExpiredateUpdatetime() {
        return tierHonorExpiredateUpdatetime;
    }

    public void setTierHonorExpiredateUpdatetime(String tierHonorExpiredateUpdatetime) {
        this.tierHonorExpiredateUpdatetime = tierHonorExpiredateUpdatetime;
    }

    public String getHighTravelerDsSource() {
        return highTravelerDsSource;
    }

    public void setHighTravelerDsSource(String highTravelerDsSource) {
        this.highTravelerDsSource = highTravelerDsSource;
    }

    public String getHighTravelerDsUpdatetime() {
        return highTravelerDsUpdatetime;
    }

    public void setHighTravelerDsUpdatetime(String highTravelerDsUpdatetime) {
        this.highTravelerDsUpdatetime = highTravelerDsUpdatetime;
    }

    public String getIsYjPersonnelSource() {
        return isYjPersonnelSource;
    }

    public void setIsYjPersonnelSource(String isYjPersonnelSource) {
        this.isYjPersonnelSource = isYjPersonnelSource;
    }

    public String getIsYjPersonnelUpdatetime() {
        return isYjPersonnelUpdatetime;
    }

    public void setIsYjPersonnelUpdatetime(String isYjPersonnelUpdatetime) {
        this.isYjPersonnelUpdatetime = isYjPersonnelUpdatetime;
    }

    public String getVipFlagSource() {
        return vipFlagSource;
    }

    public void setVipFlagSource(String vipFlagSource) {
        this.vipFlagSource = vipFlagSource;
    }

    public String getVipFlagUpdatetime() {
        return vipFlagUpdatetime;
    }

    public void setVipFlagUpdatetime(String vipFlagUpdatetime) {
        this.vipFlagUpdatetime = vipFlagUpdatetime;
    }

    public String getCipFlagSource() {
        return cipFlagSource;
    }

    public void setCipFlagSource(String cipFlagSource) {
        this.cipFlagSource = cipFlagSource;
    }

    public String getCipFlagUpdatetime() {
        return cipFlagUpdatetime;
    }

    public void setCipFlagUpdatetime(String cipFlagUpdatetime) {
        this.cipFlagUpdatetime = cipFlagUpdatetime;
    }

    public String getVvipFlagSource() {
        return vvipFlagSource;
    }

    public void setVvipFlagSource(String vvipFlagSource) {
        this.vvipFlagSource = vvipFlagSource;
    }

    public String getVvipFlagUpdatetime() {
        return vvipFlagUpdatetime;
    }

    public void setVvipFlagUpdatetime(String vvipFlagUpdatetime) {
        this.vvipFlagUpdatetime = vvipFlagUpdatetime;
    }

    public String getFoodPreferenceSource() {
        return foodPreferenceSource;
    }

    public void setFoodPreferenceSource(String foodPreferenceSource) {
        this.foodPreferenceSource = foodPreferenceSource;
    }

    public String getFoodPreferenceUpdatetime() {
        return foodPreferenceUpdatetime;
    }

    public void setFoodPreferenceUpdatetime(String foodPreferenceUpdatetime) {
        this.foodPreferenceUpdatetime = foodPreferenceUpdatetime;
    }

    public String getSeatPreferenceSource() {
        return seatPreferenceSource;
    }

    public void setSeatPreferenceSource(String seatPreferenceSource) {
        this.seatPreferenceSource = seatPreferenceSource;
    }

    public String getSeatPreferenceUpdatetime() {
        return seatPreferenceUpdatetime;
    }

    public void setSeatPreferenceUpdatetime(String seatPreferenceUpdatetime) {
        this.seatPreferenceUpdatetime = seatPreferenceUpdatetime;
    }

    public String getTempFoodPreferenceSource() {
        return tempFoodPreferenceSource;
    }

    public void setTempFoodPreferenceSource(String tempFoodPreferenceSource) {
        this.tempFoodPreferenceSource = tempFoodPreferenceSource;
    }

    public String getTempFoodPreferenceUpdatetime() {
        return tempFoodPreferenceUpdatetime;
    }

    public void setTempFoodPreferenceUpdatetime(String tempFoodPreferenceUpdatetime) {
        this.tempFoodPreferenceUpdatetime = tempFoodPreferenceUpdatetime;
    }

    public String getTempSeatPreferenceSource() {
        return tempSeatPreferenceSource;
    }

    public void setTempSeatPreferenceSource(String tempSeatPreferenceSource) {
        this.tempSeatPreferenceSource = tempSeatPreferenceSource;
    }

    public String getTempSeatPreferenceUpdatetime() {
        return tempSeatPreferenceUpdatetime;
    }

    public void setTempSeatPreferenceUpdatetime(String tempSeatPreferenceUpdatetime) {
        this.tempSeatPreferenceUpdatetime = tempSeatPreferenceUpdatetime;
    }

    public String getBeveragePreferenceSource() {
        return beveragePreferenceSource;
    }

    public void setBeveragePreferenceSource(String beveragePreferenceSource) {
        this.beveragePreferenceSource = beveragePreferenceSource;
    }

    public String getBeveragePreferenceUpdatetime() {
        return beveragePreferenceUpdatetime;
    }

    public void setBeveragePreferenceUpdatetime(String beveragePreferenceUpdatetime) {
        this.beveragePreferenceUpdatetime = beveragePreferenceUpdatetime;
    }

    public String getLongTermSeatPreferenceSource() {
        return longTermSeatPreferenceSource;
    }

    public void setLongTermSeatPreferenceSource(String longTermSeatPreferenceSource) {
        this.longTermSeatPreferenceSource = longTermSeatPreferenceSource;
    }

    public String getLongTermSeatPreferenceUpdatetime() {
        return longTermSeatPreferenceUpdatetime;
    }

    public void setLongTermSeatPreferenceUpdatetime(String longTermSeatPreferenceUpdatetime) {
        this.longTermSeatPreferenceUpdatetime = longTermSeatPreferenceUpdatetime;
    }

    public String getFirstClassLoungePreferenceSource() {
        return firstClassLoungePreferenceSource;
    }

    public void setFirstClassLoungePreferenceSource(String firstClassLoungePreferenceSource) {
        this.firstClassLoungePreferenceSource = firstClassLoungePreferenceSource;
    }

    public String getFirstClassLoungePreferenceUpdatetime() {
        return firstClassLoungePreferenceUpdatetime;
    }

    public void setFirstClassLoungePreferenceUpdatetime(String firstClassLoungePreferenceUpdatetime) {
        this.firstClassLoungePreferenceUpdatetime = firstClassLoungePreferenceUpdatetime;
    }

    public String getMergedToUseridSource() {
        return mergedToUseridSource;
    }

    public void setMergedToUseridSource(String mergedToUseridSource) {
        this.mergedToUseridSource = mergedToUseridSource;
    }

    public String getMergedToUseridUpdatetime() {
        return mergedToUseridUpdatetime;
    }

    public void setMergedToUseridUpdatetime(String mergedToUseridUpdatetime) {
        this.mergedToUseridUpdatetime = mergedToUseridUpdatetime;
    }

    public String getIsFrequentFlyerNumberVerifiedSource() {
        return isFrequentFlyerNumberVerifiedSource;
    }

    public void setIsFrequentFlyerNumberVerifiedSource(String isFrequentFlyerNumberVerifiedSource) {
        this.isFrequentFlyerNumberVerifiedSource = isFrequentFlyerNumberVerifiedSource;
    }

    public String getIsFrequentFlyerNumberVerifiedUpdatetime() {
        return isFrequentFlyerNumberVerifiedUpdatetime;
    }

    public void setIsFrequentFlyerNumberVerifiedUpdatetime(String isFrequentFlyerNumberVerifiedUpdatetime) {
        this.isFrequentFlyerNumberVerifiedUpdatetime = isFrequentFlyerNumberVerifiedUpdatetime;
    }

    public String getHkMacaoTaiwanResidencePermitUpdatetime() {
        return hkMacaoTaiwanResidencePermitUpdatetime;
    }

    @Override
    public String toString() {
        return "UserDimSummaryModel{" +
                "pkId='" + pkId + '\'' +
                ", crmCustomerIdSource='" + crmCustomerIdSource + '\'' +
                ", crmCustomerIdUpdatetime='" + crmCustomerIdUpdatetime + '\'' +
                ", lyVipIdSource='" + lyVipIdSource + '\'' +
                ", lyVipIdUpdatetime='" + lyVipIdUpdatetime + '\'' +
                ", lyMemberIdSource='" + lyMemberIdSource + '\'' +
                ", lyMemberIdUpdatetime='" + lyMemberIdUpdatetime + '\'' +
                ", sysRegisterIdSource='" + sysRegisterIdSource + '\'' +
                ", sysRegisterIdUpdatetime='" + sysRegisterIdUpdatetime + '\'' +
                ", ffpRegisterIdSource='" + ffpRegisterIdSource + '\'' +
                ", ffpRegisterIdUpdatetime='" + ffpRegisterIdUpdatetime + '\'' +
                ", historicalFfpIdSource='" + historicalFfpIdSource + '\'' +
                ", historicalFfpIdUpdatetime='" + historicalFfpIdUpdatetime + '\'' +
                ", alipayIdSource='" + alipayIdSource + '\'' +
                ", alipayIdUpdatetime='" + alipayIdUpdatetime + '\'' +
                ", wechatIdSource='" + wechatIdSource + '\'' +
                ", wechatIdUpdatetime='" + wechatIdUpdatetime + '\'' +
                ", douyinIdSource='" + douyinIdSource + '\'' +
                ", douyinIdUpdatetime='" + douyinIdUpdatetime + '\'' +
                ", cnNameSource='" + cnNameSource + '\'' +
                ", cnNameUpdatetime='" + cnNameUpdatetime + '\'' +
                ", enNameSource='" + enNameSource + '\'' +
                ", enNameUpdatetime='" + enNameUpdatetime + '\'' +
                ", sexSource='" + sexSource + '\'' +
                ", sexUpdatetime='" + sexUpdatetime + '\'' +
                ", userTypeSource='" + userTypeSource + '\'' +
                ", userTypeUpdatetime='" + userTypeUpdatetime + '\'' +
                ", birthdaySource='" + birthdaySource + '\'' +
                ", birthdayUpdatetime='" + birthdayUpdatetime + '\'' +
                ", provinceSource='" + provinceSource + '\'' +
                ", provinceUpdatetime='" + provinceUpdatetime + '\'' +
                ", citySource='" + citySource + '\'' +
                ", cityUpdatetime='" + cityUpdatetime + '\'' +
                ", nationalitySource='" + nationalitySource + '\'' +
                ", nationalityUpdatetime='" + nationalityUpdatetime + '\'' +
                ", ethnicitySource='" + ethnicitySource + '\'' +
                ", ethnicityUpdatetime='" + ethnicityUpdatetime + '\'' +
                ", employerSource='" + employerSource + '\'' +
                ", employerUpdatetime='" + employerUpdatetime + '\'' +
                ", mobilePhoneSource='" + mobilePhoneSource + '\'' +
                ", mobilePhoneUpdatetime='" + mobilePhoneUpdatetime + '\'' +
                ", mobileNumberReliabilitySource='" + mobileNumberReliabilitySource + '\'' +
                ", mobileNumberReliabilityUpdatetime='" + mobileNumberReliabilityUpdatetime + '\'' +
                ", emailSource='" + emailSource + '\'' +
                ", emailUpdatetime='" + emailUpdatetime + '\'' +
                ", idCardSource='" + idCardSource + '\'' +
                ", idCardUpdatetime='" + idCardUpdatetime + '\'' +
                ", passportSource='" + passportSource + '\'' +
                ", passportUpdatetime='" + passportUpdatetime + '\'' +
                ", seamanIdSource='" + seamanIdSource + '\'' +
                ", seamanIdUpdatetime='" + seamanIdUpdatetime + '\'' +
                ", alienPermitSource='" + alienPermitSource + '\'' +
                ", alienPermitUpdatetime='" + alienPermitUpdatetime + '\'' +
                ", diplomaticStaffCertificateSource='" + diplomaticStaffCertificateSource + '\'' +
                ", diplomaticStaffCertificateUpdatetime='" + diplomaticStaffCertificateUpdatetime + '\'' +
                ", permanentResidentIdSource='" + permanentResidentIdSource + '\'' +
                ", permanentResidentIdUpdatetime='" + permanentResidentIdUpdatetime + '\'' +
                ", civilianStaffIdSource='" + civilianStaffIdSource + '\'' +
                ", civilianStaffIdUpdatetime='" + civilianStaffIdUpdatetime + '\'' +
                ", staffIdSource='" + staffIdSource + '\'' +
                ", staffIdUpdatetime='" + staffIdUpdatetime + '\'' +
                ", officerIdCardSource='" + officerIdCardSource + '\'' +
                ", officerIdCardUpdatetime='" + officerIdCardUpdatetime + '\'' +
                ", armedPoliceOfficerSource='" + armedPoliceOfficerSource + '\'' +
                ", armedPoliceOfficerUpdatetime='" + armedPoliceOfficerUpdatetime + '\'' +
                ", armedPoliceSoldierSource='" + armedPoliceSoldierSource + '\'' +
                ", armedPoliceSoldierUpdatetime='" + armedPoliceSoldierUpdatetime + '\'' +
                ", civilianOfficialIdSource='" + civilianOfficialIdSource + '\'' +
                ", civilianOfficialIdUpdatetime='" + civilianOfficialIdUpdatetime + '\'' +
                ", conscriptSoldierIdSource='" + conscriptSoldierIdSource + '\'' +
                ", conscriptSoldierIdUpdatetime='" + conscriptSoldierIdUpdatetime + '\'' +
                ", nonCommissionedOfficerIdSource='" + nonCommissionedOfficerIdSource + '\'' +
                ", nonCommissionedOfficerIdUpdatetime='" + nonCommissionedOfficerIdUpdatetime + '\'' +
                ", hkMacaoResidentPermitSource='" + hkMacaoResidentPermitSource + '\'' +
                ", hkMacaoResidentPermitUpdatetime='" + hkMacaoResidentPermitUpdatetime + '\'' +
                ", taiwanResidentTravelPermitSource='" + taiwanResidentTravelPermitSource + '\'' +
                ", taiwanResidentTravelPermitUpdatetime='" + taiwanResidentTravelPermitUpdatetime + '\'' +
                ", blindPassengerSource='" + blindPassengerSource + '\'' +
                ", blindPassengerUpdatetime='" + blindPassengerUpdatetime + '\'' +
                ", deafPassengerSource='" + deafPassengerSource + '\'' +
                ", deafPassengerUpdatetime='" + deafPassengerUpdatetime + '\'' +
                ", isDirectUserSource='" + isDirectUserSource + '\'' +
                ", isDirectUserUpdatetime='" + isDirectUserUpdatetime + '\'' +
                ", isOfficialWebsiteNonRegisteredSource='" + isOfficialWebsiteNonRegisteredSource + '\'' +
                ", isOfficialWebsiteNonRegisteredUpdatetime='" + isOfficialWebsiteNonRegisteredUpdatetime + '\'' +
                ", isDouyinCardPurchaserSource='" + isDouyinCardPurchaserSource + '\'' +
                ", isDouyinCardPurchaserUpdatetime='" + isDouyinCardPurchaserUpdatetime + '\'' +
                ", directUserStatusSource='" + directUserStatusSource + '\'' +
                ", directUserStatusUpdatetime='" + directUserStatusUpdatetime + '\'' +
                ", directRegisterDateSource='" + directRegisterDateSource + '\'' +
                ", directRegisterDateUpdatetime='" + directRegisterDateUpdatetime + '\'' +
                ", directLastLoginDateSource='" + directLastLoginDateSource + '\'' +
                ", directLastLoginDateUpdatetime='" + directLastLoginDateUpdatetime + '\'' +
                ", directVerifiedFlagSource='" + directVerifiedFlagSource + '\'' +
                ", directVerifiedFlagUpdatetime='" + directVerifiedFlagUpdatetime + '\'' +
                ", directVerifyDateSource='" + directVerifyDateSource + '\'' +
                ", directVerifyDateUpdatetime='" + directVerifyDateUpdatetime + '\'' +
                ", isBlacklistUserSource='" + isBlacklistUserSource + '\'' +
                ", isBlacklistUserUpdatetime='" + isBlacklistUserUpdatetime + '\'' +
                ", studentFlagSource='" + studentFlagSource + '\'' +
                ", studentFlagUpdatetime='" + studentFlagUpdatetime + '\'' +
                ", teacherFlagSource='" + teacherFlagSource + '\'' +
                ", teacherFlagUpdatetime='" + teacherFlagUpdatetime + '\'' +
                ", agentFlagSource='" + agentFlagSource + '\'' +
                ", agentFlagUpdatetime='" + agentFlagUpdatetime + '\'' +
                ", isKeyAccountSource='" + isKeyAccountSource + '\'' +
                ", isKeyAccountUpdatetime='" + isKeyAccountUpdatetime + '\'' +
                ", keyAccountNumberSource='" + keyAccountNumberSource + '\'' +
                ", keyAccountNumberUpdatetime='" + keyAccountNumberUpdatetime + '\'' +
                ", isFrequentTravelerSource='" + isFrequentTravelerSource + '\'' +
                ", isFrequentTravelerUpdatetime='" + isFrequentTravelerUpdatetime + '\'' +
                ", frequentTravelerCardnoSource='" + frequentTravelerCardnoSource + '\'' +
                ", frequentTravelerCardnoUpdatetime='" + frequentTravelerCardnoUpdatetime + '\'' +
                ", frequentTravelerLevelSource='" + frequentTravelerLevelSource + '\'' +
                ", frequentTravelerLevelUpdatetime='" + frequentTravelerLevelUpdatetime + '\'' +
                ", yjCardNumberSource='" + yjCardNumberSource + '\'' +
                ", yjCardNumberUpdatetime='" + yjCardNumberUpdatetime + '\'' +
                ", yjCardExpiredateSource='" + yjCardExpiredateSource + '\'' +
                ", yjCardExpiredateUpdatetime='" + yjCardExpiredateUpdatetime + '\'' +
                ", devChannelOneSource='" + devChannelOneSource + '\'' +
                ", devChannelOneUpdatetime='" + devChannelOneUpdatetime + '\'' +
                ", devChannelTwoSource='" + devChannelTwoSource + '\'' +
                ", devChannelTwoUpdatetime='" + devChannelTwoUpdatetime + '\'' +
                ", devChannelThreeSource='" + devChannelThreeSource + '\'' +
                ", devChannelThreeUpdatetime='" + devChannelThreeUpdatetime + '\'' +
                ", devChannelFourSource='" + devChannelFourSource + '\'' +
                ", devChannelFourUpdatetime='" + devChannelFourUpdatetime + '\'' +
                ", ftRegisterTimeSource='" + ftRegisterTimeSource + '\'' +
                ", ftRegisterTimeUpdatetime='" + ftRegisterTimeUpdatetime + '\'' +
                ", acceptSmsMarketingSource='" + acceptSmsMarketingSource + '\'' +
                ", acceptSmsMarketingUpdatetime='" + acceptSmsMarketingUpdatetime + '\'' +
                ", acceptEmailMarketingSource='" + acceptEmailMarketingSource + '\'' +
                ", acceptEmailMarketingUpdatetime='" + acceptEmailMarketingUpdatetime + '\'' +
                ", parentFtCardnoSource='" + parentFtCardnoSource + '\'' +
                ", parentFtCardnoUpdatetime='" + parentFtCardnoUpdatetime + '\'' +
                ", isLyUserSource='" + isLyUserSource + '\'' +
                ", isLyUserUpdatetime='" + isLyUserUpdatetime + '\'' +
                ", lyRegisterTimeSource='" + lyRegisterTimeSource + '\'' +
                ", lyRegisterTimeUpdatetime='" + lyRegisterTimeUpdatetime + '\'' +
                ", lyCardNumberSource='" + lyCardNumberSource + '\'' +
                ", lyCardNumberUpdatetime='" + lyCardNumberUpdatetime + '\'' +
                ", lyUserLevelSource='" + lyUserLevelSource + '\'' +
                ", lyUserLevelUpdatetime='" + lyUserLevelUpdatetime + '\'' +
                ", lyUserStatusSource='" + lyUserStatusSource + '\'' +
                ", lyUserStatusUpdatetime='" + lyUserStatusUpdatetime + '\'' +
                ", lyRegisterStatusSource='" + lyRegisterStatusSource + '\'' +
                ", lyRegisterStatusUpdatetime='" + lyRegisterStatusUpdatetime + '\'' +
                ", lyVerifyStatusSource='" + lyVerifyStatusSource + '\'' +
                ", lyVerifyStatusUpdatetime='" + lyVerifyStatusUpdatetime + '\'' +
                ", lyLifetimePointsSource='" + lyLifetimePointsSource + '\'' +
                ", lyLifetimePointsUpdatetime='" + lyLifetimePointsUpdatetime + '\'' +
                ", lyAvailablePointsSource='" + lyAvailablePointsSource + '\'' +
                ", lyAvailablePointsUpdatetime='" + lyAvailablePointsUpdatetime + '\'' +
                ", isHighTravelerSource='" + isHighTravelerSource + '\'' +
                ", isHighTravelerUpdatetime='" + isHighTravelerUpdatetime + '\'' +
                ", highTravelerTypeSource='" + highTravelerTypeSource + '\'' +
                ", highTravelerTypeUpdatetime='" + highTravelerTypeUpdatetime + '\'' +
                ", highTravelerTierSource='" + highTravelerTierSource + '\'' +
                ", highTravelerTierUpdatetime='" + highTravelerTierUpdatetime + '\'' +
                ", tierPrestigeExpiredateSource='" + tierPrestigeExpiredateSource + '\'' +
                ", tierPrestigeExpiredateUpdatetime='" + tierPrestigeExpiredateUpdatetime + '\'' +
                ", tierHonorExpiredateSource='" + tierHonorExpiredateSource + '\'' +
                ", tierHonorExpiredateUpdatetime='" + tierHonorExpiredateUpdatetime + '\'' +
                ", highTravelerDsSource='" + highTravelerDsSource + '\'' +
                ", highTravelerDsUpdatetime='" + highTravelerDsUpdatetime + '\'' +
                ", isYjPersonnelSource='" + isYjPersonnelSource + '\'' +
                ", isYjPersonnelUpdatetime='" + isYjPersonnelUpdatetime + '\'' +
                ", vipFlagSource='" + vipFlagSource + '\'' +
                ", vipFlagUpdatetime='" + vipFlagUpdatetime + '\'' +
                ", cipFlagSource='" + cipFlagSource + '\'' +
                ", cipFlagUpdatetime='" + cipFlagUpdatetime + '\'' +
                ", vvipFlagSource='" + vvipFlagSource + '\'' +
                ", vvipFlagUpdatetime='" + vvipFlagUpdatetime + '\'' +
                ", foodPreferenceSource='" + foodPreferenceSource + '\'' +
                ", foodPreferenceUpdatetime='" + foodPreferenceUpdatetime + '\'' +
                ", seatPreferenceSource='" + seatPreferenceSource + '\'' +
                ", seatPreferenceUpdatetime='" + seatPreferenceUpdatetime + '\'' +
                ", tempFoodPreferenceSource='" + tempFoodPreferenceSource + '\'' +
                ", tempFoodPreferenceUpdatetime='" + tempFoodPreferenceUpdatetime + '\'' +
                ", tempSeatPreferenceSource='" + tempSeatPreferenceSource + '\'' +
                ", tempSeatPreferenceUpdatetime='" + tempSeatPreferenceUpdatetime + '\'' +
                ", beveragePreferenceSource='" + beveragePreferenceSource + '\'' +
                ", beveragePreferenceUpdatetime='" + beveragePreferenceUpdatetime + '\'' +
                ", longTermSeatPreferenceSource='" + longTermSeatPreferenceSource + '\'' +
                ", longTermSeatPreferenceUpdatetime='" + longTermSeatPreferenceUpdatetime + '\'' +
                ", firstClassLoungePreferenceSource='" + firstClassLoungePreferenceSource + '\'' +
                ", firstClassLoungePreferenceUpdatetime='" + firstClassLoungePreferenceUpdatetime + '\'' +
                ", mergedToUseridSource='" + mergedToUseridSource + '\'' +
                ", mergedToUseridUpdatetime='" + mergedToUseridUpdatetime + '\'' +
                ", isValidUserSource='" + isValidUserSource + '\'' +
                ", isValidUserUpdatetime='" + isValidUserUpdatetime + '\'' +
                ", isFrequentFlyerNumberVerifiedSource='" + isFrequentFlyerNumberVerifiedSource + '\'' +
                ", isFrequentFlyerNumberVerifiedUpdatetime='" + isFrequentFlyerNumberVerifiedUpdatetime + '\'' +
                ", hkMacaoTravelPermitSource='" + hkMacaoTravelPermitSource + '\'' +
                ", hkMacaoTravelPermitUpdatetime='" + hkMacaoTravelPermitUpdatetime + '\'' +
                ", mainlandToTaiwanTravelPermitSource='" + mainlandToTaiwanTravelPermitSource + '\'' +
                ", mainlandToTaiwanTravelPermitUpdatetime='" + mainlandToTaiwanTravelPermitUpdatetime + '\'' +
                ", hkMacaoTaiwanIdCardSource='" + hkMacaoTaiwanIdCardSource + '\'' +
                ", hkMacaoTaiwanIdCardUpdatetime='" + hkMacaoTaiwanIdCardUpdatetime + '\'' +
                ", hkMacaoTaiwanResidencePermitSource='" + hkMacaoTaiwanResidencePermitSource + '\'' +
                ", hkMacaoTaiwanResidencePermitUpdatetime='" + hkMacaoTaiwanResidencePermitUpdatetime + '\'' +
                '}';
    }

    public void setHkMacaoTaiwanResidencePermitUpdatetime(String hkMacaoTaiwanResidencePermitUpdatetime) {
        this.hkMacaoTaiwanResidencePermitUpdatetime = hkMacaoTaiwanResidencePermitUpdatetime;
    }

    public String getHkMacaoTaiwanResidencePermitSource() {
        return hkMacaoTaiwanResidencePermitSource;
    }

    public void setHkMacaoTaiwanResidencePermitSource(String hkMacaoTaiwanResidencePermitSource) {
        this.hkMacaoTaiwanResidencePermitSource = hkMacaoTaiwanResidencePermitSource;
    }

    public String getHkMacaoTaiwanIdCardUpdatetime() {
        return hkMacaoTaiwanIdCardUpdatetime;
    }

    public void setHkMacaoTaiwanIdCardUpdatetime(String hkMacaoTaiwanIdCardUpdatetime) {
        this.hkMacaoTaiwanIdCardUpdatetime = hkMacaoTaiwanIdCardUpdatetime;
    }

    public String getHkMacaoTaiwanIdCardSource() {
        return hkMacaoTaiwanIdCardSource;
    }

    public void setHkMacaoTaiwanIdCardSource(String hkMacaoTaiwanIdCardSource) {
        this.hkMacaoTaiwanIdCardSource = hkMacaoTaiwanIdCardSource;
    }

    public String getMainlandToTaiwanTravelPermitUpdatetime() {
        return mainlandToTaiwanTravelPermitUpdatetime;
    }

    public void setMainlandToTaiwanTravelPermitUpdatetime(String mainlandToTaiwanTravelPermitUpdatetime) {
        this.mainlandToTaiwanTravelPermitUpdatetime = mainlandToTaiwanTravelPermitUpdatetime;
    }

    public String getMainlandToTaiwanTravelPermitSource() {
        return mainlandToTaiwanTravelPermitSource;
    }

    public void setMainlandToTaiwanTravelPermitSource(String mainlandToTaiwanTravelPermitSource) {
        this.mainlandToTaiwanTravelPermitSource = mainlandToTaiwanTravelPermitSource;
    }

    public String getHkMacaoTravelPermitUpdatetime() {
        return hkMacaoTravelPermitUpdatetime;
    }

    public void setHkMacaoTravelPermitUpdatetime(String hkMacaoTravelPermitUpdatetime) {
        this.hkMacaoTravelPermitUpdatetime = hkMacaoTravelPermitUpdatetime;
    }

    public String getHkMacaoTravelPermitSource() {
        return hkMacaoTravelPermitSource;
    }

    public void setHkMacaoTravelPermitSource(String hkMacaoTravelPermitSource) {
        this.hkMacaoTravelPermitSource = hkMacaoTravelPermitSource;
    }
}
