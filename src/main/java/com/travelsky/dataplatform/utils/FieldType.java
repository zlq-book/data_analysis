package com.travelsky.dataplatform.utils;

public enum FieldType {
    // 证件类型
    DOCUMENT_TYPE,
    // 证件号
    DOCUMENT_NUM,
    // 国籍
    NATIONALITY,
    // 省份
    PROVINCE,
    // 城市
    CITY,
    // 民族
    ETHNIC_GROUP,
    // 性别
    GENDER,
    // 用户类型
    USER_TYPE,
    // 盲人旅客标识
    BLIND_PASSENGER_FLAG,
    // 失聪旅客标识
    DEAF_PASSENGER_FLAG,
    // 是否直销用户
    IS_DIRECT_SALES_USER,
    // 直销用户状态
    DIRECT_SALES_USER_STATUS,
    // 直销实名认证标识
    DIRECT_SALES_REAL_NAME_AUTH_FLAG,
    // 直销实名认证方式
    DIRECT_SALES_REAL_NAME_AUTH_METHOD,
    // 是否直销黑名单用户
    IS_DIRECT_SALES_BLACKLIST_USER,
    // 学生标识
    STUDENT_FLAG,
    // 教师标识
    TEACHER_FLAG,
    // 代理人标识
    AGENT_FLAG,
    // 官网账号状态
    OFFICIAL_WEBSITE_ACCOUNT_STATUS,
    // 常旅客等级
    FREQUENT_FLYER_LEVEL,
    // 常旅客发展渠道（一级）
    FREQUENT_FLYER_DEVELOPMENT_CHANNEL_LEVEL1,
    // 常旅客发展渠道（二级）
    FREQUENT_FLYER_DEVELOPMENT_CHANNEL_LEVEL2,
    // 常旅客发展渠道（三级）
    FREQUENT_FLYER_DEVELOPMENT_CHANNEL_LEVEL3,
    // 常旅客发展渠道（四级）
    FREQUENT_FLYER_DEVELOPMENT_CHANNEL_LEVEL4,
    // 是否接受会员天地邮件营销
    ACCEPT_MEMBER_TERRITORY_EMAIL_MARKETING,
    // 鲁雁行用户等级
    LUNYANXING_USER_LEVEL,
    // 鲁雁行用户状态
    LUNYANXING_USER_STATUS,
    // 鲁雁行注册状态
    LUNYANXING_REGISTRATION_STATUS,
    // 鲁雁行实名认证状态
    LUNYANXING_REAL_NAME_AUTH_STATUS,
    // 高端旅客标识
    HIGH_END_PASSENGER_FLAG,
    // 高端旅客类型
    HIGH_END_PASSENGER_TYPE,
    // 高端旅客分层类型
    HIGH_END_PASSENGER_TIER_TYPE,
    // 高端旅客数据来源
    HIGH_END_PASSENGER_DATA_SOURCE,
    // VIP标识
    VIP_FLAG,
    // CIP标识
    CIP_FLAG,
    // VVIP标识
    VVIP_FLAG,
    // 身份类型
    IDENTITY_TYPE,
    // 学历
    EDUCATION_LEVEL,
    // 保险状态
    INSURANCE_STATUS,
    // 订单状态
    ORDER_STATUS,
    // 订单类型
    ORDER_TYPE,
    // 客票状态
    TICKET_STATUS,
    // 订单下单渠道
    ORDER_PLACEMENT_CHANNEL,
    // 常客注册渠道
    FREQUENT_FLYER_REGISTRATION_CHANNEL,
    // 活动报名渠道
    ACTIVITY_REGISTRATION_CHANNEL,
    // 累积类型代码(一级分类)/兑换类型代码（一级分类)
    ACCUMULATION_TYPE_CODE_LEVEL1,
    // 累积类型代码（二级分类）/兑换类型代码（二级分类
    ACCUMULATION_TYPE_CODE_LEVEL2,
    // 合作伙伴代码
    PARTNER_CODE,
    // 评价状态
    EVALUATION_STATUS,
    // 选座状态
    SEAT_SELECTION_STATUS,
    // 选座类型
    SEAT_SELECTION_TYPE,
    // 是否自动值机
    IS_AUTO_CHECK_IN,
    // 手机值机类型
    MOBILE_CHECK_IN_TYPE,
    // 是否是付费选座订单
    IS_PAID_SEAT_ORDER,
    // 订单支付方式
    ORDER_PAYMENT_METHOD,
    // 支付币种
    PAYMENT_CURRENCY,
    // 国际国内标识
    INTERNATIONAL_DOMESTIC_FLAG,
    // 国航常旅客身份识别
    AIR_CHINA_FREQUENT_FLYER_ID,
    // 特殊服务类型
    SPECIAL_SERVICE_TYPE,
    // 值机方式
    CHECK_IN_METHOD,
    // 特殊旅客标识
    SPECIAL_PASSENGER_FLAG,
    // 降舱信息
    DOWNGRADE_INFO,
    // GP标识
    GP_FLAG,
    // 是否团队
    IS_GROUP,
    // 升舱信息
    UPGRADE_INFO,
    // 值机渠道
    CHECK_IN_CHANNEL,
    // 支付渠道
    PAYMENT_CHANNEL,
    // 现金支付平台
    CASH_PAYMENT_PLATFORM,
    // 优惠券类型
    COUPON_TYPE,
    // 票号
    TICKET_NO,
    // 手机号
    MOBILE_NO,
    // 常旅客卡号
    FREQUENT_FLYER_NUM,
    // 英文姓名
    EN_NAME,
    // 中文姓名
    CN_NAME,
    // 姓名拼音
    NAME_PINYIN,
    // 机场
    AIRPORT,
    // 退票渠道
    REFUND_CHANNEL,
    // 预订渠道
    ORDER_CHANNEL,
    // 特殊餐食
    SPECIAL_MEAL,
    // 票价类型
    PRICE_TYPE,
    // 舱等
    CABIN,
    // 退票类型
    REFUND_TYPE,
    // 非自愿退票原因
    NON_VOLUNTARY_REFUND_REASON,
    // 升舱状态
    UPGRADE_STATUS,
    // 支付状态
    PAYMENT_STATUS,
    // 升舱退票渠道
    UPGRADE_REFUND_CHANNEL,
    // 评价级别
    EVALUATION_LEVEL,
    // 会员级别
    MEMBER_LEVEL,
    // 航空首兑资格
    FIRST_EXCH_QUALIFI,
    // 是否直销实名认证手机号
    LYGJ_DIRECT_SALES_REAL_NAME_MOBILE,
    // 是否鲁雁行实名认证用户手机号
    LYGJ_LUNYANXING_REAL_NAME_MOBILE,
    // 是否为常客手机号
    LYGJ_FREQUENT_FLYER_MOBILE,
    // 是否高端旅客手机号
    LYGJ_HIGH_END_PASSENGER_MOBILE,
    // 是否虚拟手机号
    LYGJ_VIRTUAL_MOBILE,
    // 是否大客户差旅系统军人手机号
    LYGJ_QIYE_TRAVEL_SYSTEM_JIAREN_MOBILE,
    // 是否企微小山客户手机号
    LYGJ_QIWEI_XIAOSHAN_MOBILE,
    // 鲁雁行是否需要LIP三要素认证
    LYGJ_LUNYANXING_NEED_LIP_THREE_ELEMENTS_AUTH,
    // 餐食喜好 && 临时餐食喜好  && 饮品喜好,这三类都是用同一个
    LYGJ_VIP_FOOD_LOVE_NAME,
    // 升舱预订渠道
    UPGRADE_ORDER_CHANNEL,
    // 保险预订渠道
    INSURANCE_ORDER_CHANNEL,
    // 销售渠道（升舱券）
    UPGRADE_SALES_CHANNEL,
    // 保险类型
    INSURANCE_TYPE,
    // 预订渠道（机票）
    ORDER_CHANNEL_AIRTICKET,
    // 退票渠道（机票）
    REFUND_CHANNEL_AIRTICKET,
    //预订渠道（鲁雁行）
    ORDER_CHANNEL_LUYANXING,
    // 渠道记录的改期类型
    CHANGE_TYPE,
    // 购买渠道（休息室券）
    PURCHASE_CHANNEL_LOUNGE,
    // 退订渠道（休息室券）
    REFUND_CHANNEL_LOUNGE,
}
