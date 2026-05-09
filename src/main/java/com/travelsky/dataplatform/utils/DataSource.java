package com.travelsky.dataplatform.utils;

/**
 * 数据源枚举类，定义了不同的数据来源。
 */
public enum DataSource {

    /**
     * 直销用户
     */
    DIRECT_SALES_USER("直销用户"),
    /**
     * 鲁雁管家
     */
    LUYAN_STEWARD("鲁雁管家"),
    /**
     * 数仓
     */
    DATA_WAREHOUSE("数仓"),
    /**
     * 常旅客
     */
    FREQUENT_FLYER("常旅客"),
    /**
     * 常旅客FTP
     */
    FREQUENT_FLYER_FTP("常旅客FTP"),
    /**
     * 会员天地
     */
    MEMBER_WORLD("会员天地"),
    /**
     * 鲁雁行-免票兑换订单
     */
    LUYAN_TRIP("鲁雁行-免票兑换订单"),
    /**
     * 鲁雁行-免票兑换改期退票
     */
    LUYAN_TRIP_CHANGE("鲁雁行-免票兑换改期退票"),
    /**
     * 掌尚飞
     */
    ZHANGSHANGFEI("掌尚飞"),
    /**
     * 微信小程序
     */
    WECHAT_MINI_PROGRAM("微信小程序"),
    /**
     * 支付宝小程序（订单统一数据库）
     */
    ALIPAY_MINI_PROGRAM_ORDER_SYSTEM("支付宝小程序（订单统一数据库）"),
    /**
     * 常旅客短信
     */
    FREQUENT_FLYER_SMS("常旅客短信"),
    /**
     * 标准数据系统
     */
    STANDARD_DATA_SYSTEM("标准数据系统"),
    /**
     * 预购升舱&机上升舱
     */
    PRE_PURCHASED_UPGRADE("预购升舱&机上升舱"),
    /**
     * 登机口升舱
     */
    GATE_UPGRADE("登机口升舱"),
    /**
     * 大客户管理系统
     */
    KEY_ACCOUNT_MANAGEMENT("大客户管理系统"),
    /**
     * 大客户差旅系统
     */
    KEY_ACCOUNT_TRAVEL("大客户差旅系统"),
    /**
     * 企微私域-小山
     */
    QIW_PRIVATE_DOMAIN("企微私域-小山"),
    /**
     * 国际销售系统
     */
    INTERNATIONAL_SALES_SYSTEM("国际销售系统"),
    /**
     * 高频数据
     */
    HIGH_FREQUENCY_DATA("高频数据"),
    /**
     * 呼叫白屏票务系统
     */
    CALL_CENTER_TICKETING_SYSTEM("呼叫白屏票务系统"),
    /**
     * 航信零售平台
     */
    TRP("航信零售平台"),
    /**
     * 航信零售平台FTP
     */
    TRP_FTP("航信零售平台FTP"),
    /**
     * 航信零售平台SC
     */
    TRP_SC("航信零售平台数仓"),
    /**
     * 国航常旅客接口
     */
    CLK_API("国航常旅客接口");

    private final String description; // 数据源描述

    /**
     * 构造函数，初始化数据源描述。
     *
     * @param description 数据源描述
     */
    DataSource(String description) {
        this.description = description;
    }

    /**
     * 获取数据源描述。
     *
     * @return 数据源描述
     */
    public String getDescription() {
        return description;
    }

    /**
     * 根据枚举值名称获取枚举对象。
     *
     * @param name 枚举值名称
     * @return 对应的 DataSource 枚举对象，如果未找到则返回 null
     */
    public static DataSource fromName(String name) {
        try {
            return DataSource.valueOf(name);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}

