package com.travelsky.dataplatform.constans;

import com.travelsky.dataplatform.utils.GetTableSql;

public class KfbpCreateToSql {
    public static final String BUS_REFUND = "CREATE TABLE T_ODS_KFBP_BUS_REFUND (\n" +
            "    `PKID` VARCHAR(1000) NOT NULL COMMENT '主键',\n" +
            "    `REFUNDTYPE` VARCHAR(155) COMMENT '退票类型',\n" +
            "    `CREATE_BY` VARCHAR(200) COMMENT '创建人ID',\n" +
            "    `CREATE_DATE` TIMESTAMP(3) COMMENT '创建时间',\n" +
            "    `CREATE_NAME` VARCHAR(200) COMMENT '创建人名称',\n" +
            "    `UPDATE_BY` VARCHAR(200) COMMENT '修改人',\n" +
            "    `UPDATE_DATE` TIMESTAMP(3) COMMENT '修改时间',\n" +
            "    `UPDATE_NAME` VARCHAR(200) COMMENT '修改人姓名',\n" +
            "    `AUDIT_MEMO` VARCHAR(2000) COMMENT '审核备注',\n" +
            "    `AUDITING_THROUGH_DATE` TIMESTAMP(3) COMMENT '审核通过时间',\n" +
            "    `CONTACT_EMAIL` VARCHAR(320) COMMENT '联系人邮箱',\n" +
            "    `CONTACT_ID_CODE` VARCHAR(1275) COMMENT '证件号码',\n" +
            "    `CONTACT_MOBILE` VARCHAR(160) COMMENT '联系人手机',\n" +
            "    `CONTACT_NAME` VARCHAR(160) COMMENT '联系人姓名',\n" +
            "    `CONTACT_PHONE` VARCHAR(160) COMMENT '联系人电话',\n" +
            "    `COURIER_NUMBER` VARCHAR(320) COMMENT '快递单号',\n" +
            "    `CUR_TEL` VARCHAR(100) COMMENT '当前来电',\n" +
            "    `DATA_COMPLETE` INT COMMENT '是否资料齐全',\n" +
            "    `MEMO` VARCHAR(2000) COMMENT '备注',\n" +
            "    `NEED_REFUND_AMOUT` DECIMAL(10, 2) COMMENT '应退款',\n" +
            "    `REASON` VARCHAR(1275) COMMENT '退款原因',\n" +
            "    `REFUND_DATA_FLAG` INT COMMENT '是否需要退票审核资料',\n" +
            "    `REFUND_DATA_TYPE` VARCHAR(1275) COMMENT '所需退票资料',\n" +
            "    `REFUND_FEE` DECIMAL(10, 2) COMMENT '退款手续费',\n" +
            "    `REFUND_SN` VARCHAR(1275) COMMENT '退款单号',\n" +
            "    `REFUND_STATUS` INT COMMENT '退款申请单状态',\n" +
            "    `REFUNDEDMENT_DATE` TIMESTAMP(3) COMMENT '完成退款时间',\n" +
            "    `REFUNDMENT_STATUS` INT COMMENT '退款状态',\n" +
            "    `SALE_CHANNEL` INT COMMENT '销售渠道',\n" +
            "    `SUTMIT_USER_INFO_BUS_DEPT_ID` INT COMMENT '用户登陆名称',\n" +
            "    `SUTMIT_USER_INFO_DEPT_ID` INT COMMENT '用户所属当前部门ID',\n" +
            "    `SUTMIT_USER_INFO_LOGIN` VARCHAR(1275) COMMENT '用户登陆名称',\n" +
            "    `SUTMIT_USER_INFO_NAME` VARCHAR(1275) COMMENT '用户名',\n" +
            "    `RET_PRICE_ALL_FEE` DECIMAL(19, 2) COMMENT '总税',\n" +
            "    `RET_PRICE_BUILD_FEE` DECIMAL(19, 2) COMMENT '机场建设费',\n" +
            "    `RET_PRICE_CABIN_PRICE` DECIMAL(19, 2) COMMENT '舱位原价',\n" +
            "    `RET_PRICE_CURRENCY` INT COMMENT '支付币种',\n" +
            "    `RET_PRICE_EXCHANGE_RATE` DECIMAL(10, 4) COMMENT '汇率',\n" +
            "    `RET_PRICE_FUEL_FEE` DECIMAL(19, 2) COMMENT '机场燃油费',\n" +
            "    `RET_PRICE_OB_FEE` DECIMAL(19, 2) COMMENT '换开变更费',\n" +
            "    `RET_PRICE_TICKET_PRICE` DECIMAL(19, 2) COMMENT '机票价',\n" +
            "    `RET_PRICE_TOTAL_PRICE` DECIMAL(19, 2) COMMENT '总价',\n" +
            "    `AUDIT_USER_USER_ID` INT COMMENT '审核人',\n" +
            "    `ORDER_PKID` VARCHAR(1275) COMMENT '订单ID',\n" +
            "    `SUBMIT_USER_USER_ID` INT COMMENT '提交人',\n" +
            "    `TK_ORDER_PKID` VARCHAR(1275) COMMENT '机票订单ID',\n" +
            "    `DATA_PRINT` INT COMMENT '是否资料打印',\n" +
            "    `MAIL_MATERIAL` VARCHAR(1275) COMMENT '邮寄资料',\n" +
            "    `SHIP_ADDRESS` VARCHAR(320) COMMENT '收货地址',\n" +
            "    `SHIP_MEMO` VARCHAR(2000) COMMENT '备注',\n" +
            "    `SHIP_MOBILE` VARCHAR(80) COMMENT '收货手机',\n" +
            "    `SHIP_NAME` VARCHAR(200) COMMENT '收货人姓名',\n" +
            "    `SHIP_PHONE` VARCHAR(80) COMMENT '收货电话',\n" +
            "    `SHIP_SEND_DATE` DATE COMMENT '配送日期',\n" +
            "    `SHIP_SEND_TIME` VARCHAR(80) COMMENT '送票时间',\n" +
            "    `SHIP_SHIP_MAIL_TYPE` INT COMMENT '邮寄方式',\n" +
            "    `SHIP_SHIP_STATUS` INT COMMENT '配送状态',\n" +
            "    `SHIP_SHIP_TYPE` INT COMMENT '配送方式',\n" +
            "    `SHIP_ZIP_CODE` VARCHAR(40) COMMENT '收货邮编',\n" +
            "    `SHIP_DEPT_GROUP_ID` INT COMMENT '配送部门id',\n" +
            "    `REFUND_TRFD_STATUS` INT COMMENT 'TRFD状态',\n" +
            "    `INSURE_ORDER_PKID` VARCHAR(1275) COMMENT '保险订单ID',\n" +
            "    `SHIP_IS_URGENCY`  tinyint COMMENT '是否紧急邮寄派送',\n" +
            "    `TK_REFUND_FARE_TICKET_FLT_PKID` VARCHAR(1275) COMMENT '退票航班ID',\n" +
            "    `REFUND_MAIL_INVOICE_TITLE` VARCHAR(150) COMMENT '发票抬头',\n" +
            "    `REFUND_MAIL_REFUND` VARCHAR(100) COMMENT '退票费',\n" +
            "    `REFUND_MAIL_REFUNDABLE` VARCHAR(100) COMMENT '应退金额',\n" +
            "    `REFUND_MAIL_ISSUE_TICKET` VARCHAR(50) COMMENT '出票方',\n" +
            "    `REFUND_MAIL_DRAWER` VARCHAR(50) COMMENT '出款方',\n" +
            "    `REFUND_MAIL_AGGREGATE_AMOUNT` VARCHAR(100) COMMENT '支付总金额',\n" +
            "    `REFUND_MEMO` VARCHAR(2000) COMMENT '退票邮寄备注',\n" +
            "    `OTHER_FLAG`  tinyint COMMENT '是否退农行/跨行储蓄卡',\n" +
            "    `VOLUNTARILY_FLAG`  tinyint COMMENT '自愿/非自愿: 1自愿2非自愿',\n" +
            "    `REFUND_MAIL_TAXPAYER_NO` VARCHAR(100) COMMENT '纳税人识别号',\n" +
            "    `REFUND_MAIL_INVOICE_TYPE` INT COMMENT '发票类型',\n" +
            "    `SHIP_EMS_MAIL_TYPE` INT COMMENT 'EMS项目',\n" +
            "    `SHIP_EMSREMARK` VARCHAR(2000) COMMENT 'EMS备注',\n" +
            "    `REFUND_MAIL_BUILD_FEE` VARCHAR(100) COMMENT '民航发展基金',\n" +
            "    `REFUND_MAIL_FUEL_FEE` VARCHAR(100) COMMENT '燃油附加费',\n" +
            "    `REFUND_MAIL_INSURE_FEE` VARCHAR(100) COMMENT '保险费用',\n" +
            "    `OPER_FLAG`  tinyint COMMENT '操作标记0暂不处理1正常处理',\n" +
            "    `OFFLINE_TICKET_REFUND`  tinyint COMMENT '退票渠道：0白屏退票1线下退票2中台退票',\n" +
            "    `REFUND_MARK_STATUS`  tinyint COMMENT '退票状态0未退票1已退票',\n" +
            "    `REFUND_AGENCY_FEE` DECIMAL(10, 2) COMMENT '代理费手续费',\n" +
            "    `REFUND_CATEGORY` INT COMMENT '退款类型',\n" +
            "    `TICKETNO_OR_ORDERNO` VARCHAR(500) COMMENT '关联票号/订单号',\n" +
            "    `OCRP_ID` VARCHAR(500) COMMENT '中台的退单号',\n" +
            "    `BUSINESS_TYPE` VARCHAR(50) COMMENT '业务类型2小时错购D2/I2退票券R非连续非自愿AB',\n" +
            "    `DI_IND` VARCHAR(50) COMMENT '类型标识国内D国际I',\n" +
            "    `REFUND_TICKET_NUMBER` VARCHAR(2000) COMMENT '中台的退票票号',\n" +
            "    `SUPPLY_FLAG`  tinyint COMMENT '是否已补充退款信息0否1是',\n" +
            "    `CONTACT_BIRTH` DATE COMMENT '联系人出生日期',\n" +
            "    `CONNECT_NUMBER` VARCHAR(2000) COMMENT '中台的联票票号',\n" +
            "    `REFUND_PNR` VARCHAR(500) COMMENT '中台的PNR',\n" +
            "    `EXCHANGED_IND` VARCHAR(50) COMMENT '所退机票是否为换开后机票Y是N否',\n" +
            "    `IRR_TICKET_NUMBER` VARCHAR(2000) COMMENT '航变机票号',\n" +
            "    `IRR_AFFECTED_TICKET_NUMBER` VARCHAR(2000) COMMENT '航变关联机票号',\n" +
            "  `ETL_CREATE_TIME` TIMESTAMP(6) NULL  COMMENT '数据入仓时间',\n" +
            "  `ETL_UPDATE_TIME` TIMESTAMP(6)  NULL  COMMENT '数据在数仓更新时间时间',\n" +
            "  `ETL_DATE` date  NULL COMMENT '数据ETL日期'" +
            ") WITH (\n" +

            GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_KFBP_BUS_REFUND", Constants.ODS_USER, Constants.ODS_PWD)

            + ")";
    public static final String BUS_ORDER = "CREATE TABLE T_ODS_KFBP_BUS_ORDER (\n" +
            "  `PKID` STRING COMMENT '主键',\n" +
            "  `ORDER_TYPE` STRING COMMENT '订单类型',\n" +
            "  `CREATE_BY` STRING COMMENT '创建人ID',\n" +
            "  `CREATE_DATE` TIMESTAMP(3) COMMENT '创建时间',\n" +
            "  `CREATE_NAME` STRING COMMENT '创建人名称',\n" +
            "  `UPDATE_BY` STRING COMMENT '修改人',\n" +
            "  `UPDATE_DATE` TIMESTAMP(3) COMMENT '修改时间',\n" +
            "  `UPDATE_NAME` STRING COMMENT '修改人姓名',\n" +
            "  `BIG_CUS_INFO_BIG_CUS_TYPE` INT COMMENT '大客户类型',\n" +
            "  `BIG_CUS_INFO_CARD_NO` STRING COMMENT '大客户协议号',\n" +
            "  `BIG_CUS_INFO_DEP_NAME` STRING COMMENT '所属部门',\n" +
            "  `BIG_CUS_INFO_FIRM_NAME` STRING COMMENT '大客户公司名',\n" +
            "  `BIG_CUS_INFO_ID_NUMBER` STRING COMMENT '大客户主键',\n" +
            "  `BIG_CUS_INFO_IS_BIG_CUS` tinyINT COMMENT '是否大客户订单',\n" +
            "  `BIG_CUS_INFO_MANANGER_NAME` STRING COMMENT '大客户经理名称',\n" +
            "  `BIG_CUS_INFO_MEET_TITLE` STRING COMMENT '会议主题',\n" +
            "  `BIG_CUS_INFO_PARBU` STRING COMMENT '大客户结算代码',\n" +
            "  `CASH_CASH_STATUS` INT COMMENT '支付状态',\n" +
            "  `CASH_CURRENCY` INT COMMENT '支付时的币种',\n" +
            "  `CASH_DELIVERY_FEE` DECIMAL(22, 2) COMMENT '配送费用(人民币)',\n" +
            "  `CASH_DELIVERY_FEE_CUR` DECIMAL(22, 2) COMMENT '配送费用(支付时币种)',\n" +
            "  `CASH_FPF_CARD_NO` STRING COMMENT '常客卡号',\n" +
            "  `CASH_FPF_NAME` STRING COMMENT '常客卡用户名',\n" +
            "  `CASH_FPF_TAX_SCORE` INT COMMENT '所需积分(税费)',\n" +
            "  `CASH_FPF_TICKET_DISCOUNT` DECIMAL(22, 2) COMMENT '折扣',\n" +
            "  `CASH_FPF_TICKET_SCORE` INT COMMENT '所需积分(票面)',\n" +
            "  `CASH_IS_FPF` tinyINT COMMENT '是否积分支付',\n" +
            "  `CASH_IS_FPF_MIX` tinyINT COMMENT '积分支付是否混合支付',\n" +
            "  `CASH_PAID_AMOUNT` DECIMAL(22, 2) COMMENT '已付金额(人民币)',\n" +
            "  `CASH_PAID_AMOUNT_CUR` DECIMAL(22, 2) COMMENT '已付金额(支付时币种)',\n" +
            "  `CASH_PAY_ADDRESS` INT COMMENT '支付时的地址',\n" +
            "  `CASH_TOTAL_ORDER_AMOUNT` DECIMAL(22, 2) COMMENT '订单总额(人民币)',\n" +
            "  `CASH_TOTAL_ORDER_AMOUNT_CUR` DECIMAL(22, 2) COMMENT '订单总额(支付时币种)',\n" +
            "  `CASH_TOTAL_PRODUCT_PRICE` DECIMAL(22, 2) COMMENT '总商品价格(人民币)',\n" +
            "  `CASH_TOTAL_PRODUCT_PRICE_CUR` DECIMAL(22, 2) COMMENT '总商品价格(支付时币种)',\n" +
            "  `CHINS_ID` STRING COMMENT '话务系统取到ID',\n" +
            "  `CONTACT_EMAIL` STRING COMMENT '联系人邮箱',\n" +
            "  `CONTACT_ID_CODE` STRING COMMENT '证件号码',\n" +
            "  `CONTACT_MOBILE` STRING COMMENT '联系人手机',\n" +
            "  `CONTACT_NAME` STRING COMMENT '联系人姓名',\n" +
            "  `CONTACT_PHONE` STRING COMMENT '联系人电话',\n" +
            "  `CUR_TEL` STRING COMMENT '当前来电号码',\n" +
            "  `GOV_INFO_BIN` STRING COMMENT '政府卡BIN',\n" +
            "  `GOV_INFO_BUGDET_NAME` STRING COMMENT '预算单位名称',\n" +
            "  `GOV_INFO_IS_GOV` tinyINT COMMENT '是否政府采购',\n" +
            "  `IS_URGENCY` tinyINT COMMENT '是否紧急订单',\n" +
            "  `MEMO` STRING COMMENT '备注',\n" +
            "  `ORDER_SN` STRING COMMENT '订单号',\n" +
            "  `ORDER_SOURCE` INT COMMENT '订单来源',\n" +
            "  `ORDER_STATUS` INT COMMENT '订单状态',\n" +
            "  `SHIP_ADDRESS` STRING COMMENT '收货地址',\n" +
            "  `SHIP_MEMO` STRING COMMENT '备注',\n" +
            "  `SHIP_MOBILE` STRING COMMENT '收货手机',\n" +
            "  `SHIP_NAME` STRING COMMENT '收货人姓名',\n" +
            "  `SHIP_PHONE` STRING COMMENT '收货电话',\n" +
            "  `SHIP_SEND_DATE` DATE COMMENT '配送日期',\n" +
            "  `SHIP_SEND_TIME` STRING COMMENT '送票时间',\n" +
            "  `SHIP_SHIP_MAIL_TYPE` INT COMMENT '邮寄方式',\n" +
            "  `SHIP_SHIP_STATUS` INT COMMENT '配送状态',\n" +
            "  `SHIP_SHIP_TYPE` INT COMMENT '配送方式',\n" +
            "  `SHIP_ZIP_CODE` STRING COMMENT '收货邮编',\n" +
            "  `SUTMIT_USER_INFO_BUS_DEPT_ID` INT COMMENT '用户登陆名称',\n" +
            "  `SUTMIT_USER_INFO_DEPT_ID` INT COMMENT '用户所属当前部门ID',\n" +
            "  `SUTMIT_USER_INFO_LOGIN` STRING COMMENT '用户登陆名称',\n" +
            "  `SUTMIT_USER_INFO_NAME` STRING COMMENT '用户名',\n" +
            "  `BUS_ORDER_STATUS` INT COMMENT '便捷巴士单状态',\n" +
            "  `BUS_ORDER_TYPE` INT COMMENT '便捷巴士单类型',\n" +
            "  `CLEAN_QUEUE_CHANGE_TYPE` INT COMMENT '清Q变动类型',\n" +
            "  `IS_ADD_INF` tinyINT COMMENT '是否补开婴儿',\n" +
            "  `OPEN_TICKET_LIMIT_DATE` TIMESTAMP(3) COMMENT '出票时限',\n" +
            "  `OT_USER_INFO_BUS_DEPT_ID` INT COMMENT '用户登陆名称',\n" +
            "  `OT_USER_INFO_DEPT_ID` INT COMMENT '用户所属当前部门ID',\n" +
            "  `OT_USER_INFO_LOGIN` STRING COMMENT '用户登陆名称',\n" +
            "  `OT_USER_INFO_NAME` STRING COMMENT '用户名',\n" +
            "  `OTHER_PRODUCT_TYPE` INT COMMENT '其它服务',\n" +
            "  `PNR` STRING COMMENT 'PNR号',\n" +
            "  `PRICE_ALL_FEE` DECIMAL(22, 2) COMMENT '总税',\n" +
            "  `PRICE_BUILD_FEE` DECIMAL(22, 2) COMMENT '机场建设费',\n" +
            "  `PRICE_CABIN_PRICE` DECIMAL(22, 2) COMMENT '舱位原价',\n" +
            "  `PRICE_CURRENCY` INT COMMENT '支付币种',\n" +
            "  `PRICE_EXCHANGE_RATE` DECIMAL(14, 4) COMMENT '汇率',\n" +
            "  `PRICE_FUEL_FEE` DECIMAL(22, 2) COMMENT '机场燃油费',\n" +
            "  `PRICE_OB_FEE` DECIMAL(22, 2) COMMENT '换开变更费',\n" +
            "  `PRICE_TICKET_PRICE` DECIMAL(22, 2) COMMENT '机票价',\n" +
            "  `PRICE_TOTAL_PRICE` DECIMAL(22, 2) COMMENT '总价',\n" +
            "  `TICKET_TYPE` INT COMMENT '机票类型',\n" +
            "  `TK_ORDER_STATUS` INT COMMENT '机票订单状态',\n" +
            "  `TK_ORDER_TYPE` INT COMMENT '机票订单类型',\n" +
            "  `ALL_RISKS_INSURE_FEE` DECIMAL(22, 2) COMMENT '保费(单价)',\n" +
            "  `ALL_RISKS_INSURE_INSURE_COUNT` DECIMAL(22, 2) COMMENT '保险份数',\n" +
            "  `ALL_RISKS_INSURE_TOTAL_FEE` DECIMAL(22, 2) COMMENT '总保费',\n" +
            "  `DELAY_INSURE_FEE` DECIMAL(22, 2) COMMENT '保费(单价)',\n" +
            "  `DELAY_INSURE_INSURE_COUNT` DECIMAL(22, 2) COMMENT '保险份数',\n" +
            "  `DELAY_INSURE_TOTAL_FEE` DECIMAL(22, 2) COMMENT '总保费',\n" +
            "  `EXPECT_INSURE_FEE` DECIMAL(22, 2) COMMENT '保费(单价)',\n" +
            "  `EXPECT_INSURE_INSURE_COUNT` DECIMAL(22, 2) COMMENT '保险份数',\n" +
            "  `EXPECT_INSURE_TOTAL_FEE` DECIMAL(22, 2) COMMENT '总保费',\n" +
            "  `FLT_COUNT` DECIMAL(22, 2) COMMENT '航段数',\n" +
            "  `INSURE_DAYS` INT COMMENT '天数(境外险使用)',\n" +
            "  `INSURE_SOURCE` INT COMMENT '保险来源',\n" +
            "  `INSURE_STATUS` INT COMMENT '航联保险状态',\n" +
            "  `OVERSEA_FEE` DECIMAL(22, 2) COMMENT '境外险保费',\n" +
            "  `POLICY_NO` STRING COMMENT '保险号，退保需要使用',\n" +
            "  `TOTAL_FEE` DECIMAL(22, 2) COMMENT '总保费',\n" +
            "  `CUR_INCOME_NAME` STRING COMMENT '来电姓名',\n" +
            "  `POST_NO` STRING COMMENT '快递单号',\n" +
            "  `CHANGE_ORDER_SN` STRING COMMENT '变更单号',\n" +
            "  `CHANGE_ORDER_STATUS` INT COMMENT '变更单状态',\n" +
            "  `IS_FEE` tinyINT COMMENT '是否免费变更',\n" +
            "  `PNR_CHANGE_AFTER` STRING COMMENT '变更后的pnr',\n" +
            "  `CASH_DEPT_GROUP_ID` INT COMMENT '支付部门ID',\n" +
            "  `GAIN_USER_USER_ID` INT COMMENT '领用人',\n" +
            "  `SHIP_DEPT_GROUP_ID` INT COMMENT '配送部门ID',\n" +
            "  `SUBMIT_USER_USER_ID` INT COMMENT '提交人',\n" +
            "  `UNION_ORDER_PKID` STRING COMMENT '统一收款单ID',\n" +
            "  `OT_USER_USER_ID` INT COMMENT '预定人',\n" +
            "  `FARE_PKID` STRING COMMENT '旅客ID',\n" +
            "  `ORDER_PKID` STRING COMMENT '付费EMD订单属于的机票订单ID',\n" +
            "  `SEAT_NO` STRING COMMENT '选座座位号',\n" +
            "  `FLIGHT_PKID` STRING COMMENT '订单ID',\n" +
            "  `SEAT_ORDER_STATUS` INT COMMENT '选座订单状态',\n" +
            "  `RECEIPT_TYPE` TINYINT COMMENT '收款类型',\n" +
            "  `CASH_PAID_TIME` TIMESTAMP COMMENT '最新支付时间',\n" +
            "  `EMD_NO` STRING COMMENT 'emd票号',\n" +
            "  `SEAT_ORDER_REFUND_PKID` STRING COMMENT '退座单ID',\n" +
            "  `IS_PNR_BY_WHITE` tinyINT COMMENT '是否白屏产生PNR',\n" +
            "  `IS_FICTITIOUS_ORDER` tinyINT COMMENT '是否虚拟订单',\n" +
            "  `SHIP_IS_URGENCY` tinyINT COMMENT '是否紧急邮寄派送',\n" +
            "  `OVERSEA_INSURE_STATUS` INT COMMENT '境外险状态',\n" +
            "  `EMD_TYPE` INT COMMENT 'emd类型(1选座2行李)',\n" +
            "  `PRODUCT_PKID` STRING COMMENT '产品ID',\n" +
            "  `CASH_FPF_TAX_PRICE` DECIMAL(22, 2) COMMENT '税费总额',\n" +
            "  `IS_ONLY_ORDER` tinyINT COMMENT '是否单独购保单，选座单等',\n" +
            "  `CASH_SCORE_DISCOUNT` DECIMAL(22, 2) COMMENT '积分折扣',\n" +
            "  `CHANGE_FK_TK_ORDER_PKID` STRING COMMENT '变更单属于的机票订单ID',\n" +
            "  `REISSUE_TYPE` INT COMMENT '改期类型',\n" +
            "  `REFUND_MAIL_INVOICE_TITLE` STRING COMMENT '发票抬头',\n" +
            "  `REFUND_MAIL_REFUND` STRING COMMENT '退票费',\n" +
            "  `REFUND_MAIL_REFUNDABLE` STRING COMMENT '应退金额',\n" +
            "  `REFUND_MAIL_ISSUE_TICKET` STRING COMMENT '出票方',\n" +
            "  `REFUND_MAIL_DRAWER` STRING COMMENT '出款方',\n" +
            "  `REFUND_MAIL_AGGREGATE_AMOUNT` STRING COMMENT '支付总金额',\n" +
            "  `PRINT_TKT_ORIGIN` INT COMMENT '行程单来源',\n" +
            "  `LUAAGE_ORDER_STATUS` INT COMMENT '行李订单状态',\n" +
            "  `EMD_SEAT_ORDER_REFUND_PKID` STRING COMMENT '退选座单ID',\n" +
            "  `EMD_LUAAGE_ORDER_REFUND_PKID` STRING COMMENT '退行李单ID',\n" +
            "  `ESS_ORDER_PKID` STRING COMMENT '选座单关联机票ID',\n" +
            "  `TEAM_NUM` STRING COMMENT '团队人数',\n" +
            "  `CASH_CURRENCY_CODE` STRING COMMENT '货币代码',\n" +
            "  `CASH_EXCHANGE_RATE` DECIMAL(23, 4) COMMENT '以人民币汇率支付货币为1 人民币为1*exchangeRate',\n" +
            "  `PRINT_TKT_PENDING_DATE` TIMESTAMP COMMENT '状态变为待处理的时间',\n" +
            "  `PRINT_TKT_COMPLETED_DATE` TIMESTAMP COMMENT '状态变为已完成的时间',\n" +
            "  `EMD_FLIGHT_NO` STRING COMMENT 'EMD对应的航班号',\n" +
            "  `EMD_FARE_NAME` STRING COMMENT 'EMD对应的旅客姓名',\n" +
            "  `REFUND_MAIL_TAXPAYER_NO` STRING COMMENT '纳税人识别号',\n" +
            "  `CASH_FPF_SEASON` STRING COMMENT '季节',\n" +
            "  `REFUND_MAIL_INVOICE_TYPE` INT COMMENT '发票类型',\n" +
            "  `IS_TKEN` STRING COMMENT '是否处理Tkne',\n" +
            "  `PRINT_TKT_COMPLETED_NAME` STRING COMMENT '状态变为已完成的账号',\n" +
            "  `PRINT_MAIL_NAME` STRING COMMENT '打印邮寄快递账号',\n" +
            "  `PRINT_MAIL_DATE` TIMESTAMP COMMENT '打印邮寄快递时间',\n" +
            "  `CASH_CREDIT_SCORE` INT COMMENT '透支积分',\n" +
            "  `SHIP_EMS_MAIL_TYPE` INT COMMENT 'EMS项目',\n" +
            "  `SHIP_EMSREMARK` STRING COMMENT 'EMS备注',\n" +
            "  `APPLYFOR_MEMO` STRING COMMENT '产品备注',\n" +
            "  `APPLYFOR_PHONE` STRING COMMENT '产品联系人',\n" +
            "  `APPLYFOR_STATUS` STRING COMMENT '产品状态',\n" +
            "  `OFFICE` STRING COMMENT '产品状态',\n" +
            "  `CASH_SPECIAL_AMOUNT` DECIMAL(22, 2) COMMENT '附加其它价格',\n" +
            "  `IS_MANUAL` tinyINT COMMENT '是否手工补价',\n" +
            "  `IS_TURNDOWN` tinyINT COMMENT '是否拒绝',\n" +
            "  `REFUND_MAIL_BUILD_FEE` STRING COMMENT '民航发展基金',\n" +
            "  `REFUND_MAIL_FUEL_FEE` STRING COMMENT '燃油附加费',\n" +
            "  `IS_MANUAL_FPF` tinyINT COMMENT '是否线下常客扣减',\n" +
            "  `REFUND_MAIL_INSURE_FEE` STRING COMMENT '保险费用',\n" +
            "  `INSURE_SUC_DATE` TIMESTAMP COMMENT '购保成功时间',\n" +
            "  `INSURE_REFUNDED_DATE` TIMESTAMP COMMENT '完成退保时间',\n" +
            "  `IS_HAS_INSURE_FAIL` tinyINT COMMENT '是否包含购保失败的旅客保单',\n" +
            "  `INSURE_ALLRISKS_TYPE` INT COMMENT '综合险类型',\n" +
            "  `REISSUE_INFO2` STRING COMMENT '改期相关信息',\n" +
            "  `REISSUE_INFO` STRING COMMENT '改期相关信息1',\n" +
            "  `SUMMARY_SHORT_CODE` STRING COMMENT '短信概要内容h5Url code',\n" +
            "  `SUMMARY_URL` STRING COMMENT '概要短信内容H5url',\n" +
            "  `PRINT_REPEAT` STRING COMMENT '打印单是否二次登记',\n" +
            "  `CONTACE_BIRTH` DATE COMMENT '联系人出生日期',\n" +
            "  `CONTACT_BIRTH` DATE COMMENT '联系人出生日期',\n" +
            "  `ETL_CREATE_TIME` TIMESTAMP(6) NULL  COMMENT '数据入仓时间',\n" +
            "  `ETL_UPDATE_TIME` TIMESTAMP(6)  NULL  COMMENT '数据在数仓更新时间时间',\n" +
            "  `ETL_DATE` date  NULL COMMENT '数据ETL日期'" +
            ") WITH (\n" +

            GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_KFBP_BUS_ORDER", Constants.ODS_USER, Constants.ODS_PWD)

            + ")";
    public static final String BUS_FARE_TICKET_FLT = "CREATE TABLE T_ODS_KFBP_BUS_FARE_TICKET_FLT (\n" +
            "`PKID` varchar(1000) ,\n" +
            "`CREATE_BY` varchar(200) ,\n" +
            "`CREATE_DATE` TIMESTAMP(6) ,\n" +
            "`CREATE_NAME` varchar(200) ,\n" +
            "`UPDATE_BY` varchar(200) ,\n" +
            "`UPDATE_DATE` TIMESTAMP(6) ,\n" +
            "`UPDATE_NAME` varchar(200) ,\n" +
            "`COUPON_NUMER` int ,\n" +
            "`FLT_ACTION_CODE` varchar(30) ,\n" +
            "`FLT_AIRPLANE_TYPE` varchar(40) ,\n" +
            "`FLT_ARRIVAL_TERMINAL` varchar(20) ,\n" +
            "`FLT_ARRIVE_CITY` varchar(40) ,\n" +
            "`FLT_ARRIVE_DAY` TIMESTAMP(6) ,\n" +
            "`FLT_ARRIVE_TIME` varchar(20) ,\n" +
            "`FLT_CABIN` varchar(20) ,\n" +
            "`FLT_DEPARTURE_DAY` TIMESTAMP(6) ,\n" +
            "`FLT_DEPARTURE_TERMINAL` varchar(20) ,\n" +
            "`FLT_DEPARTURE_TIME` varchar(20) ,\n" +
            "`FLT_FLIGHT_NO` varchar(40) ,\n" +
            "`FLT_IS_ARNK` tinyint ,\n" +
            "`FLT_IS_SHARE_FLIGHT` tinyint ,\n" +
            "`FLT_REAL_FLIGHT_NO` varchar(40) ,\n" +
            "`FLT_SEGMENT_STATUS` int ,\n" +
            "`FLT_START_CITY` varchar(40) ,\n" +
            "`PRICE_ALL_FEE` decimal(19,2) ,\n" +
            "`PRICE_BUILD_FEE` decimal(19,2) ,\n" +
            "`PRICE_CABIN_PRICE` decimal(19,2) ,\n" +
            "`PRICE_CURRENCY` int ,\n" +
            "`PRICE_EXCHANGE_RATE` decimal(10,4) ,\n" +
            "`PRICE_FUEL_FEE` decimal(19,2) ,\n" +
            "`PRICE_OB_FEE` decimal(19,2) ,\n" +
            "`PRICE_TICKET_PRICE` decimal(19,2) ,\n" +
            "`PRICE_TOTAL_PRICE` decimal(19,2) ,\n" +
            "`TRAVELLED_DAY` TIMESTAMP(6) ,\n" +
            "`TRAVELLED_STATUE` int ,\n" +
            "`FARE_PKID` varchar(1275) ,\n" +
            "`FARE_TICKET_PKID` varchar(1275) ,\n" +
            "`ORDER_PKID` varchar(1275) ,\n" +
            "`FLT_INDEX_IN_PNR` varchar(15) ,\n" +
            "`FLIGHT_PKID` varchar(1275) ,\n" +
            "`IS_PRICE_IGNORE` tinyint ,\n" +
            "`INSURE_EXPECT_POLICY_NO` varchar(1275) ,\n" +
            "`INSURE_EXPECT_SERIAL_NO` varchar(1275) ,\n" +
            "`INSURE_EXPECT_STATUS` int ,\n" +
            "`INSURE_DELAY_SERIAL_NO` varchar(1275) ,\n" +
            "`INSURE_DELAY_POLICY_NO` varchar(1275) ,\n" +
            "`INSURE_DELAY_STATUS` int ,\n" +
            "`INSURE_ALL_RISKS_SERIAL_NO` varchar(1275) ,\n" +
            "`INSURE_ALL_RISKS_POLICY_NO` varchar(1275) ,\n" +
            "`INSURE_ALL_RISKS_STATUS` int ,\n" +
            "`TICKET_NO` varchar(80) ,\n" +
            "`ETL_CREATE_TIME` TIMESTAMP(6) ,\n" +
            "`ETL_UPDATE_TIME` TIMESTAMP(6) ,\n" +
            "`ETL_DATE` date  \n" +
            ") WITH (\n" +

            GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_KFBP_BUS_FARE_TICKET_FLT", Constants.ODS_USER, Constants.ODS_PWD)

            + ")";

    public static final String BUS_TK_REFUND_FARE_TICKET_FLT = "CREATE TABLE T_ODS_KFBP_BUS_FARE_TICKET_FLT (\n" +
            "  `PKID` VARCHAR(1000) NOT NULL COMMENT '主键',\n" +
            "  `CREATE_BY` VARCHAR(200) COMMENT '创建人ID',\n" +
            "  `CREATE_DATE` TIMESTAMP(3) COMMENT '创建时间',\n" +
            "  `CREATE_NAME` VARCHAR(200) COMMENT '创建人名称',\n" +
            "  `UPDATE_BY` VARCHAR(200) COMMENT '修改人',\n" +
            "  `UPDATE_DATE` TIMESTAMP(3) COMMENT '修改时间',\n" +
            "  `UPDATE_NAME` VARCHAR(200) COMMENT '修改人姓名',\n" +
            "  `ARRIVE_CITY` VARCHAR(40) COMMENT '到达城市',\n" +
            "  `CABIN` VARCHAR(20) COMMENT '舱位',\n" +
            "  `DEPARTURE_DAY` DATE COMMENT '出发日期',\n" +
            "  `FARE_NAME` VARCHAR(1275) COMMENT '旅客姓名',\n" +
            "  `FLIGHT_NO` VARCHAR(40) COMMENT '航班号(市场方)',\n" +
            "  `IS_ALL_SGM`  tinyint COMMENT '是否选定了全部航段',\n" +
            "  `OPEN_TICKET_DATE` TIMESTAMP(3) COMMENT '出票日期',\n" +
            "  `RATE` DECIMAL(19, 2) COMMENT '手续费率',\n" +
            "  `RET_PRICE_ALL_FEE` DECIMAL(19, 2) COMMENT '总税',\n" +
            "  `RET_PRICE_BUILD_FEE` DECIMAL(19, 2) COMMENT '机场建设费',\n" +
            "  `RET_PRICE_CABIN_PRICE` DECIMAL(19, 2) COMMENT '舱位原价',\n" +
            "  `RET_PRICE_CURRENCY` INT COMMENT '支付币种',\n" +
            "  `RET_PRICE_EXCHANGE_RATE` DECIMAL(10, 4) COMMENT '汇率',\n" +
            "  `RET_PRICE_FUEL_FEE` DECIMAL(19, 2) COMMENT '机场燃油费',\n" +
            "  `RET_PRICE_OB_FEE` DECIMAL(19, 2) COMMENT '换开变更费',\n" +
            "  `RET_PRICE_TICKET_PRICE` DECIMAL(19, 2) COMMENT '机票价',\n" +
            "  `RET_PRICE_TOTAL_PRICE` DECIMAL(19, 2) COMMENT '总价',\n" +
            "  `SEGMENT_IDX` VARCHAR(1275) COMMENT '操作航段在detr中的序号',\n" +
            "  `SEGMENT_STATUS` INT COMMENT '客票航段状态',\n" +
            "  `START_CITY` VARCHAR(40) COMMENT '出发城市',\n" +
            "  `TICKET_NO` VARCHAR(80) COMMENT '票号',\n" +
            "  `FARE_TICKET_PKID` VARCHAR(1275) COMMENT '票号ID',\n" +
            "  `FLIGHT_PKID` VARCHAR(1275) COMMENT '航班ID',\n" +
            "  `TK_REFUND_PKID` VARCHAR(1275) COMMENT '退票ID',\n" +
            "  `REC_TOTAL_FEE` DECIMAL(19, 2) COMMENT '退票费',\n" +
            "  `REFUND_TOTAL_FEE` DECIMAL(19, 2) COMMENT '应退款',\n" +
            "  `REMARK` VARCHAR(1000) COMMENT '备注',\n" +
            "  `TRFD_SN` VARCHAR(150) COMMENT 'trfd退票单号',\n" +
            "  `OTHER_TAX_DETAIL` VARCHAR(2000) COMMENT '其他税费明细',\n" +
            "  `INSURE_ORDER_REFUND_PKID` VARCHAR(1275) COMMENT '保险订单退款ID',\n" +
            "  `PAY_CONFIRM_STATUS` INT COMMENT '开账状态',\n" +
            "  `PAY_TRADE_NO` VARCHAR(80) COMMENT '开账流水号',\n" +
            "  `BILL_TIME` TIMESTAMP(3) COMMENT '开账日期',\n" +
            "  `HIS_TICKET_NO` VARCHAR(2000) COMMENT '历史票号',\n" +
            "  `WRONG_MAC_FEE` DECIMAL(19, 2) COMMENT '误机费',\n" +
            "  `RET_TOTAL_PRICE_ALL_FEE` DECIMAL(19, 2) COMMENT '总税',\n" +
            "  `RET_TOTAL_PRICE_BUILD_FEE` DECIMAL(19, 2) COMMENT '机场建设费',\n" +
            "  `RET_TOTAL_PRICE_CABIN_PRICE` DECIMAL(19, 2) COMMENT '舱位原价',\n" +
            "  `RET_TOTAL_PRICE_CURRENCY` INT COMMENT '支付币种',\n" +
            "  `RET_TOTAL_PRICE_FUEL_FEE` DECIMAL(19, 2) COMMENT '机场燃油费',\n" +
            "  `RET_TOTAL_PRICE_OB_FEE` DECIMAL(19, 2) COMMENT '换开变更费',\n" +
            "  `RET_TOTAL_PRICE_TICKET_PRICE` DECIMAL(19, 2) COMMENT '机票价',\n" +
            "  `RET_TOTAL_PRICE_TOTAL_PRICE` DECIMAL(19, 2) COMMENT '总价',\n" +
            "  `RET_USE_PRICE_ALL_FEE` DECIMAL(19, 2) COMMENT '总税',\n" +
            "  `RET_USE_PRICE_BUILD_FEE` DECIMAL(19, 2) COMMENT '机场建设费',\n" +
            "  `RET_USE_PRICE_CABIN_PRICE` DECIMAL(19, 2) COMMENT '舱位原价',\n" +
            "  `RET_USE_PRICE_CURRENCY` INT COMMENT '支付币种',\n" +
            "  `RET_USE_PRICE_FUEL_FEE` DECIMAL(19, 2) COMMENT '机场燃油费',\n" +
            "  `RET_USE_PRICE_OB_FEE` DECIMAL(19, 2) COMMENT '换开变更费',\n" +
            "  `RET_USE_PRICE_TICKET_PRICE` DECIMAL(19, 2) COMMENT '机票价',\n" +
            "  `RET_USE_PRICE_TOTAL_PRICE` DECIMAL(19, 2) COMMENT '总价',\n" +
            "  `RET_TOTAL_TAX_DETAIL` VARCHAR(2000) COMMENT '总的其他税费明细',\n" +
            "  `RET_USE_TAX_DETAIL` VARCHAR(2000) COMMENT '已使用的其他税费明细',\n" +
            "  `XSFSI_INFO` VARCHAR(2000) COMMENT '已使用票面信息的报文',\n" +
            "  `REC_AGENCY_FEE` DECIMAL(8, 2) COMMENT '代理费',\n" +
            "  `FARE_TYPE` INT COMMENT '旅客类型',\n" +
            "  `EXCHANGED_IND` VARCHAR(50) COMMENT '所退机票是否为换开后机票Y是N否',\n" +
            "  `IRR_TICKET_NUMBER` VARCHAR(2000) COMMENT '航变机票号',\n" +
            "  `IRR_AFFECTED_TICKET_NUMBER` VARCHAR(2000) COMMENT '航变关联机票号',\n" +
            "  `ETL_CREATE_TIME` TIMESTAMP(6) NULL  COMMENT '数据入仓时间',\n" +
            "  `ETL_UPDATE_TIME` TIMESTAMP(6)  NULL  COMMENT '数据在数仓更新时间时间',\n" +
            "  `ETL_DATE` date  NULL COMMENT '数据ETL日期'" +
            ") WITH (\n" +

            GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_KFBP_BUS_TK_REFUND_FARE_TICKET_FLT", Constants.ODS_USER, Constants.ODS_PWD)

            + ")";
    public static final String BUS_FLIGHT = "CREATE TABLE T_ODS_KFBP_BUS_FLIGHT (\n" +

            "  `PKID` VARCHAR(1275) NOT NULL COMMENT '主键',\n" +
            "  `CREATE_BY` VARCHAR(200) COMMENT '创建人ID',\n" +
            "  `CREATE_DATE` TIMESTAMP(3) COMMENT '创建时间',\n" +
            "  `CREATE_NAME` VARCHAR(200) COMMENT '创建人名称',\n" +
            "  `UPDATE_BY` VARCHAR(200) COMMENT '修改人',\n" +
            "  `UPDATE_DATE` TIMESTAMP(3) COMMENT '修改时间',\n" +
            "  `UPDATE_NAME` VARCHAR(200) COMMENT '修改人姓名',\n" +
            "  `FLT_ACTION_CODE` VARCHAR(30) COMMENT 'PNR航段状态',\n" +
            "  `FLT_AIRPLANE_TYPE` VARCHAR(40) COMMENT '机型',\n" +
            "  `FLT_ARRIVAL_TERMINAL` VARCHAR(20) COMMENT '到达航站楼',\n" +
            "  `FLT_ARRIVE_CITY` VARCHAR(40) COMMENT '到达城市',\n" +
            "  `FLT_ARRIVE_DAY` DATE COMMENT '到达日期',\n" +
            "  `FLT_ARRIVE_TIME` VARCHAR(20) COMMENT '到达时刻',\n" +
            "  `FLT_CABIN` VARCHAR(20) COMMENT '舱位',\n" +
            "  `FLT_DEPARTURE_DAY` DATE COMMENT '出发日期',\n" +
            "  `FLT_DEPARTURE_TERMINAL` VARCHAR(20) COMMENT '出发航站楼',\n" +
            "  `FLT_DEPARTURE_TIME` VARCHAR(20) COMMENT '出发时刻',\n" +
            "  `FLT_FLIGHT_NO` VARCHAR(40) COMMENT '航班号(市场方)',\n" +
            "  `FLT_IS_ARNK` tinyint COMMENT '是否缺口段',\n" +
            "  `FLT_IS_SHARE_FLIGHT` tinyint COMMENT '是否共享航班',\n" +
            "  `FLT_REAL_FLIGHT_NO` VARCHAR(40) COMMENT '航班号(承运方)',\n" +
            "  `FLT_SEGMENT_STATUS` INT COMMENT '客票航段状态',\n" +
            "  `FLT_START_CITY` VARCHAR(40) COMMENT '出发城市',\n" +
            "  `BUS_ORDER_PKID` VARCHAR(1275) COMMENT '订单ID',\n" +
            "  `ORDER_PKID` VARCHAR(1275) COMMENT '订单ID',\n" +
            "  `FLT_INDEX_IN_PNR` VARCHAR(15) COMMENT '在pnr中的序号',\n" +
            "  `ETL_CREATE_TIME` TIMESTAMP(6) NULL  COMMENT '数据入仓时间',\n" +
            "  `ETL_UPDATE_TIME` TIMESTAMP(6)  NULL  COMMENT '数据在数仓更新时间时间',\n" +
            "  `ETL_DATE` date  NULL COMMENT '数据ETL日期'" +
            ") WITH (\n" +

            GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_KFBP_BUS_FLIGHT", Constants.ODS_USER, Constants.ODS_PWD)

            + ")";
    public static final String BUS_FARE = "CREATE TABLE T_ODS_KFBP_BUS_FARE (\n" +
            "  `PKID` varchar(1000) NOT NULL COMMENT '主键',\n" +
            "  `CREATE_BY` varchar(200)  NULL COMMENT '创建人ID',\n" +
            "  `CREATE_DATE` TIMESTAMP(6)  NULL COMMENT '创建时间',\n" +
            "  `CREATE_NAME` varchar(200)  NULL COMMENT '创建人名称',\n" +
            "  `UPDATE_BY` varchar(200)  NULL COMMENT '修改人',\n" +
            "  `UPDATE_DATE` TIMESTAMP(6)  NULL COMMENT '修改时间',\n" +
            "  `UPDATE_NAME` varchar(200)  NULL COMMENT '修改人姓名',\n" +
            "  `BIRTH` date  NULL COMMENT '出生日期',\n" +
            "  `COUNTY` varchar(1275)  NULL COMMENT '国籍',\n" +
            "  `DEP_NAME` varchar(1275)  NULL COMMENT '旅客所在公司部门',\n" +
            "  `EMAIL` varchar(640)  NULL COMMENT 'EMAIL',\n" +
            "  `FARE_CATEGORY` INT  NULL COMMENT '旅客类别',\n" +
            "  `FARE_NAME` varchar(1275)  NOT NULL COMMENT '订单旅客姓名',\n" +
            "  `FARE_TYPE` INT  NULL COMMENT '旅客类型',\n" +
            "  `FARE_TYPECN` varchar(1275)  NULL COMMENT '旅客类型名称',\n" +
            "  `FFP_CARD_NO` varchar(1275)  NULL COMMENT '常客卡号',\n" +
            "  `FOOD_CODE` varchar(1275)  NULL COMMENT '餐食代码',\n" +
            "  `FOOD_TYPE` INT  NULL COMMENT '餐食代码',\n" +
            "  `GENDER` INT  NULL COMMENT '性别',\n" +
            "  `ID_CODE` varchar(160)  NULL COMMENT '旅客证件号',\n" +
            "  `ID_CODEGP` varchar(150)  NULL COMMENT '旅客证件号码GP',\n" +
            "  `ID_EXPIRY_DATE` TIMESTAMP(6)  NULL COMMENT '证件有效期',\n" +
            "  `ID_TYPE` INT  NULL COMMENT '旅客证件类型',\n" +
            "  `ID_TYPECN` varchar(1275)  NULL COMMENT '证件名称',\n" +
            "  `ID_TYPEGP` INT  NULL COMMENT '旅客证件类型GP',\n" +
            "  `IS_SEND_SMS` tinyINT  NULL COMMENT '是否发送短信',\n" +
            "  `ISSUE_COUNTRY` varchar(1275)  NULL COMMENT '证件签发国',\n" +
            "  `MOBILE` varchar(1275)  NULL COMMENT '旅客电话',\n" +
            "  `PARENT_NAME` varchar(1275)  NULL COMMENT '父亲名字',\n" +
            "  `PRICE_ALL_FEE` decimal(19,2)  NULL COMMENT '总税',\n" +
            "  `PRICE_BUILD_FEE` decimal(19,2)  NULL COMMENT '机场建设费',\n" +
            "  `PRICE_CABIN_PRICE` decimal(19,2)  NULL COMMENT '舱位原价',\n" +
            "  `PRICE_CURRENCY` INT  NULL COMMENT '支付币种',\n" +
            "  `PRICE_EXCHANGE_RATE` decimal(10,4)  NULL COMMENT '汇率',\n" +
            "  `PRICE_FUEL_FEE` decimal(19,2)  NULL COMMENT '机场燃油费',\n" +
            "  `PRICE_OB_FEE` decimal(19,2)  NULL COMMENT '换开变更费',\n" +
            "  `PRICE_TICKET_PRICE` decimal(19,2)  NULL COMMENT '机票价',\n" +
            "  `PRICE_TOTAL_PRICE` decimal(19,2)  NULL COMMENT '总价',\n" +
            "  `SPECIAL_TYPE` INT  NULL COMMENT '特殊服务项',\n" +
            "  `TAXES_JSON` varchar(5000)  NULL COMMENT '税明细json',\n" +
            "  `US_ADDRESS` varchar(500)  NULL COMMENT '美国航线地址',\n" +
            "  `US_CITY` varchar(200)  NULL COMMENT '美国航线城市名',\n" +
            "  `ZHIWU` varchar(640)  NULL COMMENT '特殊服务VIP  职务',\n" +
            "  `ZIP_CODE` varchar(100)  NULL COMMENT '美国航线邮编',\n" +
            "  `BUS_ORDER_PKID` varchar(1275)  NULL COMMENT '订单ID',\n" +
            "  `INSURE_ORDER_PKID` varchar(1275)  NULL COMMENT '订单ID',\n" +
            "  `ORDER_PKID` varchar(1275)  NULL COMMENT '订单ID',\n" +
            "  `INDEX_IN_PNR` varchar(15)  NULL COMMENT '在pnr中的序号',\n" +
            "  `FARE_NAME_CN` varchar(1275)  NULL COMMENT '订单旅客姓名中文国际GP用',\n" +
            "  `GOV_BIN` varchar(1275)  NULL COMMENT '政采卡bin',\n" +
            "  `GOV_BUGDET_NAME` varchar(1275)  NULL COMMENT '政采预算单位名称',\n" +
            "  `FARE_NAMEEN` varchar(1275)  NULL COMMENT '用于婴儿的SSR INFT中的英文名',\n" +
            "  `GMJC_RMK` varchar(320)  NULL COMMENT '革残警残军官备注',\n" +
            "  `LYX_ORDERS` varchar(1275)  NULL COMMENT '鲁彦行订单',\n" +
            "  `ETL_CREATE_TIME` TIMESTAMP(6) NULL  COMMENT '数据入仓时间',\n" +
            "  `ETL_UPDATE_TIME` TIMESTAMP(6)  NULL  COMMENT '数据在数仓更新时间时间',\n" +
            "  `ETL_DATE` date  NULL COMMENT '数据ETL日期'" +
            ") WITH (\n" +

            GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_KFBP_BUS_FARE", Constants.ODS_USER, Constants.ODS_PWD)

            + ")";
    public static final String BUS_FARE_TICKET = "CREATE TABLE T_ODS_KFBP_BUS_FARE_TICKET (\n" +
            "  `PKID` VARCHAR(1000) NOT NULL,\n" +
                    "  `CREATE_BY` VARCHAR(200),\n" +
                    "  `CREATE_DATE` TIMESTAMP(3),\n" +
                    "  `CREATE_NAME` VARCHAR(200),\n" +
                    "  `UPDATE_BY` VARCHAR(200),\n" +
                    "  `UPDATE_DATE` TIMESTAMP(3),\n" +
                    "  `UPDATE_NAME` VARCHAR(200),\n" +
                    "  `PAY_BALANCE_STATUS` INT,\n" +
                    "  `PAY_CONFIRM_STATUS` INT,\n" +
                    "  `PAY_TRADE_NO` VARCHAR(80),\n" +
                    "  `PRICE_ALL_FEE` DECIMAL(19, 2),\n" +
                    "  `PRICE_BUILD_FEE` DECIMAL(19, 2),\n" +
                    "  `PRICE_CABIN_PRICE` DECIMAL(19, 2),\n" +
                    "  `PRICE_CURRENCY` INT,\n" +
                    "  `PRICE_EXCHANGE_RATE` DECIMAL(10, 4),\n" +
                    "  `PRICE_FUEL_FEE` DECIMAL(19, 2),\n" +
                    "  `PRICE_OB_FEE` DECIMAL(19, 2),\n" +
                    "  `PRICE_TICKET_PRICE` DECIMAL(19, 2),\n" +
                    "  `PRICE_TOTAL_PRICE` DECIMAL(19, 2),\n" +
                    "  `TICKET_NO` VARCHAR(80),\n" +
                    "  `FARE_PKID` VARCHAR(1275),\n" +
                    "  `ORDER_PKID` VARCHAR(1275),\n" +
                    "  `IS_VOID` tinyint,  -- tinyint 转换为 BOOLEAN\n" +
                    "  `BILL_TIME` TIMESTAMP(3),\n" +
                    "  `IS_REGISTRATION` tinyint,\n" +
                    "  `ETL_CREATE_TIME` TIMESTAMP(6) NULL  COMMENT '数据入仓时间',\n" +
                    "  `ETL_UPDATE_TIME` TIMESTAMP(6)  NULL  COMMENT '数据在数仓更新时间时间',\n" +
                    "  `ETL_DATE` date  NULL COMMENT '数据ETL日期'" +
                    ") WITH (\n" +

                    GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_KFBP_BUS_FARE_TICKET", Constants.ODS_USER, Constants.ODS_PWD)

                    + ")";
}
