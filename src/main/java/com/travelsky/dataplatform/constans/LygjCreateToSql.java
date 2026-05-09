package com.travelsky.dataplatform.constans;

import com.travelsky.dataplatform.utils.GetTableSql;

public class LygjCreateToSql {


    public static final String VIP_INFO = "CREATE TABLE T_ODS_LYGJ_VIP_INFO (\n" +
            "  ETL_DATE DATE NOT NULL  COMMENT '数据ETL日期',\n" +
            "  ID BIGINT NOT NULL COMMENT '主键ID',\n" +
            "  VIP_TYPE_ID BIGINT COMMENT 'VIP类型ID',\n" +
            "  NAME VARCHAR(100) COMMENT '姓名',\n" +
            "  GENDER VARCHAR(16) COMMENT '性别',\n" +
            "  NATIONALITY VARCHAR(20) COMMENT '国籍',\n" +
            "  ID_CARD VARCHAR(4000) COMMENT '身份证件号',\n" +
            "  DUTY VARCHAR(320) COMMENT '职务',\n" +
            "  CARD_TYPE VARCHAR(64) COMMENT '卡类型',\n" +
            "  CARD_NUM VARCHAR(96) COMMENT '卡号',\n" +
            "  MOBILE VARCHAR(4000) COMMENT '手机号',\n" +
            "  PHOTO VARCHAR(64) COMMENT '照片',\n" +
            "  FAVORITE VARCHAR(1024) COMMENT '喜好',\n" +
            "  SERVICE_DESC VARCHAR(1024) COMMENT '服务描述',\n" +
            "  MEMO VARCHAR(1024) COMMENT '备注',\n" +
            "  CERTIFICATE_TYPE VARCHAR(10) COMMENT '证件类型',\n" +
            "  FOOD_DESC VARCHAR(1024) COMMENT '餐饮描述',\n" +
            "  SEAT_LOVE VARCHAR(1024) COMMENT '座位偏好',\n" +
            "  OTHER VARCHAR(1024) COMMENT '其他信息',\n" +
            "  PAXID BIGINT NOT NULL COMMENT '旅客ID',\n" +
            "  GRP_ID VARCHAR(20) COMMENT '团队ID',\n" +
            "  GRP_CLASS VARCHAR(20) COMMENT '团队等级',\n" +
            "  UPD_TIME TIMESTAMP(6) COMMENT '更新时间',\n" +
            "  UPD_USER VARCHAR(50) COMMENT '更新用户',\n" +
            "  BL1 VARCHAR(60) COMMENT '保留字段1',\n" +
            "  BL2 VARCHAR(60) COMMENT '保留字段2',\n" +
            "  BL3 VARCHAR(60) COMMENT '保留字段3',\n" +
            "  BL4 VARCHAR(20) COMMENT '保留字段4',\n" +
            "  BL5 VARCHAR(20) COMMENT '保留字段5',\n" +
            "  BL6 VARCHAR(20) COMMENT '保留字段6',\n" +
            "  BL7 VARCHAR(20) COMMENT '保留字段7',\n" +
            "  BL8 VARCHAR(20) COMMENT '保留字段8',\n" +
            "  FOOD_DESC_TEMP VARCHAR(1024) COMMENT '餐饮描述临时',\n" +
            "  SEAT_LOVE_TEMP VARCHAR(1024) COMMENT '座位偏好临时',\n" +
            "  OTHER_TEMP VARCHAR(1024) COMMENT '其他信息临时',\n" +
            "  COMPANY VARCHAR(200) COMMENT '公司',\n" +
            "  VIP_APPEAL VARCHAR(2000) COMMENT 'VIP诉求',\n" +
            "  NAME_MEMO VARCHAR(50) COMMENT '姓名备注',\n" +
            "  VIP_FOOD_LOVE_IDS VARCHAR(500) COMMENT '喜爱食品ID',\n" +
            "  VIP_FOOD_LOVE_NAME VARCHAR(500) COMMENT '喜爱食品名称',\n" +
            "  VIP_FOOD_LOVE_MEMO VARCHAR(500) COMMENT '食品备注',\n" +
            "  VIP_DRINK_LOVE_IDS VARCHAR(500) COMMENT '喜爱饮品ID',\n" +
            "  VIP_DRINK_LOVE_NAMES VARCHAR(500) COMMENT '喜爱饮品名称',\n" +
            "  VIP_DRINK_LOVE_MEMO VARCHAR(500) COMMENT '饮品备注',\n" +
            "  VIP_SEAT_LOVE_IDS VARCHAR(500) COMMENT '座位偏好ID',\n" +
            "  VIP_SEAT_LOVE_NAMES VARCHAR(500) COMMENT '座位偏好名称',\n" +
            "  VIP_SEAT_LOVE_MEMO VARCHAR(500) COMMENT '座位备注',\n" +
            "  VIP_LOUNGE_LOVE_IDS VARCHAR(500) COMMENT '休息室偏好ID',\n" +
            "  VIP_LOUNGE_LOVE_NAMES VARCHAR(500) COMMENT '休息室偏好名称',\n" +
            "  VIP_LOUNGE_LOVE_MEMO VARCHAR(500) COMMENT '休息室备注',\n" +
            "  BIRTHDAY VARCHAR(10) COMMENT '生日',\n" +
            "  UPDATE_TIME TIMESTAMP(6) COMMENT '更新时间',\n" +
            "  CREATE_TIME TIMESTAMP(6) COMMENT '创建时间',\n" +
            "  VIP_TYPE_NAMES VARCHAR(100) COMMENT 'VIP类型名称',\n" +
            "  CREATER VARCHAR(100) COMMENT '创建人',\n" +
            "  UPDATE_PERSON VARCHAR(100) COMMENT '更新人',\n" +
            "  GRADE VARCHAR(100) COMMENT '等级',\n" +
            "  APPLICANTDEPT BIGINT COMMENT '申请部门',\n" +
            "  APPLICANT BIGINT COMMENT '申请人',\n" +
            "  CREATORDEPT BIGINT COMMENT '创建部门',\n" +
            "  LAYER_TYPE_ID VARCHAR(20) COMMENT '层级类型ID',\n" +
            "  LAYER_SOURCE VARCHAR(100) COMMENT '层级来源',\n" +
            "  LAYER_BASE VARCHAR(100) COMMENT '基础层级',\n" +
            "  LAYER_CREATE_TM TIMESTAMP(6) COMMENT '层级创建时间',\n" +
            "  LAYER_UPDATE_TM TIMESTAMP(6) COMMENT '层级更新时间',\n" +
            "  LAYER_VALIDITY TIMESTAMP(6) COMMENT '层级有效期',\n" +
            "  LAYER_LONG_VALIDITY VARCHAR(6) COMMENT '长期有效标识'\n" +
            ") WITH (\n" +
            GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_LYGJ_VIP_INFO", Constants.ODS_USER, Constants.ODS_PWD)
            + ")";
    public static final String VIP_TYPE = "CREATE TABLE T_ODS_LYGJ_VIP_TYPE (\n" +
            "    ETL_DATE DATE NOT NULL  COMMENT '数据ETL日期',\n" +
            "    ID BIGINT NOT NULL COMMENT 'VIP类型ID',\n" +
            "    CODE VARCHAR(16) NOT NULL COMMENT 'VIP类型编码',\n" +
            "    NAME VARCHAR(64) NOT NULL COMMENT 'VIP类型名称',\n" +
            "    MEMO VARCHAR(1024) COMMENT '备注',\n" +
            "    BOOK_TICKET VARCHAR(1) COMMENT '订票标识',\n" +
            "    PROVIDE_FOOD VARCHAR(1) COMMENT '提供餐饮标识',\n" +
            "    SENT_CAR VARCHAR(1) COMMENT '派车标识',\n" +
            "    BOARD_CHECK VARCHAR(1) COMMENT '登机检查标识',\n" +
            "    RECEIVED_MAN VARCHAR(1) COMMENT '接待人员标识',\n" +
            "    VISITANT_ROOM VARCHAR(1) COMMENT '贵宾室标识',\n" +
            "    CABIN_SERVICE VARCHAR(1) COMMENT '客舱服务标识',\n" +
            "    RECEIVED_CAR VARCHAR(1) COMMENT '接车标识',\n" +
            "    RECEIVED_OFFICE VARCHAR(1) COMMENT '接待办公室标识',\n" +
            "    OTHER VARCHAR(1) COMMENT '其他服务标识',\n" +
            "    BL1 VARCHAR(60) COMMENT '保留字段1',\n" +
            "    BL2 VARCHAR(60) COMMENT '保留字段2',\n" +
            "    BL3 VARCHAR(60) COMMENT '保留字段3',\n" +
            "    BL4 VARCHAR(20) COMMENT '保留字段4',\n" +
            "    BL5 VARCHAR(20) COMMENT '保留字段5',\n" +
            "    BL6 VARCHAR(20) COMMENT '保留字段6',\n" +
            "    BL7 VARCHAR(20) COMMENT '保留字段7',\n" +
            "    BL8 VARCHAR(20) COMMENT '保留字段8',\n" +
            "    IS_VIP VARCHAR(20) COMMENT '是否VIP标识',\n" +
            "    REAL_NAME_FLAG VARCHAR(2) COMMENT '实名认证标志'" +
            ") WITH (\n" +

            GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_LYGJ_VIP_TYPE", Constants.ODS_USER, Constants.ODS_PWD)

            + ")";
    public static final String LAYER_TYPE = "CREATE TABLE T_ODS_LYGJ_LAYER_TYPE (\n" +
            "   ETL_DATE DATE NOT NULL  COMMENT '数据ETL日期',\n" +
            "    ID BIGINT NOT NULL COMMENT '唯一标识符',\n" +
            "    TYPE_NAME VARCHAR(64) NOT NULL COMMENT '类型名称',\n" +
            "    VIP_TYPE_ID BIGINT COMMENT 'VIP类型ID',\n" +
            "    PRIORITY BIGINT NOT NULL COMMENT '优先级',\n" +
            "    CREATE_TM TIMESTAMP(6) COMMENT '创建时间',\n" +
            "    CREATOR VARCHAR(128) COMMENT '创建人',\n" +
            "    CANCEL_FLAG VARCHAR(2) COMMENT '取消标志',\n" +
            "    UPDATE_TM TIMESTAMP(6) COMMENT '更新时间',\n" +
            "    UPDATER VARCHAR(128) COMMENT '更新人',\n" +
            "    MEMO VARCHAR(1024) COMMENT '备注',\n" +
            "    PHONE_REMINDER VARCHAR(2) COMMENT '电话提醒标志',\n" +
            "    CHANGE_SMS_FLAG VARCHAR(2) COMMENT '变更短信通知标志',\n" +
            "    REAL_NAME_FLAG VARCHAR(2) COMMENT '实名认证标志'" +
            ") WITH (\n" +
            GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_LYGJ_LAYER_TYPE", Constants.ODS_USER, Constants.ODS_PWD)
            + ")";

    //创建Doris目标表
    public static String DEPART_PASSENGER_INFO = "CREATE TABLE T_ODS_LYGJ_DEPART_PASSENGER_INFO (\n" +
            "  ETL_DATE DATE,\n" +
            "  IDX BIGINT,\n" +
            "  PSG_NAME_EN VARCHAR(100),\n" +
            "  PSG_NAME_CN VARCHAR(100),\n" +
            "  FLT_SEG_IDS BIGINT,\n" +
            "  FLT_NUM VARCHAR(10),\n" +
            "  FLT_DATE TIMESTAMP(6),\n" +
            "  ORIG_SEQ VARCHAR(2),\n" +
            "  DEST_SEQ VARCHAR(2),\n" +
            "  PNR_ICS VARCHAR(10),\n" +
            "  PNR_CRS VARCHAR(20),\n" +
            "  MAIN_CLASS VARCHAR(2),\n" +
            "  SELL_CLASS VARCHAR(2),\n" +
            "  FFP VARCHAR(50),\n" +
            "  ET VARCHAR(4000),\n" +
            "  ET_NUM VARCHAR(20),\n" +
            "  ET_ID_NUM VARCHAR(4000),\n" +
            "  ID_NUM VARCHAR(4000),\n" +
            "  IS_VIP VARCHAR(2),\n" +
            "  IS_CIP VARCHAR(2),\n" +
            "  IS_BLND VARCHAR(2),\n" +
            "  IS_BSCT VARCHAR(2),\n" +
            "  IS_GOSHOW VARCHAR(2),\n" +
            "  IS_DEAF VARCHAR(2),\n" +
            "  IS_INFANT VARCHAR(2),\n" +
            "  IS_SPML VARCHAR(2),\n" +
            "  SPML_ITEM VARCHAR(100),\n" +
            "  EXST VARCHAR(20),\n" +
            "  IS_STCR VARCHAR(2),\n" +
            "  WCHR_TYPE VARCHAR(5),\n" +
            "  GROUP_ID VARCHAR(50),\n" +
            "  RECHECK_TO VARCHAR(20),\n" +
            "  RECHECK_FROM VARCHAR(20),\n" +
            "  IS_XRES VARCHAR(2),\n" +
            "  GENDER VARCHAR(3),\n" +
            "  SEAT_NO VARCHAR(20),\n" +
            "  STATUS VARCHAR(4),\n" +
            "  BAGS VARCHAR(5),\n" +
            "  BAGWHT VARCHAR(10),\n" +
            "  MSG VARCHAR(400),\n" +
            "  PSM VARCHAR(1000),\n" +
            "  PIL VARCHAR(100),\n" +
            "  PCTC VARCHAR(500),\n" +
            "  PSPT VARCHAR(4000),\n" +
            "  HOSTNBR VARCHAR(5),\n" +
            "  BRD_NO VARCHAR(5),\n" +
            "  CKI_TIME VARCHAR(5),\n" +
            "  CKI_AGENT VARCHAR(8),\n" +
            "  CKI_OFFICE VARCHAR(10),\n" +
            "  CKI_PID VARCHAR(8),\n" +
            "  SIP VARCHAR(10),\n" +
            "  UD_GRADE VARCHAR(2),\n" +
            "  VUD_GRADE VARCHAR(2),\n" +
            "  UD_CLASS VARCHAR(2),\n" +
            "  OFL VARCHAR(2),\n" +
            "  DEP VARCHAR(2),\n" +
            "  EDI_IN VARCHAR(2),\n" +
            "  EDI_OUT VARCHAR(2),\n" +
            "  INBOUND VARCHAR(256),\n" +
            "  OUTBOUND VARCHAR(256),\n" +
            "  EDIWARN VARCHAR(100),\n" +
            "  EDIMARK VARCHAR(2),\n" +
            "  RELATION VARCHAR(4000),\n" +
            "  BAGTAG VARCHAR(2000),\n" +
            "  FILE_NAME VARCHAR(100),\n" +
            "  ORIG_CNNAME VARCHAR(100),\n" +
            "  IN_FLT_NUM VARCHAR(10),\n" +
            "  IN_FLT_DATE TIMESTAMP(6),\n" +
            "  IN_CLASS VARCHAR(1),\n" +
            "  IN_CITY VARCHAR(3),\n" +
            "  IN_BN VARCHAR(6),\n" +
            "  IN_SEAT VARCHAR(6),\n" +
            "  OUT_FLT_NUM VARCHAR(10),\n" +
            "  OUT_FLT_DATE TIMESTAMP(6),\n" +
            "  OUT_CLASS VARCHAR(1),\n" +
            "  OUT_CITY VARCHAR(3),\n" +
            "  RESERVED_SEAT VARCHAR(20),\n" +
            "  ORIG VARCHAR(3),\n" +
            "  DEST VARCHAR(3),\n" +
            "  IS_FFP_JK VARCHAR(2),\n" +
            "  TRA_FLT_IDS BIGINT,\n" +
            "  BIRTHDAYONFLT VARCHAR(2),\n" +
            "  SPT_ID BIGINT,\n" +
            "  IS_UM VARCHAR(20),\n" +
            "  UPDATE_TIME TIMESTAMP(6),\n" +
            "  UPDATE_USER VARCHAR(30),\n" +
            "  VERIFY_STATUS VARCHAR(30),\n" +
            "  SPECIAL_TYPE VARCHAR(16),\n" +
            "  CREATE_TIME TIMESTAMP(6),\n" +
            "  BIRTHDAY VARCHAR(10),\n" +
            "  RECORD_LAST_UPDATE_TIME TIMESTAMP(6),\n" +
            "  RECORD_CREATE_TIME TIMESTAMP(6),\n" +
            "  STAR_FFP VARCHAR(10),\n" +
            "  STAR_FFP_NO VARCHAR(50),\n" +
            "  LAST_FQTV_LEVEL VARCHAR(100),\n" +
            "  TEL_NUM VARCHAR(4000),\n" +
            "  SUID VARCHAR(500),\n" +
            "  FQTV_DETAIL_LEVEL VARCHAR(100),\n" +
            "  MDM_GENDER CHAR(1),\n" +
            "  CERT_NO VARCHAR(4000),\n" +
            "  SPECIAL_SERVICE VARCHAR(10),\n" +
            "  SPECIAL_TAG VARCHAR(10),\n" +
            "  IS_VVIP VARCHAR(5),\n" +
            "  IS_CHILD VARCHAR(5),\n" +
            "  CKI_TYPE VARCHAR(5),\n" +
            "  PROC_AGENT VARCHAR(20),\n" +
            "  CKI_DATE VARCHAR(20),\n" +
            "  INFANT_NAME VARCHAR(50),\n" +
            "  RUSH_LUGGAGE VARCHAR(100),\n" +
            "  DRY_BAT_WHEELCHAIR VARCHAR(100),\n" +
            "  WET_BAT_WHEELCHAIR VARCHAR(100),\n" +
            "  ELEC_BAT_WHEELCHAIR VARCHAR(100),\n" +
            "  ONBOARD_WHEELCHAIR VARCHAR(100),\n" +
            "  CKI_INFO VARCHAR(100),\n" +
            "  PREFER_SEAT VARCHAR(20),\n" +
            "  FFP_VALID_FAIL VARCHAR(20),\n" +
            "  INCABIN_LITTLE_ANIMAL_WITHCAGE VARCHAR(100),\n" +
            "  INCABIN_LITTLE_ANIMAL_NOCAGE VARCHAR(100),\n" +
            "  ALLIANCE_SENIOR_TIER_LEVEL VARCHAR(20),\n" +
            "  BLACK_LIST_TAG VARCHAR(20),\n" +
            "  AEC_REDISTRIBUTE_SEAT VARCHAR(20),\n" +
            "  AEC_REBOOK_SEAT VARCHAR(20),\n" +
            "  CANCEL_CHANGE_PORT VARCHAR(20),\n" +
            "  STAND_BY_PSR VARCHAR(20),\n" +
            "  SEAT_OCCUPIED VARCHAR(20),\n" +
            "  ASR_SEAT VARCHAR(20),\n" +
            "  SSR_MAAS VARCHAR(20),\n" +
            "  SSR_MEDA VARCHAR(20),\n" +
            "  SSR_LANG VARCHAR(20),\n" +
            "  SSR_DPNA VARCHAR(20),\n" +
            "  SSR_SPEQ VARCHAR(20),\n" +
            "  SSR_PPOC VARCHAR(20),\n" +
            "  SSR_UMPG VARCHAR(20),\n" +
            "  SSR_UMBD VARCHAR(20),\n" +
            "  SSR_UMDF VARCHAR(20),\n" +
            "  SSR_UMLG VARCHAR(20),\n" +
            "  SSR_SPBG VARCHAR(20),\n" +
            "  SSR_YFPB VARCHAR(20),\n" +
            "  SSR_WCON VARCHAR(20),\n" +
            "  SSR_UMAD VARCHAR(20),\n" +
            "  ACTION_CODE VARCHAR(5),\n" +
            "  ID_TYPE VARCHAR(50),\n" +
            "  ID_INFO VARCHAR(4000),\n" +
            "  MC_FLT VARCHAR(10),\n" +
            "  PRE_SEG VARCHAR(50),\n" +
            "  NEXT_SEG VARCHAR(50),\n" +
            "  IO_FLAG VARCHAR(20),\n" +
            "  EDI_FLAG VARCHAR(5),\n" +
            "  ET_SEG VARCHAR(5),\n" +
            "  FARE VARCHAR(20),\n" +
            "  PSR_TYPE VARCHAR(10),\n" +
            "  CKI_NUM VARCHAR(1000),\n" +
            "  DOWN_GRADE VARCHAR(10),\n" +
            "  AVIH_BAGS VARCHAR(20),\n" +
            "  AVIH_BAGWGT VARCHAR(10),\n" +
            "  AVIH_BAG_TAG VARCHAR(1000),\n" +
            "  COM_UNIQUE_STR VARCHAR(200),\n" +
            "  FLT_SEG_COM_UNIQUE_STR VARCHAR(200),\n" +
            "  ISTRANSITSEND VARCHAR(5),\n" +
            "  PRINT_TICKET_TIME TIMESTAMP(6),\n" +
            "  CHECK_IN_TIME TIMESTAMP(6),\n" +
            "  ID_CARD_AGE VARCHAR(5),\n" +
            "  ID_CARD_TYPE VARCHAR(3),\n" +
            "  ID_CARD_NATIVE VARCHAR(5),\n" +
            "  MOBILE_PHONE VARCHAR(4000),\n" +
            "  MOBILE_PHONE_SOURCE VARCHAR(50),\n" +
            "  BOOK_AGT VARCHAR(50),\n" +
            "  BOOK_OFFI VARCHAR(50),\n" +
            "  IS_EXCHANGE_CARD VARCHAR(1),\n" +
            "  ID_SEQ BIGINT,\n" +
            "  BKO_UPTIME TIMESTAMP(6),\n" +
            "  PNR_GROUP_ID VARCHAR(50),\n" +
            "  LKT_IDX BIGINT,\n" +
            "  LKT_STATUS VARCHAR(20),\n" +
            "  MOBILES VARCHAR(4000),\n" +
            "  TELEPHONE VARCHAR(4000),\n" +
            "  LKT_UPTIME TIMESTAMP(6),\n" +
            "  CKI_STATUS VARCHAR(6),\n" +
            "  COUPON_STATUS VARCHAR(6),\n" +
            "  ORIGINALTKNE VARCHAR(20),\n" +
            "  MARKGP VARCHAR(3),\n" +
            "  BIG_CUTNUM VARCHAR(10),\n" +
            "  EVENT VARCHAR(40),\n" +
            "  INF_ET_NUM VARCHAR(20),\n" +
            "  FLT_NUM_SELECT VARCHAR(10),\n" +
            "  LKT_XR VARCHAR(4),\n" +
            "  LKTXR_UPTM TIMESTAMP(6),\n" +
            "  GDS_CODE VARCHAR(6),\n" +
            "  FBA_MAX_PIECES VARCHAR(6),\n" +
            "  FBA_TOTAL_WEIGHT VARCHAR(6),\n" +
            "  TKNEGP VARCHAR(3),\n" +
            "  DCS_DPTM VARCHAR(10),\n" +
            "  DCS_ATTM VARCHAR(10),\n" +
            "  PNR_SELL_CLASS VARCHAR(2),\n" +
            "  PAX_FFP VARCHAR(20),\n" +
            "  IS_GROUP VARCHAR(2),\n" +
            "  GROUP_NAME VARCHAR(20),\n" +
            "  PASSENGER_NUMBER VARCHAR(6),\n" +
            "  DCS_SELL_CLASS VARCHAR(2),\n" +
            "  ISSUE_AGENT VARCHAR(10),\n" +
            "  ISSUE_OFFI VARCHAR(10),\n" +
            "  PHONE_FLG BIGINT,\n" +
            "  PHONE_FLG_DATE TIMESTAMP(6),\n" +
            "  DCS_ATDATE TIMESTAMP(6),\n" +
            "  ORIG_SUB_CLASS VARCHAR(100),\n" +
            "  TICKET_TYPE VARCHAR(100),\n" +
            "  IATA_CODE VARCHAR(100),\n" +
            "  ISSUE_COUNTRY VARCHAR(100),\n" +
            "  SSR_INFO STRING,\n" +
            "  CKI_CITY VARCHAR(100),\n" +
            "  TCHB VARCHAR(100),\n" +
            "  PETC_BAGS VARCHAR(100),\n" +
            "  PETC_BAGWGT VARCHAR(100),\n" +
            "  PETC_BAG_TAG VARCHAR(100),\n" +
            "  IS_CHILDCRADLE VARCHAR(16),\n" +
            "  IS_CHILDPASSENGER VARCHAR(16),\n" +
            "  IS_DEPA VARCHAR(16),\n" +
            "  IS_DEPU VARCHAR(16),\n" +
            "  IS_DNPA VARCHAR(16),\n" +
            "  IS_DPNA VARCHAR(16),\n" +
            "  IS_INAD VARCHAR(16),\n" +
            "  IS_LEGRORLEGL VARCHAR(16),\n" +
            "  IS_INF VARCHAR(16),\n" +
            "  IS_MEDA VARCHAR(16),\n" +
            "  IS_HELPPASSENGER VARCHAR(16),\n" +
            "  IS_SVAN VARCHAR(16),\n" +
            "  IS_CABINDOG VARCHAR(16),\n" +
            "  IS_PPOC_PSG VARCHAR(1),\n" +
            "  RESERVE_SEAT_NO VARCHAR(30),\n" +
            "  BAG_TIME TIMESTAMP(6),\n" +
            "  BAG_AGENT VARCHAR(10),\n" +
            "  FARE_BASIS_CODE VARCHAR(100),\n" +
            "  IS_PETC VARCHAR(2),\n" +
            "  EMAIL VARCHAR(500)" +

            ") WITH (\n" +
            GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_LYGJ_DEPART_PASSENGER_INFO", Constants.ODS_USER, Constants.ODS_PWD) +
            ")";

    //创建Doris目标表
    public static String DEPART_PASSENGER_INFO_BAK = "CREATE TABLE T_ODS_LYGJ_DEPART_PASSENGER_INFO_BAK (\n" +
            "  ETL_DATE DATE,\n" +
            "  IDX BIGINT,\n" +
            "  PSG_NAME_EN VARCHAR(100),\n" +
            "  PSG_NAME_CN VARCHAR(100),\n" +
            "  FLT_SEG_IDS BIGINT,\n" +
            "  FLT_NUM VARCHAR(10),\n" +
            "  FLT_DATE TIMESTAMP(6),\n" +
            "  ORIG_SEQ VARCHAR(2),\n" +
            "  DEST_SEQ VARCHAR(2),\n" +
            "  PNR_ICS VARCHAR(10),\n" +
            "  PNR_CRS VARCHAR(20),\n" +
            "  MAIN_CLASS VARCHAR(2),\n" +
            "  SELL_CLASS VARCHAR(2),\n" +
            "  FFP VARCHAR(50),\n" +
            "  ET VARCHAR(4000),\n" +
            "  ET_NUM VARCHAR(20),\n" +
            "  ET_ID_NUM VARCHAR(4000),\n" +
            "  ID_NUM VARCHAR(4000),\n" +
            "  IS_VIP VARCHAR(2),\n" +
            "  IS_CIP VARCHAR(2),\n" +
            "  IS_BLND VARCHAR(2),\n" +
            "  IS_BSCT VARCHAR(2),\n" +
            "  IS_GOSHOW VARCHAR(2),\n" +
            "  IS_DEAF VARCHAR(2),\n" +
            "  IS_INFANT VARCHAR(2),\n" +
            "  IS_SPML VARCHAR(2),\n" +
            "  SPML_ITEM VARCHAR(100),\n" +
            "  EXST VARCHAR(20),\n" +
            "  IS_STCR VARCHAR(2),\n" +
            "  WCHR_TYPE VARCHAR(5),\n" +
            "  GROUP_ID VARCHAR(50),\n" +
            "  RECHECK_TO VARCHAR(20),\n" +
            "  RECHECK_FROM VARCHAR(20),\n" +
            "  IS_XRES VARCHAR(2),\n" +
            "  GENDER VARCHAR(3),\n" +
            "  SEAT_NO VARCHAR(20),\n" +
            "  STATUS VARCHAR(4),\n" +
            "  BAGS VARCHAR(5),\n" +
            "  BAGWHT VARCHAR(10),\n" +
            "  MSG VARCHAR(400),\n" +
            "  PSM VARCHAR(1000),\n" +
            "  PIL VARCHAR(100),\n" +
            "  PCTC VARCHAR(500),\n" +
            "  PSPT VARCHAR(4000),\n" +
            "  HOSTNBR VARCHAR(5),\n" +
            "  BRD_NO VARCHAR(5),\n" +
            "  CKI_TIME VARCHAR(5),\n" +
            "  CKI_AGENT VARCHAR(8),\n" +
            "  CKI_OFFICE VARCHAR(10),\n" +
            "  CKI_PID VARCHAR(8),\n" +
            "  SIP VARCHAR(10),\n" +
            "  UD_GRADE VARCHAR(2),\n" +
            "  VUD_GRADE VARCHAR(2),\n" +
            "  UD_CLASS VARCHAR(2),\n" +
            "  OFL VARCHAR(2),\n" +
            "  DEP VARCHAR(2),\n" +
            "  EDI_IN VARCHAR(2),\n" +
            "  EDI_OUT VARCHAR(2),\n" +
            "  INBOUND VARCHAR(256),\n" +
            "  OUTBOUND VARCHAR(256),\n" +
            "  EDIWARN VARCHAR(100),\n" +
            "  EDIMARK VARCHAR(2),\n" +
            "  RELATION VARCHAR(4000),\n" +
            "  BAGTAG VARCHAR(2000),\n" +
            "  FILE_NAME VARCHAR(100),\n" +
            "  ORIG_CNNAME VARCHAR(100),\n" +
            "  IN_FLT_NUM VARCHAR(10),\n" +
            "  IN_FLT_DATE TIMESTAMP(6),\n" +
            "  IN_CLASS VARCHAR(1),\n" +
            "  IN_CITY VARCHAR(3),\n" +
            "  IN_BN VARCHAR(6),\n" +
            "  IN_SEAT VARCHAR(6),\n" +
            "  OUT_FLT_NUM VARCHAR(10),\n" +
            "  OUT_FLT_DATE TIMESTAMP(6),\n" +
            "  OUT_CLASS VARCHAR(1),\n" +
            "  OUT_CITY VARCHAR(3),\n" +
            "  RESERVED_SEAT VARCHAR(20),\n" +
            "  ORIG VARCHAR(3),\n" +
            "  DEST VARCHAR(3),\n" +
            "  IS_FFP_JK VARCHAR(2),\n" +
            "  TRA_FLT_IDS BIGINT,\n" +
            "  BIRTHDAYONFLT VARCHAR(2),\n" +
            "  SPT_ID BIGINT,\n" +
            "  IS_UM VARCHAR(20),\n" +
            "  UPDATE_TIME TIMESTAMP(6),\n" +
            "  UPDATE_USER VARCHAR(30),\n" +
            "  VERIFY_STATUS VARCHAR(30),\n" +
            "  SPECIAL_TYPE VARCHAR(16),\n" +
            "  CREATE_TIME TIMESTAMP(6),\n" +
            "  BIRTHDAY VARCHAR(10),\n" +
            "  RECORD_LAST_UPDATE_TIME TIMESTAMP(6),\n" +
            "  RECORD_CREATE_TIME TIMESTAMP(6),\n" +
            "  STAR_FFP VARCHAR(10),\n" +
            "  STAR_FFP_NO VARCHAR(50),\n" +
            "  LAST_FQTV_LEVEL VARCHAR(100),\n" +
            "  TEL_NUM VARCHAR(4000),\n" +
            "  SUID VARCHAR(500),\n" +
            "  FQTV_DETAIL_LEVEL VARCHAR(100),\n" +
            "  MDM_GENDER CHAR(1),\n" +
            "  CERT_NO VARCHAR(4000),\n" +
            "  SPECIAL_SERVICE VARCHAR(10),\n" +
            "  SPECIAL_TAG VARCHAR(10),\n" +
            "  IS_VVIP VARCHAR(5),\n" +
            "  IS_CHILD VARCHAR(5),\n" +
            "  CKI_TYPE VARCHAR(5),\n" +
            "  PROC_AGENT VARCHAR(20),\n" +
            "  CKI_DATE VARCHAR(20),\n" +
            "  INFANT_NAME VARCHAR(50),\n" +
            "  RUSH_LUGGAGE VARCHAR(100),\n" +
            "  DRY_BAT_WHEELCHAIR VARCHAR(100),\n" +
            "  WET_BAT_WHEELCHAIR VARCHAR(100),\n" +
            "  ELEC_BAT_WHEELCHAIR VARCHAR(100),\n" +
            "  ONBOARD_WHEELCHAIR VARCHAR(100),\n" +
            "  CKI_INFO VARCHAR(100),\n" +
            "  PREFER_SEAT VARCHAR(20),\n" +
            "  FFP_VALID_FAIL VARCHAR(20),\n" +
            "  INCABIN_LITTLE_ANIMAL_WITHCAGE VARCHAR(100),\n" +
            "  INCABIN_LITTLE_ANIMAL_NOCAGE VARCHAR(100),\n" +
            "  ALLIANCE_SENIOR_TIER_LEVEL VARCHAR(20),\n" +
            "  BLACK_LIST_TAG VARCHAR(20),\n" +
            "  AEC_REDISTRIBUTE_SEAT VARCHAR(20),\n" +
            "  AEC_REBOOK_SEAT VARCHAR(20),\n" +
            "  CANCEL_CHANGE_PORT VARCHAR(20),\n" +
            "  STAND_BY_PSR VARCHAR(20),\n" +
            "  SEAT_OCCUPIED VARCHAR(20),\n" +
            "  ASR_SEAT VARCHAR(20),\n" +
            "  SSR_MAAS VARCHAR(20),\n" +
            "  SSR_MEDA VARCHAR(20),\n" +
            "  SSR_LANG VARCHAR(20),\n" +
            "  SSR_DPNA VARCHAR(20),\n" +
            "  SSR_SPEQ VARCHAR(20),\n" +
            "  SSR_PPOC VARCHAR(20),\n" +
            "  SSR_UMPG VARCHAR(20),\n" +
            "  SSR_UMBD VARCHAR(20),\n" +
            "  SSR_UMDF VARCHAR(20),\n" +
            "  SSR_UMLG VARCHAR(20),\n" +
            "  SSR_SPBG VARCHAR(20),\n" +
            "  SSR_YFPB VARCHAR(20),\n" +
            "  SSR_WCON VARCHAR(20),\n" +
            "  SSR_UMAD VARCHAR(20),\n" +
            "  ACTION_CODE VARCHAR(5),\n" +
            "  ID_TYPE VARCHAR(50),\n" +
            "  ID_INFO VARCHAR(4000),\n" +
            "  MC_FLT VARCHAR(10),\n" +
            "  PRE_SEG VARCHAR(50),\n" +
            "  NEXT_SEG VARCHAR(50),\n" +
            "  IO_FLAG VARCHAR(20),\n" +
            "  EDI_FLAG VARCHAR(5),\n" +
            "  ET_SEG VARCHAR(5),\n" +
            "  FARE VARCHAR(20),\n" +
            "  PSR_TYPE VARCHAR(10),\n" +
            "  CKI_NUM VARCHAR(1000),\n" +
            "  DOWN_GRADE VARCHAR(10),\n" +
            "  AVIH_BAGS VARCHAR(20),\n" +
            "  AVIH_BAGWGT VARCHAR(10),\n" +
            "  AVIH_BAG_TAG VARCHAR(1000),\n" +
            "  COM_UNIQUE_STR VARCHAR(200),\n" +
            "  FLT_SEG_COM_UNIQUE_STR VARCHAR(200),\n" +
            "  ISTRANSITSEND VARCHAR(5),\n" +
            "  PRINT_TICKET_TIME TIMESTAMP(6),\n" +
            "  CHECK_IN_TIME TIMESTAMP(6),\n" +
            "  ID_CARD_AGE VARCHAR(5),\n" +
            "  ID_CARD_TYPE VARCHAR(3),\n" +
            "  ID_CARD_NATIVE VARCHAR(5),\n" +
            "  MOBILE_PHONE VARCHAR(4000),\n" +
            "  MOBILE_PHONE_SOURCE VARCHAR(50),\n" +
            "  BOOK_AGT VARCHAR(50),\n" +
            "  BOOK_OFFI VARCHAR(50),\n" +
            "  IS_EXCHANGE_CARD VARCHAR(1),\n" +
            "  ID_SEQ BIGINT,\n" +
            "  BKO_UPTIME TIMESTAMP(6),\n" +
            "  PNR_GROUP_ID VARCHAR(50),\n" +
            "  LKT_IDX BIGINT,\n" +
            "  LKT_STATUS VARCHAR(20),\n" +
            "  MOBILES VARCHAR(4000),\n" +
            "  TELEPHONE VARCHAR(4000),\n" +
            "  LKT_UPTIME TIMESTAMP(6),\n" +
            "  CKI_STATUS VARCHAR(6),\n" +
            "  COUPON_STATUS VARCHAR(6),\n" +
            "  ORIGINALTKNE VARCHAR(20),\n" +
            "  MARKGP VARCHAR(3),\n" +
            "  BIG_CUTNUM VARCHAR(10),\n" +
            "  EVENT VARCHAR(40),\n" +
            "  INF_ET_NUM VARCHAR(20),\n" +
            "  FLT_NUM_SELECT VARCHAR(10),\n" +
            "  LKT_XR VARCHAR(4),\n" +
            "  LKTXR_UPTM TIMESTAMP(6),\n" +
            "  GDS_CODE VARCHAR(6),\n" +
            "  FBA_MAX_PIECES VARCHAR(6),\n" +
            "  FBA_TOTAL_WEIGHT VARCHAR(6),\n" +
            "  TKNEGP VARCHAR(3),\n" +
            "  DCS_DPTM VARCHAR(10),\n" +
            "  DCS_ATTM VARCHAR(10),\n" +
            "  PNR_SELL_CLASS VARCHAR(2),\n" +
            "  PAX_FFP VARCHAR(20),\n" +
            "  IS_GROUP VARCHAR(2),\n" +
            "  GROUP_NAME VARCHAR(20),\n" +
            "  PASSENGER_NUMBER VARCHAR(6),\n" +
            "  DCS_SELL_CLASS VARCHAR(2),\n" +
            "  ISSUE_AGENT VARCHAR(10),\n" +
            "  ISSUE_OFFI VARCHAR(10),\n" +
            "  PHONE_FLG BIGINT,\n" +
            "  PHONE_FLG_DATE TIMESTAMP(6),\n" +
            "  DCS_ATDATE TIMESTAMP(6),\n" +
            "  ORIG_SUB_CLASS VARCHAR(100),\n" +
            "  TICKET_TYPE VARCHAR(100),\n" +
            "  IATA_CODE VARCHAR(100),\n" +
            "  ISSUE_COUNTRY VARCHAR(100),\n" +
            "  SSR_INFO STRING,\n" +
            "  CKI_CITY VARCHAR(100),\n" +
            "  TCHB VARCHAR(100),\n" +
            "  PETC_BAGS VARCHAR(100),\n" +
            "  PETC_BAGWGT VARCHAR(100),\n" +
            "  PETC_BAG_TAG VARCHAR(100),\n" +
            "  IS_CHILDCRADLE VARCHAR(16),\n" +
            "  IS_CHILDPASSENGER VARCHAR(16),\n" +
            "  IS_DEPA VARCHAR(16),\n" +
            "  IS_DEPU VARCHAR(16),\n" +
            "  IS_DNPA VARCHAR(16),\n" +
            "  IS_DPNA VARCHAR(16),\n" +
            "  IS_INAD VARCHAR(16),\n" +
            "  IS_LEGRORLEGL VARCHAR(16),\n" +
            "  IS_INF VARCHAR(16),\n" +
            "  IS_MEDA VARCHAR(16),\n" +
            "  IS_HELPPASSENGER VARCHAR(16),\n" +
            "  IS_SVAN VARCHAR(16),\n" +
            "  IS_CABINDOG VARCHAR(16),\n" +
            "  IS_PPOC_PSG VARCHAR(1),\n" +
            "  RESERVE_SEAT_NO VARCHAR(30),\n" +
            "  BAG_TIME TIMESTAMP(6),\n" +
            "  BAG_AGENT VARCHAR(10),\n" +
            "  FARE_BASIS_CODE VARCHAR(100),\n" +
            "  IS_PETC VARCHAR(2),\n" +
            "  EMAIL VARCHAR(500)" +
            ") WITH (\n" +
            GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_LYGJ_DEPART_PASSENGER_INFO_BAK1", Constants.ODS_USER, Constants.ODS_PWD)

            + ")";

    public static String COMPLAINT_INFO_ACTIVITI = "CREATE TABLE T_ODS_LYGJ_COMPLAINT_INFO_ACTIVITI (\n" +
            "    ID                           BIGINT,\n" +
            "    COMPLAINT_SERIAL_NUM         varchar(300),\n" +
            "    COMPLAINT_TITLE              varchar(1200),\n" +
            "    PAX_NAME                     varchar(300),\n" +
            "    IS_GROUP                     CHAR(30),\n" +
            "    SEX                          VARCHAR(30),\n" +
            "    TK_LUGGAGE                   VARCHAR(100),\n" +
            "    CARD_NUM                     VARCHAR(600),\n" +
            "    CARD_TYPE                    VARCHAR(30),\n" +
            "    PSG_TYPE                     VARCHAR(30),\n" +
            "    COMP_DUTY_FFP                VARCHAR(400),\n" +
            "    CABIN_CLASS                  VARCHAR(30),\n" +
            "    PHONENUM                     VARCHAR(100),\n" +
            "    ADDRESS                      VARCHAR(1200),\n" +
            "    EMAIL                        VARCHAR(200),\n" +
            "    MEMO                         VARCHAR(12000),\n" +
            "    FEED_NAME                    VARCHAR(200),\n" +
            "    FEED_PHONE                   VARCHAR(100),\n" +
            "    FLIGHT_NUM                   VARCHAR(30),\n" +
            "    FLIGHT_DATE                  TIMESTAMP(6),\n" +
            "    DEP_CITY                     VARCHAR(60),\n" +
            "    ARR_CITY                     VARCHAR(60),\n" +
            "    COMPLAINT_WAY                VARCHAR(180),\n" +
            "    COMPLAINT_ORI                VARCHAR(180),\n" +
            "    SUGGEST_TYPE                 BIGINT,\n" +
            "    SERVICE_TYPE                 BIGINT,\n" +
            "    SERVICE_TYPE_DETAIL          BIGINT,\n" +
            "    COMPLAINT_PRIORITY           BIGINT,\n" +
            "    COMPLAINT_CONTENT            VARCHAR(12000),\n" +
            "    COMPLAINT_SUPPLEMENT_CONTENT VARCHAR(12000),\n" +
            "    PSG_REQ                      VARCHAR(12000),\n" +
            "    SERVICES_REMIND              VARCHAR(3000),\n" +
            "    DEAL_DEPT                    VARCHAR(90),\n" +
            "    ASSIT_DEPT                   VARCHAR(1200),\n" +
            "    COMPLAINT_DATE               TIMESTAMP(6),\n" +
            "    COMPLAINT_STATE              BIGINT,\n" +
            "    ACCEPT_PEOPLE                VARCHAR(30),\n" +
            "    ACCEPT_DEPT                  VARCHAR(20),\n" +
            "    RECALL_TIME                  TIMESTAMP(6),\n" +
            "    PROCESS_ID                   VARCHAR(40),\n" +
            "    ATTACH_COMPLAINT             VARCHAR(12000),\n" +
            "    CLOSE_CONTENT                VARCHAR(6000),\n" +
            "    CLOSE_DATE                   TIMESTAMP(6),\n" +
            "    CLOSE_DAYS                   BIGINT,\n" +
            "    CLOSE_OP                     VARCHAR(30),\n" +
            "    VALID                        BIGINT,\n" +
            "    COMPLAINT_RESULT             VARCHAR(12000),\n" +
            "    ARCHIVE_VALID                VARCHAR(30),\n" +
            "    FIRST_DUTY_DEPT              VARCHAR(100),\n" +
            "    FIRST_DUTY_DEPT_SCORE        VARCHAR(100),\n" +
            "    INVOLVED_PERSON_ACCT         VARCHAR(1200),\n" +
            "    INVOLVED_PERSONNEL_NAME      VARCHAR(1200),\n" +
            "    UPDATE_PERSON                VARCHAR(50),\n" +
            "    UPDATE_DATE                  TIMESTAMP(6),\n" +
            "    RESULT_ATTACH                VARCHAR(1200),\n" +
            "    TICKET_CHANNEL               VARCHAR(30),\n" +
            "    CLOSE_ADDITIONAL_CONTENT     VARCHAR(3000),\n" +
            "    CLOSE_ADDITIONAL_DATE        TIMESTAMP(6),\n" +
            "    CLOSE_ADDITIONAL_OPERATOR    VARCHAR(90),\n" +
            "    CLOSE_ADDITIONAL_FILE        VARCHAR(900),\n" +
            "    COMMENT_SUMMARY              VARCHAR(12000),\n" +
            "    IS_CONVERT_MEDIATION         CHAR(1),\n" +
            "    BEFORE_COMPLAINT_ID          BIGINT,\n" +
            "    MEDIATE_FALG                 BIGINT,\n" +
            "    NOT_MEDIATE_REASON           VARCHAR(6000),\n" +
            "    MEDIATION_CLAIM              VARCHAR(6000),\n" +
            "    NEW_COMPLAINT_INFO           VARCHAR(30),\n" +
            "    ATTACH_RECOMMUNICATE         VARCHAR(12000),\n" +
            "    ETL_CREATE_TIME              TIMESTAMP(6),\n" +
            "    ETL_UPDATE_TIME              TIMESTAMP(6),\n" +
            "    ETL_DATE                     date" +
            ") WITH (\n" +
            GetTableSql.getDorisConnector(Constants.ODS_DB, "T_ODS_LYGJ_COMPLAINT_INFO_ACTIVITI", Constants.ODS_USER, Constants.ODS_PWD) +
            ")";
}
