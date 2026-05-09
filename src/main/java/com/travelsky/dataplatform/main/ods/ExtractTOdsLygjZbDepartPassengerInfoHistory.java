package com.travelsky.dataplatform.main.ods;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.udf.OracleAESDecryptor;
import com.travelsky.dataplatform.udf.OracleDESDecryptor;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import com.travelsky.dataplatform.utils.KerberosAuthUtils;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

/**
 * 鲁雁管家灾备库
 * 读取范围：2022-01-01~2023-12-31
 */
public class ExtractTOdsLygjZbDepartPassengerInfoHistory {
    private static final Logger logger = LoggerFactory.getLogger(ExtractTOdsLygjZbDepartPassengerInfoHistory.class);

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        logger.info("etl_date:" + etlDate);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint设置
        CheckpointUtils.setCheckpoint(env, "ExtractTOdsLygjZbDepartPassengerInfoHistory");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        // 注册DES解密UDF
        tEnv.createTemporarySystemFunction("des_decrypt", OracleDESDecryptor.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        //创建上游ORACLE数据源表
        tEnv.executeSql("CREATE TABLE DEPART_PASSENGER_INFO_HISTORY (\n" +
                "  IDX BIGINT,\n" +
                "  PSG_NAME_EN VARCHAR(300),\n" +
                "  PSG_NAME_CN VARCHAR(300),\n" +
                "  FLT_SEG_IDS BIGINT,\n" +
                "  FLT_NUM VARCHAR(30),\n" +
                "  FLT_DATE TIMESTAMP(6),\n" +
                "  ORIG_SEQ VARCHAR(6),\n" +
                "  DEST_SEQ VARCHAR(6),\n" +
                "  PNR_ICS VARCHAR(30),\n" +
                "  PNR_CRS VARCHAR(60),\n" +
                "  MAIN_CLASS VARCHAR(6),\n" +
                "  SELL_CLASS VARCHAR(6),\n" +
                "  FFP VARCHAR(150),\n" +
                "  ET VARCHAR(12000),\n" +
                "  ET_NUM VARCHAR(60),\n" +
                "  ET_ID_NUM VARCHAR(12000),\n" +
                "  ID_NUM VARCHAR(12000),\n" +
                "  IS_VIP VARCHAR(6),\n" +
                "  IS_CIP VARCHAR(6),\n" +
                "  IS_BLND VARCHAR(6),\n" +
                "  IS_BSCT VARCHAR(6),\n" +
                "  IS_GOSHOW VARCHAR(6),\n" +
                "  IS_DEAF VARCHAR(6),\n" +
                "  IS_INFANT VARCHAR(6),\n" +
                "  IS_SPML VARCHAR(6),\n" +
                "  SPML_ITEM VARCHAR(300),\n" +
                "  EXST VARCHAR(60),\n" +
                "  IS_STCR VARCHAR(6),\n" +
                "  WCHR_TYPE VARCHAR(15),\n" +
                "  GROUP_ID VARCHAR(150),\n" +
                "  RECHECK_TO VARCHAR(60),\n" +
                "  RECHECK_FROM VARCHAR(60),\n" +
                "  IS_XRES VARCHAR(6),\n" +
                "  GENDER VARCHAR(9),\n" +
                "  SEAT_NO VARCHAR(60),\n" +
                "  STATUS VARCHAR(12),\n" +
                "  BAGS VARCHAR(15),\n" +
                "  BAGWHT VARCHAR(30),\n" +
                "  MSG VARCHAR(1200),\n" +
                "  PSM VARCHAR(3000),\n" +
                "  PIL VARCHAR(300),\n" +
                "  PCTC VARCHAR(1500),\n" +
                "  PSPT VARCHAR(12000),\n" +
                "  HOSTNBR VARCHAR(15),\n" +
                "  BRD_NO VARCHAR(15),\n" +
                "  CKI_TIME VARCHAR(15),\n" +
                "  CKI_AGENT VARCHAR(24),\n" +
                "  CKI_OFFICE VARCHAR(30),\n" +
                "  CKI_PID VARCHAR(24),\n" +
                "  SIP VARCHAR(30),\n" +
                "  UD_GRADE VARCHAR(6),\n" +
                "  VUD_GRADE VARCHAR(6),\n" +
                "  UD_CLASS VARCHAR(6),\n" +
                "  OFL VARCHAR(6),\n" +
                "  DEP VARCHAR(6),\n" +
                "  EDI_IN VARCHAR(6),\n" +
                "  EDI_OUT VARCHAR(6),\n" +
                "  INBOUND VARCHAR(768),\n" +
                "  OUTBOUND VARCHAR(768),\n" +
                "  EDIWARN VARCHAR(300),\n" +
                "  EDIMARK VARCHAR(6),\n" +
                "  RELATION VARCHAR(12000),\n" +
                "  BAGTAG VARCHAR(6000),\n" +
                "  FILE_NAME VARCHAR(300),\n" +
                "  ORIG_CNNAME VARCHAR(300),\n" +
                "  IN_FLT_NUM VARCHAR(30),\n" +
                "  IN_FLT_DATE TIMESTAMP(6),\n" +
                "  IN_CLASS VARCHAR(3),\n" +
                "  IN_CITY VARCHAR(9),\n" +
                "  IN_BN VARCHAR(18),\n" +
                "  IN_SEAT VARCHAR(18),\n" +
                "  OUT_FLT_NUM VARCHAR(30),\n" +
                "  OUT_FLT_DATE TIMESTAMP(6),\n" +
                "  OUT_CLASS VARCHAR(3),\n" +
                "  OUT_CITY VARCHAR(9),\n" +
                "  RESERVED_SEAT VARCHAR(60),\n" +
                "  ORIG VARCHAR(9),\n" +
                "  DEST VARCHAR(9),\n" +
                "  IS_FFP_JK VARCHAR(6),\n" +
                "  TRA_FLT_IDS BIGINT,\n" +
                "  BIRTHDAYONFLT VARCHAR(6),\n" +
                "  SPT_ID BIGINT,\n" +
                "  IS_UM VARCHAR(60),\n" +
                "  UPDATE_TIME TIMESTAMP(6),\n" +
                "  UPDATE_USER VARCHAR(90),\n" +
                "  VERIFY_STATUS VARCHAR(90),\n" +
                "  SPECIAL_TYPE VARCHAR(48),\n" +
                "  CREATE_TIME TIMESTAMP(6),\n" +
                "  BIRTHDAY VARCHAR(30),\n" +
                "  RECORD_LAST_UPDATE_TIME TIMESTAMP(6),\n" +
                "  RECORD_CREATE_TIME TIMESTAMP(6),\n" +
                "  STAR_FFP VARCHAR(30),\n" +
                "  STAR_FFP_NO VARCHAR(150),\n" +
                "  LAST_FQTV_LEVEL VARCHAR(300),\n" +
                "  TEL_NUM VARCHAR(12000),\n" +
                "  SUID VARCHAR(1500),\n" +
                "  FQTV_DETAIL_LEVEL VARCHAR(300),\n" +
                "  MDM_GENDER CHAR(1),\n" +
                "  CERT_NO VARCHAR(12000),\n" +
                "  SPECIAL_SERVICE VARCHAR(30),\n" +
                "  SPECIAL_TAG VARCHAR(30),\n" +
                "  IS_VVIP VARCHAR(15),\n" +
                "  IS_CHILD VARCHAR(15),\n" +
                "  CKI_TYPE VARCHAR(15),\n" +
                "  PROC_AGENT VARCHAR(60),\n" +
                "  CKI_DATE VARCHAR(60),\n" +
                "  INFANT_NAME VARCHAR(150),\n" +
                "  RUSH_LUGGAGE VARCHAR(300),\n" +
                "  DRY_BAT_WHEELCHAIR VARCHAR(300),\n" +
                "  WET_BAT_WHEELCHAIR VARCHAR(300),\n" +
                "  ELEC_BAT_WHEELCHAIR VARCHAR(300),\n" +
                "  ONBOARD_WHEELCHAIR VARCHAR(300),\n" +
                "  CKI_INFO VARCHAR(300),\n" +
                "  PREFER_SEAT VARCHAR(60),\n" +
                "  FFP_VALID_FAIL VARCHAR(60),\n" +
                "  INCABIN_LITTLE_ANIMAL_WITHCAGE VARCHAR(300),\n" +
                "  INCABIN_LITTLE_ANIMAL_NOCAGE VARCHAR(300),\n" +
                "  ALLIANCE_SENIOR_TIER_LEVEL VARCHAR(60),\n" +
                "  BLACK_LIST_TAG VARCHAR(60),\n" +
                "  AEC_REDISTRIBUTE_SEAT VARCHAR(60),\n" +
                "  AEC_REBOOK_SEAT VARCHAR(60),\n" +
                "  CANCEL_CHANGE_PORT VARCHAR(60),\n" +
                "  STAND_BY_PSR VARCHAR(60),\n" +
                "  SEAT_OCCUPIED VARCHAR(60),\n" +
                "  ASR_SEAT VARCHAR(60),\n" +
                "  SSR_MAAS VARCHAR(60),\n" +
                "  SSR_MEDA VARCHAR(60),\n" +
                "  SSR_LANG VARCHAR(60),\n" +
                "  SSR_DPNA VARCHAR(60),\n" +
                "  SSR_SPEQ VARCHAR(60),\n" +
                "  SSR_PPOC VARCHAR(60),\n" +
                "  SSR_UMPG VARCHAR(60),\n" +
                "  SSR_UMBD VARCHAR(60),\n" +
                "  SSR_UMDF VARCHAR(60),\n" +
                "  SSR_UMLG VARCHAR(60),\n" +
                "  SSR_SPBG VARCHAR(60),\n" +
                "  SSR_YFPB VARCHAR(60),\n" +
                "  SSR_WCON VARCHAR(60),\n" +
                "  SSR_UMAD VARCHAR(60),\n" +
                "  ACTION_CODE VARCHAR(15),\n" +
                "  ID_TYPE VARCHAR(150),\n" +
                "  ID_INFO VARCHAR(12000),\n" +
                "  MC_FLT VARCHAR(30),\n" +
                "  PRE_SEG VARCHAR(150),\n" +
                "  NEXT_SEG VARCHAR(150),\n" +
                "  IO_FLAG VARCHAR(60),\n" +
                "  EDI_FLAG VARCHAR(15),\n" +
                "  ET_SEG VARCHAR(15),\n" +
                "  FARE VARCHAR(60),\n" +
                "  PSR_TYPE VARCHAR(30),\n" +
                "  CKI_NUM VARCHAR(3000),\n" +
                "  DOWN_GRADE VARCHAR(30),\n" +
                "  AVIH_BAGS VARCHAR(60),\n" +
                "  AVIH_BAGWGT VARCHAR(30),\n" +
                "  AVIH_BAG_TAG VARCHAR(3000),\n" +
                "  COM_UNIQUE_STR VARCHAR(600),\n" +
                "  FLT_SEG_COM_UNIQUE_STR VARCHAR(600),\n" +
                "  ISTRANSITSEND VARCHAR(15),\n" +
                "  PRINT_TICKET_TIME TIMESTAMP(6),\n" +
                "  CHECK_IN_TIME TIMESTAMP(6),\n" +
                "  ID_CARD_AGE VARCHAR(15),\n" +
                "  ID_CARD_TYPE VARCHAR(9),\n" +
                "  ID_CARD_NATIVE VARCHAR(15),\n" +
                "  MOBILE_PHONE VARCHAR(12000),\n" +
                "  MOBILE_PHONE_SOURCE VARCHAR(150),\n" +
                "  BOOK_AGT VARCHAR(150),\n" +
                "  BOOK_OFFI VARCHAR(150),\n" +
                "  IS_EXCHANGE_CARD VARCHAR(3),\n" +
                "  ID_SEQ BIGINT,\n" +
                "  BKO_UPTIME TIMESTAMP(6),\n" +
                "  PNR_GROUP_ID VARCHAR(150),\n" +
                "  LKT_IDX BIGINT,\n" +
                "  LKT_STATUS VARCHAR(60),\n" +
                "  MOBILES VARCHAR(12000),\n" +
                "  TELEPHONE VARCHAR(12000),\n" +
                "  LKT_UPTIME TIMESTAMP(6),\n" +
                "  CKI_STATUS VARCHAR(18),\n" +
                "  COUPON_STATUS VARCHAR(18),\n" +
                "  ORIGINALTKNE VARCHAR(60),\n" +
                "  MARKGP VARCHAR(9),\n" +
                "  BIG_CUTNUM VARCHAR(30),\n" +
                "  EVENT VARCHAR(120),\n" +
                "  INF_ET_NUM VARCHAR(60),\n" +
                "  FLT_NUM_SELECT VARCHAR(30),\n" +
                "  LKT_XR VARCHAR(12),\n" +
                "  LKTXR_UPTM TIMESTAMP(6),\n" +
                "  GDS_CODE VARCHAR(18),\n" +
                "  FBA_MAX_PIECES VARCHAR(18),\n" +
                "  FBA_TOTAL_WEIGHT VARCHAR(18),\n" +
                "  TKNEGP VARCHAR(9),\n" +
                "  DCS_DPTM VARCHAR(30),\n" +
                "  DCS_ATTM VARCHAR(30),\n" +
                "  PNR_SELL_CLASS VARCHAR(6),\n" +
                "  PAX_FFP VARCHAR(60),\n" +
                "  IS_GROUP VARCHAR(6),\n" +
                "  GROUP_NAME VARCHAR(60),\n" +
                "  PASSENGER_NUMBER VARCHAR(18),\n" +
                "  DCS_SELL_CLASS VARCHAR(6),\n" +
                "  ISSUE_AGENT VARCHAR(30),\n" +
                "  ISSUE_OFFI VARCHAR(30),\n" +
                "  PHONE_FLG BIGINT,\n" +
                "  PHONE_FLG_DATE TIMESTAMP(6),\n" +
                "  DCS_ATDATE TIMESTAMP(6),\n" +
                "  ORIG_SUB_CLASS VARCHAR(300),\n" +
                "  TICKET_TYPE VARCHAR(300),\n" +
                "  IATA_CODE VARCHAR(300),\n" +
                "  ISSUE_COUNTRY VARCHAR(300),\n" +
                "  SSR_INFO STRING,\n" +
                "  CKI_CITY VARCHAR(300),\n" +
                "  TCHB VARCHAR(300),\n" +
                "  PETC_BAGS VARCHAR(300),\n" +
                "  PETC_BAGWGT VARCHAR(300),\n" +
                "  PETC_BAG_TAG VARCHAR(300),\n" +
                "  IS_DEPA VARCHAR(48),\n" +
                "  IS_DEPU VARCHAR(48),\n" +
                "  IS_DNPA VARCHAR(48),\n" +
                "  IS_DPNA VARCHAR(48),\n" +
                "  IS_INAD VARCHAR(48),\n" +
                "  IS_LEGRORLEGL VARCHAR(48),\n" +
                "  IS_MEDA VARCHAR(48),\n" +
                "  IS_SVAN VARCHAR(48),\n" +
                "  IS_PPOC_PSG VARCHAR(3),\n" +
                "  BAG_TIME TIMESTAMP(6),\n" +
                "  BAG_AGENT VARCHAR(30)" +
                //") WITH (\n" +
                //"    'connector' = 'jdbc',\n" +
                //"    'url' = 'jdbc:oracle:thin:@//10.31.0.5:50081/testgbk16',\n" +
                //"    'table-name' = 'sis_user_v3.DEPART_PASSENGER_INFO', \n" +
                //"    'username' = 'sis_user_v3_etl',\n" +
                //"    'password' = '519sisuserv3etl~'\n" +
                //",\n" + "  'driver' = 'oracle.jdbc.driver.OracleDriver'" +
                //")");
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@//" + Constants.LYGJ_ZB_IP + ":" + Constants.LYGJ_ZB_PORT + "/" + Constants.LYGJ_ZB_DB + "',\n" +
                "    'table-name' = '" + Constants.LYGJ_ZB_SCHEMA + ".DEPART_PASSENGER_INFO_HISTORY',\n" +
                "    'username' = '" + Constants.LYGJ_ZB_USER + "',\n" +
                "    'password' = '" + Constants.LYGJ_ZB_PWD + "'\n" + ",\n" +
                "    'driver' = '" + Constants.ORACLE_DRIVER + "'" +
                ")");
        //创建Doris目标表
        tEnv.executeSql("CREATE TABLE T_ODS_LYGJ_DEPART_PASSENGER_INFO (\n" +
                "  ETL_DATE DATE,\n" +
                "  IDX BIGINT,\n" +
                "  PSG_NAME_EN VARCHAR(300),\n" +
                "  PSG_NAME_CN VARCHAR(300),\n" +
                "  FLT_SEG_IDS BIGINT,\n" +
                "  FLT_NUM VARCHAR(30),\n" +
                "  FLT_DATE TIMESTAMP(6) ,\n" +
                "  ORIG_SEQ VARCHAR(6),\n" +
                "  DEST_SEQ VARCHAR(6),\n" +
                "  PNR_ICS VARCHAR(30),\n" +
                "  PNR_CRS VARCHAR(60),\n" +
                "  MAIN_CLASS VARCHAR(6),\n" +
                "  SELL_CLASS VARCHAR(6),\n" +
                "  FFP VARCHAR(150),\n" +
                "  ET VARCHAR(12000),\n" +
                "  ET_NUM VARCHAR(60),\n" +
                "  ET_ID_NUM VARCHAR(12000),\n" +
                "  ID_NUM VARCHAR(12000),\n" +
                "  IS_VIP VARCHAR(6),\n" +
                "  IS_CIP VARCHAR(6),\n" +
                "  IS_BLND VARCHAR(6),\n" +
                "  IS_BSCT VARCHAR(6),\n" +
                "  IS_GOSHOW VARCHAR(6),\n" +
                "  IS_DEAF VARCHAR(6),\n" +
                "  IS_INFANT VARCHAR(6),\n" +
                "  IS_SPML VARCHAR(6),\n" +
                "  SPML_ITEM VARCHAR(300),\n" +
                "  EXST VARCHAR(60),\n" +
                "  IS_STCR VARCHAR(6),\n" +
                "  WCHR_TYPE VARCHAR(15),\n" +
                "  GROUP_ID VARCHAR(150),\n" +
                "  RECHECK_TO VARCHAR(60),\n" +
                "  RECHECK_FROM VARCHAR(60),\n" +
                "  IS_XRES VARCHAR(6),\n" +
                "  GENDER VARCHAR(9),\n" +
                "  SEAT_NO VARCHAR(60),\n" +
                "  STATUS VARCHAR(12),\n" +
                "  BAGS VARCHAR(15),\n" +
                "  BAGWHT VARCHAR(30),\n" +
                "  MSG VARCHAR(1200),\n" +
                "  PSM VARCHAR(3000),\n" +
                "  PIL VARCHAR(300),\n" +
                "  PCTC VARCHAR(1500),\n" +
                "  PSPT VARCHAR(12000),\n" +
                "  HOSTNBR VARCHAR(15),\n" +
                "  BRD_NO VARCHAR(15),\n" +
                "  CKI_TIME VARCHAR(15),\n" +
                "  CKI_AGENT VARCHAR(24),\n" +
                "  CKI_OFFICE VARCHAR(30),\n" +
                "  CKI_PID VARCHAR(24),\n" +
                "  SIP VARCHAR(30),\n" +
                "  UD_GRADE VARCHAR(6),\n" +
                "  VUD_GRADE VARCHAR(6),\n" +
                "  UD_CLASS VARCHAR(6),\n" +
                "  OFL VARCHAR(6),\n" +
                "  DEP VARCHAR(6),\n" +
                "  EDI_IN VARCHAR(6),\n" +
                "  EDI_OUT VARCHAR(6),\n" +
                "  INBOUND VARCHAR(768),\n" +
                "  OUTBOUND VARCHAR(768),\n" +
                "  EDIWARN VARCHAR(300),\n" +
                "  EDIMARK VARCHAR(6),\n" +
                "  RELATION VARCHAR(12000),\n" +
                "  BAGTAG VARCHAR(6000),\n" +
                "  FILE_NAME VARCHAR(300),\n" +
                "  ORIG_CNNAME VARCHAR(300),\n" +
                "  IN_FLT_NUM VARCHAR(30),\n" +
                "  IN_FLT_DATE TIMESTAMP(6),\n" +
                "  IN_CLASS VARCHAR(3),\n" +
                "  IN_CITY VARCHAR(9),\n" +
                "  IN_BN VARCHAR(18),\n" +
                "  IN_SEAT VARCHAR(18),\n" +
                "  OUT_FLT_NUM VARCHAR(30),\n" +
                "  OUT_FLT_DATE TIMESTAMP(6),\n" +
                "  OUT_CLASS VARCHAR(3),\n" +
                "  OUT_CITY VARCHAR(9),\n" +
                "  RESERVED_SEAT VARCHAR(60),\n" +
                "  ORIG VARCHAR(9),\n" +
                "  DEST VARCHAR(9),\n" +
                "  IS_FFP_JK VARCHAR(6),\n" +
                "  TRA_FLT_IDS BIGINT,\n" +
                "  BIRTHDAYONFLT VARCHAR(6),\n" +
                "  SPT_ID BIGINT,\n" +
                "  IS_UM VARCHAR(60),\n" +
                "  UPDATE_TIME TIMESTAMP(6),\n" +
                "  UPDATE_USER VARCHAR(90),\n" +
                "  VERIFY_STATUS VARCHAR(90),\n" +
                "  SPECIAL_TYPE VARCHAR(48),\n" +
                "  CREATE_TIME TIMESTAMP(6),\n" +
                "  BIRTHDAY VARCHAR(30),\n" +
                "  RECORD_LAST_UPDATE_TIME TIMESTAMP(6),\n" +
                "  RECORD_CREATE_TIME TIMESTAMP(6),\n" +
                "  STAR_FFP VARCHAR(30),\n" +
                "  STAR_FFP_NO VARCHAR(150),\n" +
                "  LAST_FQTV_LEVEL VARCHAR(300),\n" +
                "  TEL_NUM VARCHAR(12000),\n" +
                "  SUID VARCHAR(1500),\n" +
                "  FQTV_DETAIL_LEVEL VARCHAR(300),\n" +
                "  MDM_GENDER CHAR(1),\n" +
                "  CERT_NO VARCHAR(12000),\n" +
                "  SPECIAL_SERVICE VARCHAR(30),\n" +
                "  SPECIAL_TAG VARCHAR(30),\n" +
                "  IS_VVIP VARCHAR(15),\n" +
                "  IS_CHILD VARCHAR(15),\n" +
                "  CKI_TYPE VARCHAR(15),\n" +
                "  PROC_AGENT VARCHAR(60),\n" +
                "  CKI_DATE VARCHAR(60),\n" +
                "  INFANT_NAME VARCHAR(150),\n" +
                "  RUSH_LUGGAGE VARCHAR(300),\n" +
                "  DRY_BAT_WHEELCHAIR VARCHAR(300),\n" +
                "  WET_BAT_WHEELCHAIR VARCHAR(300),\n" +
                "  ELEC_BAT_WHEELCHAIR VARCHAR(300),\n" +
                "  ONBOARD_WHEELCHAIR VARCHAR(300),\n" +
                "  CKI_INFO VARCHAR(300),\n" +
                "  PREFER_SEAT VARCHAR(60),\n" +
                "  FFP_VALID_FAIL VARCHAR(60),\n" +
                "  INCABIN_LITTLE_ANIMAL_WITHCAGE VARCHAR(300),\n" +
                "  INCABIN_LITTLE_ANIMAL_NOCAGE VARCHAR(300),\n" +
                "  ALLIANCE_SENIOR_TIER_LEVEL VARCHAR(60),\n" +
                "  BLACK_LIST_TAG VARCHAR(60),\n" +
                "  AEC_REDISTRIBUTE_SEAT VARCHAR(60),\n" +
                "  AEC_REBOOK_SEAT VARCHAR(60),\n" +
                "  CANCEL_CHANGE_PORT VARCHAR(60),\n" +
                "  STAND_BY_PSR VARCHAR(60),\n" +
                "  SEAT_OCCUPIED VARCHAR(60),\n" +
                "  ASR_SEAT VARCHAR(60),\n" +
                "  SSR_MAAS VARCHAR(60),\n" +
                "  SSR_MEDA VARCHAR(60),\n" +
                "  SSR_LANG VARCHAR(60),\n" +
                "  SSR_DPNA VARCHAR(60),\n" +
                "  SSR_SPEQ VARCHAR(60),\n" +
                "  SSR_PPOC VARCHAR(60),\n" +
                "  SSR_UMPG VARCHAR(60),\n" +
                "  SSR_UMBD VARCHAR(60),\n" +
                "  SSR_UMDF VARCHAR(60),\n" +
                "  SSR_UMLG VARCHAR(60),\n" +
                "  SSR_SPBG VARCHAR(60),\n" +
                "  SSR_YFPB VARCHAR(60),\n" +
                "  SSR_WCON VARCHAR(60),\n" +
                "  SSR_UMAD VARCHAR(60),\n" +
                "  ACTION_CODE VARCHAR(15),\n" +
                "  ID_TYPE VARCHAR(150),\n" +
                "  ID_INFO VARCHAR(12000),\n" +
                "  MC_FLT VARCHAR(30),\n" +
                "  PRE_SEG VARCHAR(150),\n" +
                "  NEXT_SEG VARCHAR(150),\n" +
                "  IO_FLAG VARCHAR(60),\n" +
                "  EDI_FLAG VARCHAR(15),\n" +
                "  ET_SEG VARCHAR(15),\n" +
                "  FARE VARCHAR(60),\n" +
                "  PSR_TYPE VARCHAR(30),\n" +
                "  CKI_NUM VARCHAR(3000),\n" +
                "  DOWN_GRADE VARCHAR(30),\n" +
                "  AVIH_BAGS VARCHAR(60),\n" +
                "  AVIH_BAGWGT VARCHAR(30),\n" +
                "  AVIH_BAG_TAG VARCHAR(3000),\n" +
                "  COM_UNIQUE_STR VARCHAR(600),\n" +
                "  FLT_SEG_COM_UNIQUE_STR VARCHAR(600),\n" +
                "  ISTRANSITSEND VARCHAR(15),\n" +
                "  PRINT_TICKET_TIME TIMESTAMP(6),\n" +
                "  CHECK_IN_TIME TIMESTAMP(6),\n" +
                "  ID_CARD_AGE VARCHAR(15),\n" +
                "  ID_CARD_TYPE VARCHAR(9),\n" +
                "  ID_CARD_NATIVE VARCHAR(15),\n" +
                "  MOBILE_PHONE VARCHAR(12000),\n" +
                "  MOBILE_PHONE_SOURCE VARCHAR(150),\n" +
                "  BOOK_AGT VARCHAR(150),\n" +
                "  BOOK_OFFI VARCHAR(150),\n" +
                "  IS_EXCHANGE_CARD VARCHAR(3),\n" +
                "  ID_SEQ BIGINT,\n" +
                "  BKO_UPTIME TIMESTAMP(6),\n" +
                "  PNR_GROUP_ID VARCHAR(150),\n" +
                "  LKT_IDX BIGINT,\n" +
                "  LKT_STATUS VARCHAR(60),\n" +
                "  MOBILES VARCHAR(12000),\n" +
                "  TELEPHONE VARCHAR(12000),\n" +
                "  LKT_UPTIME TIMESTAMP(6),\n" +
                "  CKI_STATUS VARCHAR(18),\n" +
                "  COUPON_STATUS VARCHAR(18),\n" +
                "  ORIGINALTKNE VARCHAR(60),\n" +
                "  MARKGP VARCHAR(9),\n" +
                "  BIG_CUTNUM VARCHAR(30),\n" +
                "  EVENT VARCHAR(120),\n" +
                "  INF_ET_NUM VARCHAR(60),\n" +
                "  FLT_NUM_SELECT VARCHAR(30),\n" +
                "  LKT_XR VARCHAR(12),\n" +
                "  LKTXR_UPTM TIMESTAMP(6),\n" +
                "  GDS_CODE VARCHAR(18),\n" +
                "  FBA_MAX_PIECES VARCHAR(18),\n" +
                "  FBA_TOTAL_WEIGHT VARCHAR(18),\n" +
                "  TKNEGP VARCHAR(9),\n" +
                "  DCS_DPTM VARCHAR(30),\n" +
                "  DCS_ATTM VARCHAR(30),\n" +
                "  PNR_SELL_CLASS VARCHAR(6),\n" +
                "  PAX_FFP VARCHAR(60),\n" +
                "  IS_GROUP VARCHAR(6),\n" +
                "  GROUP_NAME VARCHAR(60),\n" +
                "  PASSENGER_NUMBER VARCHAR(18),\n" +
                "  DCS_SELL_CLASS VARCHAR(6),\n" +
                "  ISSUE_AGENT VARCHAR(30),\n" +
                "  ISSUE_OFFI VARCHAR(30),\n" +
                "  PHONE_FLG BIGINT,\n" +
                "  PHONE_FLG_DATE TIMESTAMP(6),\n" +
                "  DCS_ATDATE TIMESTAMP(6),\n" +
                "  ORIG_SUB_CLASS VARCHAR(300),\n" +
                "  TICKET_TYPE VARCHAR(300),\n" +
                "  IATA_CODE VARCHAR(300),\n" +
                "  ISSUE_COUNTRY VARCHAR(300),\n" +
                "  SSR_INFO STRING,\n" +
                "  CKI_CITY VARCHAR(300),\n" +
                "  TCHB VARCHAR(300),\n" +
                "  PETC_BAGS VARCHAR(300),\n" +
                "  PETC_BAGWGT VARCHAR(300),\n" +
                "  PETC_BAG_TAG VARCHAR(300),\n" +
                "  IS_CHILDCRADLE VARCHAR(48),\n" +
                "  IS_CHILDPASSENGER VARCHAR(48),\n" +
                "  IS_DEPA VARCHAR(48),\n" +
                "  IS_DEPU VARCHAR(48),\n" +
                "  IS_DNPA VARCHAR(48),\n" +
                "  IS_DPNA VARCHAR(48),\n" +
                "  IS_INAD VARCHAR(48),\n" +
                "  IS_LEGRORLEGL VARCHAR(48),\n" +
                "  IS_INF VARCHAR(48),\n" +
                "  IS_MEDA VARCHAR(48),\n" +
                "  IS_HELPPASSENGER VARCHAR(48),\n" +
                "  IS_SVAN VARCHAR(48),\n" +
                "  IS_CABINDOG VARCHAR(48),\n" +
                "  IS_PPOC_PSG VARCHAR(3),\n" +
                "  RESERVE_SEAT_NO VARCHAR(90),\n" +
                "  BAG_TIME TIMESTAMP(6),\n" +
                "  BAG_AGENT VARCHAR(30),\n" +
                "  FARE_BASIS_CODE VARCHAR(300),\n" +
                "  IS_PETC VARCHAR(6),\n" +
                "  EMAIL VARCHAR(1500)" +
                //") WITH (\n" +
                //"    'connector' = 'jdbc',\n" +
                //"    'url' = 'jdbc:mysql://" + Constants.DORIS_IP + ":" + Constants.DORIS_PORT + "/" + Constants.ODS_DB + "',\n" +
                //"    'table-name' = 'T_ODS_LYGJ_DEPART_PASSENGER_INFO', -- 替换为实际的表名\n" +
                //"    'username' = '" + Constants.ODS_USER + "',\n" +
                //"    'password' = '" + Constants.ODS_PWD + "'\n" +
                //",\n" + "  'driver' = '"+Constants.MYSQL_DRIVER+"'" +
                //")");
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_LYGJ_DEPART_PASSENGER_INFO',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                " 'sink.properties.group_commit' = 'sync_mode'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")");
        String extractSql = "INSERT INTO T_ODS_LYGJ_DEPART_PASSENGER_INFO (" +
                "  ETL_DATE,\n" +
                "  IDX,\n" +
                "  PSG_NAME_EN,\n" +
                "  PSG_NAME_CN,\n" +
                "  FLT_SEG_IDS,\n" +
                "  FLT_NUM,\n" +
                "  FLT_DATE,\n" +
                "  ORIG_SEQ,\n" +
                "  DEST_SEQ,\n" +
                "  PNR_ICS,\n" +
                "  PNR_CRS,\n" +
                "  MAIN_CLASS,\n" +
                "  SELL_CLASS,\n" +
                "  FFP,\n" +
                "  ET,\n" +
                "  ET_NUM,\n" +
                "  ET_ID_NUM,\n" +
                "  ID_NUM,\n" +
                "  IS_VIP,\n" +
                "  IS_CIP,\n" +
                "  IS_BLND,\n" +
                "  IS_BSCT,\n" +
                "  IS_GOSHOW,\n" +
                "  IS_DEAF,\n" +
                "  IS_INFANT,\n" +
                "  IS_SPML,\n" +
                "  SPML_ITEM ,\n" +
                "  EXST ,\n" +
                "  IS_STCR,\n" +
                "  WCHR_TYPE ,\n" +
                "  GROUP_ID ,\n" +
                "  RECHECK_TO ,\n" +
                "  RECHECK_FROM ,\n" +
                "  IS_XRES,\n" +
                "  GENDER ,\n" +
                "  SEAT_NO ,\n" +
                "  STATUS ,\n" +
                "  BAGS ,\n" +
                "  BAGWHT ,\n" +
                "  MSG ,\n" +
                "  PSM ,\n" +
                "  PIL ,\n" +
                "  PCTC ,\n" +
                "  PSPT ,\n" +
                "  HOSTNBR ,\n" +
                "  BRD_NO ,\n" +
                "  CKI_TIME ,\n" +
                "  CKI_AGENT ,\n" +
                "  CKI_OFFICE ,\n" +
                "  CKI_PID ,\n" +
                "  SIP ,\n" +
                "  UD_GRADE,\n" +
                "  VUD_GRADE,\n" +
                "  UD_CLASS,\n" +
                "  OFL,\n" +
                "  DEP,\n" +
                "  EDI_IN,\n" +
                "  EDI_OUT,\n" +
                "  INBOUND ,\n" +
                "  OUTBOUND ,\n" +
                "  EDIWARN ,\n" +
                "  EDIMARK,\n" +
                "  RELATION ,\n" +
                "  BAGTAG ,\n" +
                "  FILE_NAME ,\n" +
                "  ORIG_CNNAME ,\n" +
                "  IN_FLT_NUM ,\n" +
                "  IN_FLT_DATE ,\n" +
                "  IN_CLASS ,\n" +
                "  IN_CITY ,\n" +
                "  IN_BN ,\n" +
                "  IN_SEAT ,\n" +
                "  OUT_FLT_NUM ,\n" +
                "  OUT_FLT_DATE ,\n" +
                "  OUT_CLASS ,\n" +
                "  OUT_CITY ,\n" +
                "  RESERVED_SEAT ,\n" +
                "  ORIG ,\n" +
                "  DEST ,\n" +
                "  IS_FFP_JK,\n" +
                "  TRA_FLT_IDS ,\n" +
                "  BIRTHDAYONFLT,\n" +
                "  SPT_ID ,\n" +
                "  IS_UM ,\n" +
                "  UPDATE_TIME ,\n" +
                "  UPDATE_USER ,\n" +
                "  VERIFY_STATUS ,\n" +
                "  SPECIAL_TYPE ,\n" +
                "  CREATE_TIME ,\n" +
                "  BIRTHDAY ,\n" +
                "  RECORD_LAST_UPDATE_TIME ,\n" +
                "  RECORD_CREATE_TIME ,\n" +
                "  STAR_FFP ,\n" +
                "  STAR_FFP_NO ,\n" +
                "  LAST_FQTV_LEVEL ,\n" +
                "  TEL_NUM ,\n" +
                "  SUID ,\n" +
                "  FQTV_DETAIL_LEVEL ,\n" +
                "  MDM_GENDER ,\n" +
                "  CERT_NO ,\n" +
                "  SPECIAL_SERVICE ,\n" +
                "  SPECIAL_TAG ,\n" +
                "  IS_VVIP ,\n" +
                "  IS_CHILD ,\n" +
                "  CKI_TYPE ,\n" +
                "  PROC_AGENT ,\n" +
                "  CKI_DATE ,\n" +
                "  INFANT_NAME ,\n" +
                "  RUSH_LUGGAGE ,\n" +
                "  DRY_BAT_WHEELCHAIR ,\n" +
                "  WET_BAT_WHEELCHAIR ,\n" +
                "  ELEC_BAT_WHEELCHAIR ,\n" +
                "  ONBOARD_WHEELCHAIR ,\n" +
                "  CKI_INFO ,\n" +
                "  PREFER_SEAT ,\n" +
                "  FFP_VALID_FAIL ,\n" +
                "  INCABIN_LITTLE_ANIMAL_WITHCAGE ,\n" +
                "  INCABIN_LITTLE_ANIMAL_NOCAGE ,\n" +
                "  ALLIANCE_SENIOR_TIER_LEVEL ,\n" +
                "  BLACK_LIST_TAG ,\n" +
                "  AEC_REDISTRIBUTE_SEAT ,\n" +
                "  AEC_REBOOK_SEAT ,\n" +
                "  CANCEL_CHANGE_PORT ,\n" +
                "  STAND_BY_PSR ,\n" +
                "  SEAT_OCCUPIED ,\n" +
                "  ASR_SEAT ,\n" +
                "  SSR_MAAS ,\n" +
                "  SSR_MEDA ,\n" +
                "  SSR_LANG ,\n" +
                "  SSR_DPNA ,\n" +
                "  SSR_SPEQ ,\n" +
                "  SSR_PPOC ,\n" +
                "  SSR_UMPG ,\n" +
                "  SSR_UMBD ,\n" +
                "  SSR_UMDF ,\n" +
                "  SSR_UMLG ,\n" +
                "  SSR_SPBG ,\n" +
                "  SSR_YFPB ,\n" +
                "  SSR_WCON ,\n" +
                "  SSR_UMAD ,\n" +
                "  ACTION_CODE ,\n" +
                "  ID_TYPE ,\n" +
                "  ID_INFO ,\n" +
                "  MC_FLT ,\n" +
                "  PRE_SEG ,\n" +
                "  NEXT_SEG ,\n" +
                "  IO_FLAG ,\n" +
                "  EDI_FLAG ,\n" +
                "  ET_SEG ,\n" +
                "  FARE ,\n" +
                "  PSR_TYPE ,\n" +
                "  CKI_NUM ,\n" +
                "  DOWN_GRADE ,\n" +
                "  AVIH_BAGS ,\n" +
                "  AVIH_BAGWGT ,\n" +
                "  AVIH_BAG_TAG,\n" +
                "  COM_UNIQUE_STR ,\n" +
                "  FLT_SEG_COM_UNIQUE_STR ,\n" +
                "  ISTRANSITSEND ,\n" +
                "  PRINT_TICKET_TIME ,\n" +
                "  CHECK_IN_TIME ,\n" +
                "  ID_CARD_AGE,\n" +
                "  ID_CARD_TYPE,\n" +
                "  ID_CARD_NATIVE,\n" +
                "  MOBILE_PHONE,\n" +
                "  MOBILE_PHONE_SOURCE,\n" +
                "  BOOK_AGT,\n" +
                "  BOOK_OFFI,\n" +
                "  IS_EXCHANGE_CARD,\n" +
                "  ID_SEQ,\n" +
                "  BKO_UPTIME,\n" +
                "  PNR_GROUP_ID,\n" +
                "  LKT_IDX,\n" +
                "  LKT_STATUS ,\n" +
                "  MOBILES,\n" +
                "  TELEPHONE,\n" +
                "  LKT_UPTIME,\n" +
                "  CKI_STATUS,\n" +
                "  COUPON_STATUS,\n" +
                "  ORIGINALTKNE ,\n" +
                "  MARKGP,\n" +
                "  BIG_CUTNUM,\n" +
                "  EVENT,\n" +
                "  INF_ET_NUM ,\n" +
                "  FLT_NUM_SELECT,\n" +
                "  GDS_CODE,\n" +
                "  FBA_MAX_PIECES,\n" +
                "  FBA_TOTAL_WEIGHT,\n" +
                "  TKNEGP,\n" +
                "  DCS_DPTM,\n" +
                "  DCS_ATTM,\n" +
                "  PNR_SELL_CLASS,\n" +
                "  PAX_FFP ,\n" +
                "  IS_GROUP,\n" +
                "  GROUP_NAME ,\n" +
                "  PASSENGER_NUMBER,\n" +
                "  DCS_SELL_CLASS,\n" +
                "  ISSUE_AGENT,\n" +
                "  ISSUE_OFFI,\n" +
                "  PHONE_FLG,\n" +
                "  PHONE_FLG_DATE,\n" +
                "  DCS_ATDATE,\n" +
                "  ORIG_SUB_CLASS,\n" +
                "  TICKET_TYPE,\n" +
                "  IATA_CODE,\n" +
                "  ISSUE_COUNTRY,\n" +
                "  SSR_INFO,\n" +
                "  CKI_CITY,\n" +
                "  TCHB,\n" +
                "  PETC_BAGS,\n" +
                "  PETC_BAGWGT,\n" +
                "  PETC_BAG_TAG,\n" +
                "  IS_DEPA ,\n" +
                "  IS_DEPU ,\n" +
                "  IS_DNPA ,\n" +
                "  IS_DPNA ,\n" +
                "  IS_INAD ,\n" +
                "  IS_LEGRORLEGL ,\n" +
                "  IS_MEDA ,\n" +
                "  IS_SVAN ,\n" +
                "  IS_PPOC_PSG,\n" +
                "  BAG_TIME,\n" +
                "  BAG_AGENT,\n" +
                "  LKTXR_UPTM,\n" +
                "  LKT_XR)  " +
                "SELECT CAST('" +
                etlDate +
                "' AS DATE) ETL_DATE,\n" +
                "  IDX,\n" +
                "  PSG_NAME_EN,\n" +
                "  PSG_NAME_CN,\n" +
                "  FLT_SEG_IDS,\n" +
                "  FLT_NUM,\n" +
                "  FLT_DATE,\n" +
                "  ORIG_SEQ,\n" +
                "  DEST_SEQ,\n" +
                "  PNR_ICS,\n" +
                "  PNR_CRS,\n" +
                "  MAIN_CLASS,\n" +
                "  SELL_CLASS,\n" +
                "  sm4_encrypt(FFP, '" + Constants.SM4_KEY + "') FFP, \n" +
                "  sm4_encrypt(des_decrypt(ET, '" + Constants.LYGJ_ZB_AES_KEY + "'), '" + Constants.SM4_KEY + "') ET, \n" +
                "  ET_NUM,\n" +
                "  sm4_encrypt(des_decrypt(ET_ID_NUM, '" + Constants.LYGJ_ZB_AES_KEY + "'), '" + Constants.SM4_KEY + "') ET_ID_NUM, \n" +
                "  sm4_encrypt(des_decrypt(ID_NUM, '" + Constants.LYGJ_ZB_AES_KEY + "'), '" + Constants.SM4_KEY + "') ID_NUM, \n" +
                "  IS_VIP,\n" +
                "  IS_CIP,\n" +
                "  IS_BLND,\n" +
                "  IS_BSCT,\n" +
                "  IS_GOSHOW,\n" +
                "  IS_DEAF,\n" +
                "  IS_INFANT,\n" +
                "  IS_SPML,\n" +
                "  SPML_ITEM ,\n" +
                "  EXST ,\n" +
                "  IS_STCR,\n" +
                "  WCHR_TYPE ,\n" +
                "  GROUP_ID ,\n" +
                "  RECHECK_TO ,\n" +
                "  RECHECK_FROM ,\n" +
                "  IS_XRES,\n" +
                "  GENDER ,\n" +
                "  SEAT_NO ,\n" +
                "  STATUS ,\n" +
                "  BAGS ,\n" +
                "  BAGWHT ,\n" +
                "  MSG ,\n" +
                "  PSM ,\n" +
                "  PIL ,\n" +
                "  PCTC ,\n" +
                "  sm4_encrypt(des_decrypt(PSPT, '" + Constants.LYGJ_ZB_AES_KEY + "'), '" + Constants.SM4_KEY + "') AS PSPT, " +
                "  HOSTNBR ,\n" +
                "  BRD_NO ,\n" +
                "  CKI_TIME ,\n" +
                "  CKI_AGENT ,\n" +
                "  CKI_OFFICE ,\n" +
                "  CKI_PID ,\n" +
                "  SIP ,\n" +
                "  UD_GRADE,\n" +
                "  VUD_GRADE,\n" +
                "  UD_CLASS,\n" +
                "  OFL,\n" +
                "  DEP,\n" +
                "  EDI_IN,\n" +
                "  EDI_OUT,\n" +
                "  INBOUND ,\n" +
                "  OUTBOUND ,\n" +
                "  EDIWARN ,\n" +
                "  EDIMARK,\n" +
                "  sm4_encrypt(des_decrypt(RELATION, '" + Constants.LYGJ_ZB_AES_KEY + "'), '" + Constants.SM4_KEY + "') RELATION, \n" +
                "  BAGTAG ,\n" +
                "  FILE_NAME ,\n" +
                "  ORIG_CNNAME ,\n" +
                "  IN_FLT_NUM ,\n" +
                "  IN_FLT_DATE ,\n" +
                "  IN_CLASS ,\n" +
                "  IN_CITY ,\n" +
                "  IN_BN ,\n" +
                "  IN_SEAT ,\n" +
                "  OUT_FLT_NUM ,\n" +
                "  OUT_FLT_DATE ,\n" +
                "  OUT_CLASS ,\n" +
                "  OUT_CITY ,\n" +
                "  RESERVED_SEAT ,\n" +
                "  ORIG ,\n" +
                "  DEST ,\n" +
                "  IS_FFP_JK,\n" +
                "  TRA_FLT_IDS ,\n" +
                "  BIRTHDAYONFLT,\n" +
                "  SPT_ID ,\n" +
                "  IS_UM ,\n" +
                "  UPDATE_TIME ,\n" +
                "  UPDATE_USER ,\n" +
                "  VERIFY_STATUS ,\n" +
                "  SPECIAL_TYPE ,\n" +
                "  CREATE_TIME ,\n" +
                "  BIRTHDAY ,\n" +
                "  RECORD_LAST_UPDATE_TIME ,\n" +
                "  RECORD_CREATE_TIME ,\n" +
                "  STAR_FFP ,\n" +
                "  STAR_FFP_NO ,\n" +
                "  LAST_FQTV_LEVEL ,\n" +
                "  sm4_encrypt(des_decrypt(TEL_NUM, '" + Constants.LYGJ_ZB_AES_KEY + "'), '" + Constants.SM4_KEY + "') TEL_NUM, \n" +
                "  SUID ,\n" +
                "  FQTV_DETAIL_LEVEL ,\n" +
                "  MDM_GENDER ,\n" +
                "  sm4_encrypt(des_decrypt(CERT_NO, '" + Constants.LYGJ_ZB_AES_KEY + "'), '" + Constants.SM4_KEY + "') CERT_NO, \n" +
                "  SPECIAL_SERVICE ,\n" +
                "  SPECIAL_TAG ,\n" +
                "  IS_VVIP ,\n" +
                "  IS_CHILD ,\n" +
                "  CKI_TYPE ,\n" +
                "  PROC_AGENT ,\n" +
                "  CKI_DATE ,\n" +
                "  INFANT_NAME ,\n" +
                "  RUSH_LUGGAGE ,\n" +
                "  DRY_BAT_WHEELCHAIR ,\n" +
                "  WET_BAT_WHEELCHAIR ,\n" +
                "  ELEC_BAT_WHEELCHAIR ,\n" +
                "  ONBOARD_WHEELCHAIR ,\n" +
                "  CKI_INFO ,\n" +
                "  PREFER_SEAT ,\n" +
                "  FFP_VALID_FAIL ,\n" +
                "  INCABIN_LITTLE_ANIMAL_WITHCAGE ,\n" +
                "  INCABIN_LITTLE_ANIMAL_NOCAGE ,\n" +
                "  ALLIANCE_SENIOR_TIER_LEVEL ,\n" +
                "  BLACK_LIST_TAG ,\n" +
                "  AEC_REDISTRIBUTE_SEAT ,\n" +
                "  AEC_REBOOK_SEAT ,\n" +
                "  CANCEL_CHANGE_PORT ,\n" +
                "  STAND_BY_PSR ,\n" +
                "  SEAT_OCCUPIED ,\n" +
                "  ASR_SEAT ,\n" +
                "  SSR_MAAS ,\n" +
                "  SSR_MEDA ,\n" +
                "  SSR_LANG ,\n" +
                "  SSR_DPNA ,\n" +
                "  SSR_SPEQ ,\n" +
                "  SSR_PPOC ,\n" +
                "  SSR_UMPG ,\n" +
                "  SSR_UMBD ,\n" +
                "  SSR_UMDF ,\n" +
                "  SSR_UMLG ,\n" +
                "  SSR_SPBG ,\n" +
                "  SSR_YFPB ,\n" +
                "  SSR_WCON ,\n" +
                "  SSR_UMAD ,\n" +
                "  ACTION_CODE ,\n" +
                "  ID_TYPE ,\n" +
                "  sm4_encrypt(des_decrypt(ID_INFO, '" + Constants.LYGJ_ZB_AES_KEY + "'), '" + Constants.SM4_KEY + "') ID_INFO, \n" +
                "  MC_FLT ,\n" +
                "  PRE_SEG ,\n" +
                "  NEXT_SEG ,\n" +
                "  IO_FLAG ,\n" +
                "  EDI_FLAG ,\n" +
                "  ET_SEG ,\n" +
                "  FARE ,\n" +
                "  PSR_TYPE ,\n" +
                "  CKI_NUM ,\n" +
                "  DOWN_GRADE ,\n" +
                "  AVIH_BAGS ,\n" +
                "  AVIH_BAGWGT ,\n" +
                "  AVIH_BAG_TAG,\n" +
                "  COM_UNIQUE_STR ,\n" +
                "  FLT_SEG_COM_UNIQUE_STR ,\n" +
                "  ISTRANSITSEND ,\n" +
                "  PRINT_TICKET_TIME ,\n" +
                "  CHECK_IN_TIME ,\n" +
                "  ID_CARD_AGE,\n" +
                "  ID_CARD_TYPE,\n" +
                "  ID_CARD_NATIVE,\n" +
                "  sm4_encrypt(des_decrypt(MOBILE_PHONE, '" + Constants.LYGJ_ZB_AES_KEY + "'), '" + Constants.SM4_KEY + "') MOBILE_PHONE, \n" +
                "  MOBILE_PHONE_SOURCE,\n" +
                "  BOOK_AGT,\n" +
                "  BOOK_OFFI,\n" +
                "  IS_EXCHANGE_CARD,\n" +
                "  ID_SEQ,\n" +
                "  BKO_UPTIME,\n" +
                "  PNR_GROUP_ID,\n" +
                "  LKT_IDX,\n" +
                "  LKT_STATUS ,\n" +
                "  sm4_encrypt(des_decrypt(MOBILES, '" + Constants.LYGJ_ZB_AES_KEY + "'), '" + Constants.SM4_KEY + "') MOBILES, \n" +
                "  sm4_encrypt(des_decrypt(TELEPHONE, '" + Constants.LYGJ_ZB_AES_KEY + "'), '" + Constants.SM4_KEY + "') TELEPHONE, \n" +
                "  LKT_UPTIME,\n" +
                "  CKI_STATUS,\n" +
                "  COUPON_STATUS,\n" +
                "  ORIGINALTKNE ,\n" +
                "  MARKGP,\n" +
                "  BIG_CUTNUM,\n" +
                "  EVENT,\n" +
                "  INF_ET_NUM ,\n" +
                "  FLT_NUM_SELECT,\n" +
                "  GDS_CODE,\n" +
                "  FBA_MAX_PIECES,\n" +
                "  FBA_TOTAL_WEIGHT,\n" +
                "  TKNEGP,\n" +
                "  DCS_DPTM,\n" +
                "  DCS_ATTM,\n" +
                "  PNR_SELL_CLASS,\n" +
                "  PAX_FFP ,\n" +
                "  IS_GROUP,\n" +
                "  GROUP_NAME ,\n" +
                "  PASSENGER_NUMBER,\n" +
                "  DCS_SELL_CLASS,\n" +
                "  ISSUE_AGENT,\n" +
                "  ISSUE_OFFI,\n" +
                "  PHONE_FLG,\n" +
                "  PHONE_FLG_DATE,\n" +
                "  DCS_ATDATE,\n" +
                "  ORIG_SUB_CLASS,\n" +
                "  TICKET_TYPE,\n" +
                "  IATA_CODE,\n" +
                "  ISSUE_COUNTRY,\n" +
                "  sm4_encrypt(des_decrypt(SSR_INFO, '" + Constants.LYGJ_ZB_AES_KEY + "'), '" + Constants.SM4_KEY + "') SSR_INFO, \n" +
                "  CKI_CITY,\n" +
                "  TCHB,\n" +
                "  PETC_BAGS,\n" +
                "  PETC_BAGWGT,\n" +
                "  PETC_BAG_TAG,\n" +
                "  IS_DEPA ,\n" +
                "  IS_DEPU ,\n" +
                "  IS_DNPA ,\n" +
                "  IS_DPNA ,\n" +
                "  IS_INAD ,\n" +
                "  IS_LEGRORLEGL ,\n" +
                "  IS_MEDA ,\n" +
                "  IS_SVAN ,\n" +
                "  IS_PPOC_PSG,\n" +
                "  BAG_TIME,\n" +
                "  BAG_AGENT,\n" +
                "  LKTXR_UPTM,\n" +
                "  LKT_XR" +
                " FROM DEPART_PASSENGER_INFO_HISTORY "
                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '2022-01-01 00:00:00' AND TIMESTAMP '2023-12-31 23:59:59.999' " +
                " OR  UPDATE_TIME BETWEEN TIMESTAMP '2022-01-01 00:00:00' AND TIMESTAMP '2023-12-31 23:59:59.999' "
                ;
        logger.info("extract TOdsLygjZbDepartPassengerInfoHistory start...");
        TableResult result = tEnv.executeSql(extractSql);

        result.print();


    }

}
