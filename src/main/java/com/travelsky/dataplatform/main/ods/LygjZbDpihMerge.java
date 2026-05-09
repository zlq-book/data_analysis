package com.travelsky.dataplatform.main.ods;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.constans.LygjCreateToSql;
import com.travelsky.dataplatform.udf.OracleDESDecryptor;
import com.travelsky.dataplatform.udf.SM4DecryptUDF;
import com.travelsky.dataplatform.udf.SM4EncryptUDF;
import com.travelsky.dataplatform.utils.CheckpointUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class LygjZbDpihMerge {
    private static final Logger logger = LoggerFactory.getLogger(LygjZbDpihMerge.class);

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            /*加日志：etl_date参数为空*/
            System.exit(0);
        }
        String etlDate = args[0];
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointUtils.setCheckpoint(env, "LygjZbDpihMerge");
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 注册SM4加密UDF
        tEnv.createTemporarySystemFunction("sm4_encrypt", SM4EncryptUDF.class);
        // 注册SM4解密UDF
        tEnv.createTemporarySystemFunction("sm4_decrypt", SM4DecryptUDF.class);
        // 注册DES解密UDF
        tEnv.createTemporarySystemFunction("des_decrypt", OracleDESDecryptor.class);
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        String DEPART_PASSENGER_INFO = "CREATE TABLE T_ODS_LYGJ_DEPART_PASSENGER_INFO (\n" +
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
                " 'connector' = 'doris',\n" +
                "'fenodes' = '" + Constants.DORIS_FE_IP + ":" + Constants.DORIS_FE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                "'benodes' = '" + Constants.DORIS_BE_IP + ":" + Constants.DORIS_BE_PORT + "', -- 替换为 Doris FE 的地址和端口\n" +
                " 'table.identifier' = '" + Constants.ODS_DB + ".T_ODS_LYGJ_DEPART_PASSENGER_INFO',\n" +
                "'sink.label-prefix' = '" + timestamp + uuid + "'," +
                " 'sink.properties.read_json_by_line' = 'true'," +
                " 'sink.properties.format' = 'json'," +
                "    'username' = '" + Constants.ODS_USER + "',\n" +
                "    'password' = '" + Constants.ODS_PWD + "'\n" +
                ")";
        // 1. 创建Doris目标表
        tEnv.executeSql(DEPART_PASSENGER_INFO);
        //
        tEnv.executeSql(LygjCreateToSql.DEPART_PASSENGER_INFO_BAK);
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
                "  FFP, \n" +
                "  ET, \n" +
                "  ET_NUM,\n" +
                "    sm4_encrypt( " +  // 第三步：SM4加密
                "        des_decrypt( " +  // 第二步：DES解密
                "            sm4_decrypt(ET_ID_NUM, '" + Constants.SM4_KEY + "'), " +  // 第一步：SM4解密
                "            '" + Constants.LYGJ_ZB_AES_KEY + "'" +
                "        ), " +
                "        '" + Constants.SM4_KEY + "'" +
                "    ) AS ET_ID_NUM, " +
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
                "  PSPT, " +
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
                "  RELATION, \n" +
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
                "  TEL_NUM, \n" +
                "  SUID ,\n" +
                "  FQTV_DETAIL_LEVEL ,\n" +
                "  MDM_GENDER ,\n" +
                "  CERT_NO, \n" +
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
                "  ID_INFO, \n" +
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
                "  MOBILE_PHONE, \n" +
                "  MOBILE_PHONE_SOURCE,\n" +
                "  BOOK_AGT,\n" +
                "  BOOK_OFFI,\n" +
                "  IS_EXCHANGE_CARD,\n" +
                "  ID_SEQ,\n" +
                "  BKO_UPTIME,\n" +
                "  PNR_GROUP_ID,\n" +
                "  LKT_IDX,\n" +
                "  LKT_STATUS ,\n" +
                "  MOBILES, \n" +
                "  TELEPHONE, \n" +
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
                "  SSR_INFO, \n" +
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
                " FROM T_ODS_LYGJ_DEPART_PASSENGER_INFO_BAK "
                + " WHERE  CREATE_TIME BETWEEN TIMESTAMP '2022-01-01 00:00:00' AND TIMESTAMP '2023-12-31 23:59:59.999' " +
                " OR  UPDATE_TIME BETWEEN TIMESTAMP '2022-01-01 00:00:00' AND TIMESTAMP '2023-12-31 23:59:59.999' "
                ;
        logger.info("extract LygjZbDpihMerge start...");
        TableResult result = tEnv.executeSql(extractSql);

        result.print();

    }
}
