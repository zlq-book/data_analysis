package com.travelsky.dataplatform.utils;

import com.alibaba.fastjson.JSON;
import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.model.dim.UserDimModel;
import com.travelsky.trp.usercenter.data.analysis.model.dim.UserDimSummaryModel;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * @author kuangaihua
 * @date 2025/7/28 15:01
 */
public class IdMappingTest {
    private static final Logger logger = LoggerFactory.getLogger(IdMappingTest.class);

    public static void main(String[] args) throws SQLException {
/*        Set tidSet = new HashSet<String>();
        String tid1="12345";
        String tid2="123456";
        String tid3="12345";
        tidSet.add(tid1);
        tidSet.add(tid2);
        tidSet.add(tid3);
        System.out.println(tidSet.contains("12345"));*/
/*        idMappingFunctionTest("{\n" +
                "  \"tid\": \"12345\",\n" +
                "  \"crmCustomerId\": \"1234578\",\n" +
                "  \"lyCardNumber\": \"123457890\",\n" +
                "  \"certification\": {\n" +
                "    \"PP\": \"1234\",\n" +
                "    \"ID\": \"123\"\n" +
                "  },\n" +
                "  \"mobilePhone\": \"15990331111\",\n" +
                "  \"frequentTravelerCardno\": \"12345789\"\n" +
                "}");*/
        String etlDate = "2025-09-01";
        String query = "SELECT DISTINCT STRONGID " +
                "FROM T_DIM_STRONGID " +
                "WHERE UPDATE_TIME BETWEEN '" + etlDate + " 00:00:00' AND '" + etlDate + " 23:59:59.999999'";
        String strongid="1";
        String sql = "SELECT TT1.* FROM (SELECT\n" +
                "    TID,\n" +
                "    map['FFP'] as FFP,\n" +
                "    map['LYC'] as LYC,\n" +
                "    map['PHONE'] as PHONE,\n" +
                "    map['CCID'] as CCID,\n" +
                "    map['ID'] as ID,\n" +
                "    map['HR'] as HR,\n" +
                "    map['BC'] as BC,\n" +
                "    map['SI'] as SI,\n" +
                "    map['PI'] as PI,\n" +
                "    map['PP'] as PP,\n" +
                "    map['EP'] as EP,\n" +
                "    map['OP'] as OP,\n" +
                "    map['DP'] as DP,\n" +
                "    map['RP'] as RP,\n" +
                "    map['HP'] as HP,\n" +
                "    map['TC'] as TC,\n" +
                "    map['CT'] as CT,\n" +
                "    map['TP'] as TP,\n" +
                "    map['RT'] as RT,\n" +
                "    map['RR'] as RR,\n" +
                "    map['OF'] as OF,\n" +
                "    map['SM'] as SM,\n" +
                "    map['PO'] as PO,\n" +
                "    map['WP'] as WP,\n" +
                "    map['WS'] as WS,\n" +
                "    map['MP'] as MP,\n" +
                "    map['CW'] as CW,\n" +
                "    map['CS'] as CS,\n" +
                "    map['VC'] as VC,\n" +
                "    map['NC'] as NC,\n" +
                "    map['PE'] as PE,\n" +
                "    map['PR'] as PR,\n" +
                "    map['FR'] as FR,\n" +
                "    map['CD'] as CD,\n" +
                "    map['SC'] as SC,\n" +
                "    map['IS'] as `IS`,\n" +
                "    map['SE'] as SE,\n" +
                "    map['ST'] as ST,\n" +
                "    map['OT'] as OT\n" +
                "FROM\n" +
                "    (\n" +
                "    SELECT\n" +
                "        TID,\n" +
                "        map_agg(STRONGID_TYPE, STRONGID) as map\n" +
                "    FROM\n" +
                "        (\n" +
                "        SELECT\n" +
                "            TID,\n" +
                "            STRONGID_TYPE,\n" +
                "            GROUP_CONCAT(STRONGID , ',') AS STRONGID\n" +
                "        from\n" +
                "            " + Constants.DIM_DB + ".T_DIM_STRONGID\n" +
                "        WHERE\n" +
                "            TID IN(\n" +
                "SELECT TID FROM " + Constants.DIM_DB + ".T_DIM_STRONGID \n" +
                "WHERE STRONGID IN(SELECT STRONGID FROM " + Constants.DIM_DB + ".T_DIM_STRONGID WHERE STRONGID = '" + strongid + "' AND STRONGID_STATUS = 1 GROUP BY STRONGID HAVING COUNT(1)>1 ) \n" +
                "    )\n" +
                "            AND STRONGID_STATUS = 1\n" +
                "        GROUP BY\n" +
                "            TID,\n" +
                "            STRONGID_TYPE\n" +
                "    ) T\n" +
                "    GROUP BY\n" +
                "        TID) T1) TT1 LEFT JOIN " + Constants.DIM_DB + ".T_DIM_TID_SET TT2 ON TT1.TID=TT2.TID WHERE TT2.TID_STATUS=1";
        System.out.println(sql);
        System.out.println(query);
    }

    public static void idMappingFunctionTest(String tidJson) {

        String dataType = "LYX";
        Tid tid = JSON.parseObject(tidJson, Tid.class);
/*        tid.setTid("123457890");
        tid.setCrmCustomerId("1234578");
        tid.setFrequentTravelerCardno("12345789");
        tid.setMobilePhone("15990331111");
        tid.setLyCardNumber("123457890");
        Map<String, String> certification = new HashMap<>();
        certification.put("ID", "123");
        certification.put("PP", "1234");
        tid.setCertification(certification);*/
        System.out.println(tid.toString());
        String s = IdMapping.idMappingFunction(tid, dataType, true, true);
        System.out.println("tid: " + s);
    }

    public static void idMappingFunctionTest() {

        String dataType = "LYX";
        Tid tid = new Tid();
        tid.setTid("123457890");
        tid.setCrmCustomerId("1234578");
        tid.setFrequentTravelerCardno("12345789");
        tid.setMobilePhone("15990331111");
        tid.setLyCardNumber("123457890");
        String s = IdMapping.idMappingFunction(tid, dataType);
        System.out.println(tid.toString());
        System.out.println(s);
    }

    public static void run() throws SQLException {
        String etlDate = "2025-07-28";
        String strongid = "15990331111";

        Connection conn = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.DIM_USER, Constants.DIM_PWD);
        Statement stmt = DorisUtils.getStatement(conn);

        Connection connDwd = DorisUtils.getConnection(Constants.DORIS_JDBC_URL, Constants.DWD_USER, Constants.DWD_PWD);
        Statement stmtDwd = DorisUtils.getStatement(connDwd);
        List<Tid> tidList = new ArrayList();
        String sql = "SELECT TT1.* FROM (SELECT\n" +
                "    TID,\n" +
                "    map['FFP'] as FFP,\n" +
                "    map['LYC'] as LYC,\n" +
                "    map['PHONE'] as PHONE,\n" +
                "    map['CCID'] as CCID,\n" +
                "    map['ID'] as ID,\n" +
                "    map['HR'] as HR,\n" +
                "    map['BC'] as BC,\n" +
                "    map['SI'] as SI,\n" +
                "    map['PI'] as PI,\n" +
                "    map['PP'] as PP,\n" +
                "    map['EP'] as EP,\n" +
                "    map['OP'] as OP,\n" +
                "    map['DP'] as DP,\n" +
                "    map['RP'] as RP,\n" +
                "    map['HP'] as HP,\n" +
                "    map['TC'] as TC,\n" +
                "    map['CT'] as CT,\n" +
                "    map['TP'] as TP,\n" +
                "    map['RT'] as RT,\n" +
                "    map['RR'] as RR,\n" +
                "    map['OF'] as OF,\n" +
                "    map['SM'] as SM,\n" +
                "    map['PO'] as PO,\n" +
                "    map['WP'] as WP,\n" +
                "    map['WS'] as WS,\n" +
                "    map['MP'] as MP,\n" +
                "    map['CW'] as CW,\n" +
                "    map['CS'] as CS,\n" +
                "    map['VC'] as VC,\n" +
                "    map['NC'] as NC,\n" +
                "    map['PE'] as PE,\n" +
                "    map['PR'] as PR,\n" +
                "    map['FR'] as FR,\n" +
                "    map['CD'] as CD,\n" +
                "    map['SC'] as SC,\n" +
                "    map['IS'] as `IS`,\n" +
                "    map['SE'] as SE,\n" +
                "    map['ST'] as ST,\n" +
                "    map['OT'] as OT\n" +
                "FROM\n" +
                "    (\n" +
                "    SELECT\n" +
                "        TID,\n" +
                "        map_agg(STRONGID_TYPE, STRONGID) as map\n" +
                "    FROM\n" +
                "        (\n" +
                "        SELECT\n" +
                "            TID,\n" +
                "            STRONGID_TYPE,\n" +
                "            GROUP_CONCAT(STRONGID , ',') AS STRONGID\n" +
                "        from\n" +
                "            " + Constants.DIM_DB + ".T_DIM_STRONGID\n" +
                "        WHERE\n" +
                "            TID IN(\n" +
                "SELECT TID FROM " + Constants.DIM_DB + ".T_DIM_STRONGID \n" +
                "WHERE STRONGID IN(SELECT STRONGID FROM " + Constants.DIM_DB + ".T_DIM_STRONGID WHERE STRONGID = '" + strongid + "' AND STRONGID_STATUS = 1 GROUP BY STRONGID HAVING COUNT(1)>1 ) \n" +
                "    )\n" +
                "            AND STRONGID_STATUS = 1\n" +
                "        GROUP BY\n" +
                "            TID,\n" +
                "            STRONGID_TYPE\n" +
                "    ) T\n" +
                "    GROUP BY\n" +
                "        TID) T1) TT1 LEFT JOIN " + Constants.DIM_DB + ".T_DIM_TID_SET TT2 ON TT1.TID=TT2.TID WHERE TT2.TID_STATUS=1";
        System.out.println("sql:" + sql);
        ResultSet rs = DorisUtils.getDorisResult(stmt, sql);
        try {
            while (rs.next()) {
                Tid tid = new Tid();
                tid.setTid(rs.getString("TID"));
                tid.setLyCardNumber(rs.getString("LYC"));
                tid.setMobilePhone(rs.getString("PHONE"));
                tid.setFrequentTravelerCardno(rs.getString("FFP"));
                tid.setCrmCustomerId(rs.getString("CCID"));
                Map<String, String> certification = new HashMap<>();
                tid.setCertification(certification);

                String ID = rs.getString("ID");
                String HR = rs.getString("HR");
                String BC = rs.getString("BC");
                String SI = rs.getString("SI");
                String PI = rs.getString("PI");
                String PP = rs.getString("PP");
                String EP = rs.getString("EP");
                String OP = rs.getString("OP");
                String DP = rs.getString("DP");
                String RP = rs.getString("RP");
                String HP = rs.getString("HP");
                String TC = rs.getString("TC");
                String CT = rs.getString("CT");
                String TP = rs.getString("TP");
                String RT = rs.getString("RT");
                String RR = rs.getString("RR");
                String OF = rs.getString("OF");
                String SM = rs.getString("SM");
                String PO = rs.getString("PO");
                String WP = rs.getString("WP");
                String WS = rs.getString("WS");
                String MP = rs.getString("MP");
                String CW = rs.getString("CW");
                String CS = rs.getString("CS");
                String VC = rs.getString("VC");
                String NC = rs.getString("NC");
                String PE = rs.getString("PE");
                String PR = rs.getString("PR");
                String FR = rs.getString("FR");
                String CD = rs.getString("CD");
                String SC = rs.getString("SC");
                String IS = rs.getString("IS");
                String SE = rs.getString("SE");
                String ST = rs.getString("ST");
                String OT = rs.getString("OT");
                if (StringUtils.isNotBlank(ID)) certification.put("ID", ID);
                if (StringUtils.isNotBlank(HR)) certification.put("HR", HR);
                if (StringUtils.isNotBlank(BC)) certification.put("BC", BC);
                if (StringUtils.isNotBlank(SI)) certification.put("SI", SI);
                if (StringUtils.isNotBlank(PI)) certification.put("PI", PI);
                if (StringUtils.isNotBlank(PP)) certification.put("PP", PP);
                if (StringUtils.isNotBlank(EP)) certification.put("EP", EP);
                if (StringUtils.isNotBlank(OP)) certification.put("OP", OP);
                if (StringUtils.isNotBlank(DP)) certification.put("DP", DP);
                if (StringUtils.isNotBlank(RP)) certification.put("RP", RP);
                if (StringUtils.isNotBlank(HP)) certification.put("HP", HP);
                if (StringUtils.isNotBlank(TC)) certification.put("TC", TC);
                if (StringUtils.isNotBlank(CT)) certification.put("CT", CT);
                if (StringUtils.isNotBlank(TP)) certification.put("TP", TP);
                if (StringUtils.isNotBlank(RT)) certification.put("RT", RT);
                if (StringUtils.isNotBlank(RR)) certification.put("RR", RR);
                if (StringUtils.isNotBlank(OF)) certification.put("OF", OF);
                if (StringUtils.isNotBlank(SM)) certification.put("SM", SM);
                if (StringUtils.isNotBlank(PO)) certification.put("PO", PO);
                if (StringUtils.isNotBlank(WP)) certification.put("WP", WP);
                if (StringUtils.isNotBlank(WS)) certification.put("WS", WS);
                if (StringUtils.isNotBlank(MP)) certification.put("MP", MP);
                if (StringUtils.isNotBlank(CW)) certification.put("CW", CW);
                if (StringUtils.isNotBlank(CS)) certification.put("CS", CS);
                if (StringUtils.isNotBlank(VC)) certification.put("VC", VC);
                if (StringUtils.isNotBlank(NC)) certification.put("NC", NC);
                if (StringUtils.isNotBlank(PE)) certification.put("PE", PE);
                if (StringUtils.isNotBlank(PR)) certification.put("PR", PR);
                if (StringUtils.isNotBlank(FR)) certification.put("FR", FR);
                if (StringUtils.isNotBlank(CD)) certification.put("CD", CD);
                if (StringUtils.isNotBlank(SC)) certification.put("SC", SC);
                if (StringUtils.isNotBlank(IS)) certification.put("IS", IS);
                if (StringUtils.isNotBlank(SE)) certification.put("SE", SE);
                if (StringUtils.isNotBlank(ST)) certification.put("ST", ST);
                if (StringUtils.isNotBlank(OT)) certification.put("OT", OT);
                tid.setCertification(certification);
                tidList.add(tid);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            rs.close();
        }
        //做数据对比合并
        for (int i = 0; i < tidList.size(); i++) {
            for (int j = i + 1; j < tidList.size(); j++) {
                Tid tidFirst = tidList.get(i);
                Tid tidSencond = tidList.get(j);
                Map<String, String> certificationFirst = tidFirst.getCertification();
                Map<String, String> certificationSecond = tidSencond.getCertification();
                //同一自然人标识
                boolean sameFlag = false;
                if (certificationFirst != null && certificationSecond != null && certificationFirst.size() > 0 && certificationSecond.size() > 0) {
                    //唯一性证件类型的证件号不一致
                    boolean uniqueCertificationDiff = false;
                    //同一证件类型的证件号一致
                    boolean certificationSame = false;
                    //都有证件信息，遍历证件信息，按照证件类型对比。key：证件类型，value：证件号码，多个证件号逗号分隔
                    for (String key : certificationFirst.keySet()) {
                        String firstValue = certificationFirst.get(key);
                        String secondValue = certificationSecond.get(key);
                        if (StringUtils.isNotBlank(firstValue) && StringUtils.isNotBlank(secondValue)) {
                            //同一证件类型都存在，取出所有证件号码进行对比
                            String[] firstCertifications = firstValue.split(",");
                            String[] secondCertifications = secondValue.split(",");
                            for (String firstCertification : firstCertifications) {
                                for (String secondCertification : secondCertifications) {
                                    if (firstCertification.equals(secondCertification)) {
                                        //相同证件类型的证件号一致
                                        certificationSame = true;
                                    } else {
                                        //相同证件类型的证件号不一致，需继续判断是否唯一性证件
                                        if (Constants.UNIQUE_CERTIFICATION.contains(key)) {
                                            if (!("PR".equals(key) && (firstCertification.length() == 15 || secondCertification.length() == 15))) {
                                                uniqueCertificationDiff = true;
                                                //相同唯一性证件类型的证件号不一致，记录异常，且不是新版外国人永久居留证
                                                String errorSql = "insert into " + Constants.DIM_DB + ".T_DIM_IDMAPPING_ERROR (ERROR_TID,SAME_STRONGID,DIFF_STRONGID,ETL_DATE,CREATE_TIME,UPDATE_TIME) values " +
                                                        "('" + tidFirst.getTid() + "/" + tidSencond.getTid() + "','" + strongid + "','" + firstCertification + "/" + secondCertification + "','" + etlDate + "',CURRENT_TIMESTAMP(3),CURRENT_TIMESTAMP(3))";
                                                DorisUtils.excuteDorisInsert(stmt, errorSql);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if (uniqueCertificationDiff) {
                        //不存在唯一性证件不一致，正常数据，继续判断
                        if (certificationSame) {
                            //有证件类型和证件号均一致，认定为同一自然人
                            sameFlag = true;
                        } else {
                            //不存在证件类型和证件号均一致，继续判断
                            //是否存在任意一个强ID（常客卡号、CUSTOMER_ID、手机号）一致
                            boolean strongIdSame = strongidSame(tidFirst, tidSencond);
                            if (strongIdSame) {
                                //存在任意一个强ID（常客卡号、CUSTOMER_ID、手机号）一致,认定为同一自然人
                                sameFlag = true;
                            }
                        }
                    }
                } else {
                    //并非都有证件信息
                    //是否存在任意一个强ID（常客卡号、CUSTOMER_ID、手机号）一致
                    boolean strongIdSame = strongidSame(tidFirst, tidSencond);
                    if (strongIdSame) {
                        //存在任意一个强ID（常客卡号、CUSTOMER_ID、手机号）一致,认定为同一自然人
                        sameFlag = true;
                    }
                }
                String tidEarly = "";
                String tidOld = "";
                if (sameFlag) {
                    //认定为同一自然人
                    //获取创建时间更早的tid
                    String getEarlyTidSql = "select TID from " + Constants.DIM_DB + ".T_DIM_TID_SET where TID in ('" + tidFirst.getTid() + "','" + tidSencond.getTid() + "') ORDER BY TID_STATUS DESC, CREATE_TIME ASC LIMIT 1";
                    ResultSet rsEarlyTid = DorisUtils.getDorisResult(stmt, getEarlyTidSql);
                    if (rsEarlyTid != null && rsEarlyTid.next()) {
                        tidEarly = rsEarlyTid.getString("TID");
                    }
                    String strongIdInsertHead = "insert into " + Constants.DIM_DB + ".T_DIM_STRONGID (ETL_DATE,STRONGID,STRONGID_TYPE,TID,CREATE_TIME,STRONGID_STATUS) values ";
                    List<String> values = new ArrayList<>();
                    Tid finalTid = new Tid();
                    finalTid.setTid(tidEarly);
                    String frequentTravelerCardnoOld = "";
                    String frequentTravelerCardnoNew = "";
                    String mobilePhoneOld = "";
                    String mobilePhoneNew = "";
                    String crmCustomerIdOld = "";
                    String crmCustomerIdNew = "";
                    String lyCardNumberOld = "";
                    String lyCardNumberNew = "";
                    Map<String, String> certificationOld = new HashMap<>();
                    Map<String, String> certificationNew = new HashMap<>();
                    if (tidEarly.equals(tidFirst.getTid())) {
                        //数据合并到第一个tid
                        tidOld = tidSencond.getTid();
                        frequentTravelerCardnoOld = tidSencond.getFrequentTravelerCardno();
                        frequentTravelerCardnoNew = tidFirst.getFrequentTravelerCardno();
                        mobilePhoneOld = tidSencond.getMobilePhone();
                        mobilePhoneNew = tidFirst.getMobilePhone();
                        crmCustomerIdOld = tidSencond.getCrmCustomerId();
                        crmCustomerIdNew = tidFirst.getCrmCustomerId();
                        lyCardNumberOld = tidSencond.getLyCardNumber();
                        lyCardNumberNew = tidFirst.getLyCardNumber();
                        certificationOld = tidSencond.getCertification();
                        certificationNew = tidFirst.getCertification();
                    } else {
                        //数据合并到第二个tid
                        tidOld = tidFirst.getTid();
                        frequentTravelerCardnoOld = tidFirst.getFrequentTravelerCardno();
                        frequentTravelerCardnoNew = tidSencond.getFrequentTravelerCardno();
                        mobilePhoneOld = tidFirst.getMobilePhone();
                        mobilePhoneNew = tidSencond.getMobilePhone();
                        crmCustomerIdOld = tidFirst.getCrmCustomerId();
                        crmCustomerIdNew = tidSencond.getCrmCustomerId();
                        lyCardNumberOld = tidFirst.getLyCardNumber();
                        lyCardNumberNew = tidSencond.getLyCardNumber();
                        certificationOld = tidFirst.getCertification();
                        certificationNew = tidSencond.getCertification();
                    }
                    //合并常客卡号强ID
                    if (StringUtils.isNotBlank(frequentTravelerCardnoOld))
                        mergeFrequentTravelerCardno(frequentTravelerCardnoOld, frequentTravelerCardnoNew, tidEarly, tidOld, etlDate, values, finalTid);
                    //合并手机号强ID
                    if (StringUtils.isNotBlank(mobilePhoneOld))
                        mergeMobilePhone(mobilePhoneOld, mobilePhoneNew, tidEarly, tidOld, etlDate, values, finalTid);
                    //合并CRM客户ID强ID
                    if (StringUtils.isNotBlank(crmCustomerIdOld))
                        mergeCrmCustomerId(crmCustomerIdOld, crmCustomerIdNew, tidEarly, tidOld, etlDate, values, finalTid);
                    //合并鲁雁行卡号强ID
                    if (StringUtils.isNotBlank(lyCardNumberOld))
                        mergeLyCardNumber(lyCardNumberOld, lyCardNumberNew, tidEarly, tidOld, etlDate, values, finalTid);
                    //合并证件信息强ID
                    if (certificationOld != null && certificationOld.size() > 0) {
                        mergeCertification(certificationOld, certificationNew, tidEarly, tidOld, etlDate, values, finalTid);
                    }
                    String insertStrongidSql = strongIdInsertHead + String.join(",", values);
                    logger.info("insertStrongidSql:" + insertStrongidSql);
                    DorisUtils.excuteDorisInsert(stmt, insertStrongidSql);
                    //DONE:记录合并表
                    String insertMergeTidSql = "INSERT INTO " + Constants.DIM_DB + ".T_DIM_MERGE_TID (MERGED_TID,NEW_TID,CREATE_TIME) VALUES ('" + tidOld + "','" + tidEarly + "',CURRENT_TIMESTAMP(3))";
                    logger.info("insertMergeTidSql:" + insertMergeTidSql);
                    DorisUtils.excuteDorisInsert(stmt, insertMergeTidSql);
                    //原tid置为无效
                    String insertTidSql = "insert into " + Constants.DIM_DB + ".T_DIM_TID_SET (ETL_DATE,TID,TID_STATUS) values ('" + etlDate + "','" + tidOld + "',0)";
                    logger.info("insertTidSql:" + insertTidSql);
                    DorisUtils.excuteDorisInsert(stmt, insertTidSql);
                    //修正事实表
                    //直销注册T_DWD_REGISTER_UDO_FACT
                    String tDwdRegisterUdoFactSql = "INSERT INTO " + Constants.DWD_DB + ".T_DWD_REGISTER_UDO_FACT(PK_ID,FK_REGISTER_USER) " +
                            "SELECT PK_ID,'" + tidEarly + "' FROM " + Constants.DWD_DB + ".T_DWD_REGISTER_UDO_FACT WHERE FK_REGISTER_USER='" + tidOld + "'";
                    logger.info("tDwdRegisterUdoFactSql:" + tDwdRegisterUdoFactSql);
                    DorisUtils.excuteDorisInsert(stmtDwd, tDwdRegisterUdoFactSql);
                    //支付-支付流水号级T_DWD_PAYDETAILS_ORD_FACT
                    String tDwdPayDetailsOrdFactSql = "INSERT INTO " + Constants.DWD_DB + ".T_DWD_PAYDETAILS_ORD_FACT(PK_ID,FK_BOOKING_USER_TID) " +
                            "SELECT PK_ID,'" + tidEarly + "' FROM " + Constants.DWD_DB + ".T_DWD_PAYDETAILS_ORD_FACT WHERE FK_BOOKING_USER_TID='" + tidOld + "'";
                    logger.info("tDwdPayDetailsOrdFactSql:" + tDwdPayDetailsOrdFactSql);
                    DorisUtils.excuteDorisInsert(stmtDwd, tDwdPayDetailsOrdFactSql);
                    //常客-活动报名-用户业务级T_DWD_ACT_SIGNUP_FACT
                    String tDwdactSignupFactSql = "INSERT INTO " + Constants.DWD_DB + ".T_DWD_ACT_SIGNUP_FACT(PK_ID,MEMBER_TID) " +
                            "SELECT PK_ID,'" + tidEarly + "' FROM " + Constants.DWD_DB + ".T_DWD_ACT_SIGNUP_FACT WHERE MEMBER_TID='" + tidOld + "'";
                    logger.info("tDwdactSignupFactSql:" + tDwdactSignupFactSql);
                    DorisUtils.excuteDorisInsert(stmtDwd, tDwdactSignupFactSql);
                    //保险事实表-航段级 T_DWD_AUIS_SEG_FACT
                    String tDwdAuisSegFactSql = "INSERT INTO " + Constants.DWD_DB + ".T_DWD_AUIS_SEG_FACT(PK_ID,FK_PASSENGER_USER_TID,FK_BOOKING_USER_TID) " +
                            "SELECT PK_ID,\n" +
                            "CASE WHEN FK_PASSENGER_USER_TID='" + tidOld + "' THEN '" + tidEarly + "' ELSE NULL END AS FK_PASSENGER_USER_TID,\n" +
                            "CASE WHEN FK_BOOKING_USER_TID='" + tidOld + "' THEN '" + tidEarly + "' ELSE NULL END AS FK_BOOKING_USER_TID \n" +
                            "FROM " + Constants.DWD_DB + ".T_DWD_AUIS_SEG_FACT WHERE FK_PASSENGER_USER_TID='" + tidOld + "' OR FK_BOOKING_USER_TID='" + tidOld + "'";
                    logger.info("tDwdAuisSegFactSql:" + tDwdAuisSegFactSql);
                    DorisUtils.excuteDorisInsert(stmtDwd, tDwdAuisSegFactSql);
                    //附加服务_预付费行李退票_航段事实表 T_DWD_BAGGAGE_REFUND_FACT
                    String tDwdBaggageRefundFactSql = "INSERT INTO " + Constants.DWD_DB + ".T_DWD_BAGGAGE_REFUND_FACT(PK_ID,FK_PASSENGER_USER_TID,FK_REFUND_USER_TID) " +
                            "SELECT PK_ID,\n" +
                            "CASE WHEN FK_PASSENGER_USER_TID='" + tidOld + "' THEN '" + tidEarly + "' ELSE NULL END AS FK_PASSENGER_USER_TID,\n" +
                            "CASE WHEN FK_REFUND_USER_TID='" + tidOld + "' THEN '" + tidEarly + "' ELSE NULL END AS FK_REFUND_USER_TID \n" +
                            "FROM " + Constants.DWD_DB + ".T_DWD_BAGGAGE_REFUND_FACT WHERE FK_PASSENGER_USER_TID='" + tidOld + "' OR FK_REFUND_USER_TID='" + tidOld + "'";
                    logger.info("tDwdBaggageRefundFactSql:" + tDwdBaggageRefundFactSql);
                    DorisUtils.excuteDorisInsert(stmtDwd, tDwdBaggageRefundFactSql);
                    //附加服务_预付费行李出票_航段事实表T_DWD_BAGGAGE_TIK_FACT
                    String tDwdBaggageTikFactSql = "INSERT INTO " + Constants.DWD_DB + ".T_DWD_BAGGAGE_TIK_FACT(PK_ID,FK_PASSENGER_USER_TID,FK_BOOKING_USER_TID) " +
                            "SELECT PK_ID,\n" +
                            "CASE WHEN FK_PASSENGER_USER_TID='" + tidOld + "' THEN '" + tidEarly + "' ELSE NULL END AS FK_PASSENGER_USER_TID,\n" +
                            "CASE WHEN FK_BOOKING_USER_TID='" + tidOld + "' THEN '" + tidEarly + "' ELSE NULL END AS FK_BOOKING_USER_TID \n" +
                            "FROM " + Constants.DWD_DB + ".T_DWD_BAGGAGE_TIK_FACT WHERE FK_PASSENGER_USER_TID='" + tidOld + "' OR FK_BOOKING_USER_TID='" + tidOld + "'";
                    logger.info("tDwdBaggageTikFactSql:" + tDwdBaggageTikFactSql);
                    DorisUtils.excuteDorisInsert(stmtDwd, tDwdBaggageTikFactSql);
                    //机票预订PNR级事实表T_DWD_BOOKING_PNR_FACT
                    String tDwdBookingPnrFactSql = "INSERT INTO " + Constants.DWD_DB + ".T_DWD_BOOKING_PNR_FACT(PK_ID,FK_BOOKING_USER_TID) " +
                            "SELECT PK_ID,'" + tidEarly + "' FROM " + Constants.DWD_DB + ".T_DWD_BOOKING_PNR_FACT WHERE FK_BOOKING_USER_TID='" + tidOld + "'";
                    logger.info("tDwdBookingPnrFactSql:" + tDwdBookingPnrFactSql);
                    DorisUtils.excuteDorisInsert(stmtDwd, tDwdBookingPnrFactSql);
                    //机票预订航段级事实表T_DWD_BOOKING_SEG_FACT
                    String tDwdBookingSegFactSql = "INSERT INTO " + Constants.DWD_DB + ".T_DWD_BOOKING_SEG_FACT(PK_ID,FK_PASSENGER_USER_TID,FK_BOOKING_USER_TID) " +
                            "SELECT PK_ID,\n" +
                            "CASE WHEN FK_PASSENGER_USER_TID='" + tidOld + "' THEN '" + tidEarly + "' ELSE NULL END AS FK_PASSENGER_USER_TID,\n" +
                            "CASE WHEN FK_BOOKING_USER_TID='" + tidOld + "' THEN '" + tidEarly + "' ELSE NULL END AS FK_BOOKING_USER_TID \n" +
                            "FROM " + Constants.DWD_DB + ".T_DWD_BOOKING_SEG_FACT WHERE FK_PASSENGER_USER_TID='" + tidOld + "' OR FK_BOOKING_USER_TID='" + tidOld + "'";
                    logger.info("tDwdBookingSegFactSql:" + tDwdBookingSegFactSql);
                    DorisUtils.excuteDorisInsert(stmtDwd, tDwdBookingSegFactSql);
                    //附加服务_改期收费出EMD票_航段事实表T_DWD_CHARGEFEE_TIK_FACT
                    String tDwdChargeFeeTikFactSql = "INSERT INTO " + Constants.DWD_DB + ".T_DWD_CHARGEFEE_TIK_FACT(PK_ID,FK_PASSENGER_USER_TID) " +
                            "SELECT PK_ID,'" + tidEarly + "' FROM " + Constants.DWD_DB + ".T_DWD_CHARGEFEE_TIK_FACT WHERE FK_PASSENGER_USER_TID='" + tidOld + "'";
                    logger.info("tDwdChargeFeeTikFactSql:" + tDwdChargeFeeTikFactSql);
                    DorisUtils.excuteDorisInsert(stmtDwd, tDwdChargeFeeTikFactSql);
                    //值机-航段级T_DWD_CHECKIN_SEG_FACT
                    String tDwdCheckinSegFactSql = "INSERT INTO " + Constants.DWD_DB + ".T_DWD_CHECKIN_SEG_FACT(PK_ID,FK_PASSENGER_USER_TID) " +
                            "SELECT PK_ID,'" + tidEarly + "' FROM " + Constants.DWD_DB + ".T_DWD_CHECKIN_SEG_FACT WHERE FK_PASSENGER_USER_TID='" + tidOld + "'";
                    logger.info("tDwdCheckinSegFactSql:" + tDwdCheckinSegFactSql);
                    DorisUtils.excuteDorisInsert(stmtDwd, tDwdCheckinSegFactSql);
                    //机票改升换开出票航段级事实表T_DWD_DATECHANGE_SEG_FACT
                    String tDwdDateChangeSegFactSql = "INSERT INTO " + Constants.DWD_DB + ".T_DWD_DATECHANGE_SEG_FACT(PK_ID,FK_PASSENGER_USER_TID,FK_BOOKING_USER_TID) " +
                            "SELECT PK_ID,\n" +
                            "CASE WHEN FK_PASSENGER_USER_TID='" + tidOld + "' THEN '" + tidEarly + "' ELSE NULL END AS FK_PASSENGER_USER_TID,\n" +
                            "CASE WHEN FK_BOOKING_USER_TID='" + tidOld + "' THEN '" + tidEarly + "' ELSE NULL END AS FK_BOOKING_USER_TID \n" +
                            "FROM " + Constants.DWD_DB + ".T_DWD_DATECHANGE_SEG_FACT WHERE FK_PASSENGER_USER_TID='" + tidOld + "' OR FK_BOOKING_USER_TID='" + tidOld + "'";
                    logger.info("tDwdDateChangeSegFactSql:" + tDwdDateChangeSegFactSql);
                    DorisUtils.excuteDorisInsert(stmtDwd, tDwdDateChangeSegFactSql);
                    //成行-航段级T_DWD_DEPART_SEG_FACT
                    String tDwdDepartSegFactSql = "INSERT INTO " + Constants.DWD_DB + ".T_DWD_DEPART_SEG_FACT(PK_ID,FK_PASSENGER_USER_TID) " +
                            "SELECT PK_ID,'" + tidEarly + "' FROM " + Constants.DWD_DB + ".T_DWD_DEPART_SEG_FACT WHERE FK_PASSENGER_USER_TID='" + tidOld + "'";
                    logger.info("tDwdDepartSegFactSql:" + tDwdDepartSegFactSql);
                    DorisUtils.excuteDorisInsert(stmtDwd, tDwdDepartSegFactSql);
                    //附加服务逾重行李出票航段事实表T_DWD_EXCESS_BAGGAGE_TIK_FACT
                    String tDwdExcessBaggageTikFactSql = "INSERT INTO " + Constants.DWD_DB + ".T_DWD_EXCESS_BAGGAGE_TIK_FACT(PK_ID,FK_PASSENGER_USER_TID) " +
                            "SELECT PK_ID,'" + tidEarly + "' FROM " + Constants.DWD_DB + ".T_DWD_EXCESS_BAGGAGE_TIK_FACT WHERE FK_PASSENGER_USER_TID='" + tidOld + "'";
                    logger.info("tDwdExcessBaggageTikFactSql:" + tDwdExcessBaggageTikFactSql);
                    DorisUtils.excuteDorisInsert(stmtDwd, tDwdExcessBaggageTikFactSql);
                    //附加服务选餐退票航段事实表T_DWD_MEAL_REFUND_FACT
                    String tDwdMealRefundFactSql = "INSERT INTO " + Constants.DWD_DB + ".T_DWD_MEAL_REFUND_FACT(PK_ID,FK_PASSENGER_USER_TID) " +
                            "SELECT PK_ID,\n" +
                            "CASE WHEN FK_PASSENGER_USER_TID='" + tidOld + "' THEN '" + tidEarly + "' ELSE NULL END AS FK_PASSENGER_USER_TID\n" +
                            "FROM " + Constants.DWD_DB + ".T_DWD_MEAL_REFUND_FACT WHERE FK_PASSENGER_USER_TID='" + tidOld + "'";
                    logger.info("tDwdMealRefundFactSql:" + tDwdMealRefundFactSql);
                    DorisUtils.excuteDorisInsert(stmtDwd, tDwdMealRefundFactSql);
                    //附加服务选餐出票航段事实表T_DWD_MEAL_TIK_FACT
                    String tDwdMailTikFactSql = "INSERT INTO " + Constants.DWD_DB + ".T_DWD_MEAL_TIK_FACT(PK_ID,FK_PASSENGER_USER_TID) " +
                            "SELECT PK_ID,\n" +
                            "CASE WHEN FK_PASSENGER_USER_TID='" + tidOld + "' THEN '" + tidEarly + "' ELSE NULL END AS FK_PASSENGER_USER_TID\n" +
                            "FROM " + Constants.DWD_DB + ".T_DWD_MEAL_TIK_FACT WHERE FK_PASSENGER_USER_TID='" + tidOld + "'";
                    logger.info("tDwdMailTikFactSql:" + tDwdMailTikFactSql);
                    DorisUtils.excuteDorisInsert(stmtDwd, tDwdMailTikFactSql);
                    //常客-里程累积-业务级T_DWD_MILE_ACCRUAL_FACT
                    String tDwdMileAccrualFactSql = "INSERT INTO " + Constants.DWD_DB + ".T_DWD_MILE_ACCRUAL_FACT(PK_ID,CUMULATION_TID) " +
                            "SELECT PK_ID,'" + tidEarly + "' FROM " + Constants.DWD_DB + ".T_DWD_MILE_ACCRUAL_FACT WHERE CUMULATION_TID='" + tidOld + "'";
                    logger.info("tDwdMileAccrualFactSql:" + tDwdMileAccrualFactSql);
                    DorisUtils.excuteDorisInsert(stmtDwd, tDwdMileAccrualFactSql);
                    //常客-里程兑换-业务级T_DWD_MILE_REDEMPTION_FACT
                    String tDwdMileRedemptionFactSql = "INSERT INTO " + Constants.DWD_DB + ".T_DWD_MILE_REDEMPTION_FACT(PK_ID,CUMULATION_TID) " +
                            "SELECT PK_ID,'" + tidEarly + "' FROM " + Constants.DWD_DB + ".T_DWD_MILE_REDEMPTION_FACT WHERE CUMULATION_TID='" + tidOld + "'";
                    logger.info("tDwdMileRedemptionFactSql:" + tDwdMileRedemptionFactSql);
                    DorisUtils.excuteDorisInsert(stmtDwd, tDwdMileRedemptionFactSql);
                    //常客-注册事实表T_DWD_MEMBER_REGISTER_FACT
                    String tDwdMembereRegisterFactSql = "INSERT INTO " + Constants.DWD_DB + ".T_DWD_MEMBER_REGISTER_FACT(PK_ID,FK_TID) " +
                            "SELECT PK_ID,'" + tidEarly + "' FROM " + Constants.DWD_DB + ".T_DWD_MEMBER_REGISTER_FACT WHERE FK_TID='" + tidOld + "'";
                    logger.info("tDwdMembereRegisterFactSql:" + tDwdMembereRegisterFactSql);
                    DorisUtils.excuteDorisInsert(stmtDwd, tDwdMembereRegisterFactSql);
                    //明细事实表-掌尚飞-次卡预订T_DWD_MULTI_CARD_BOOK_FACT
                    String tDwdMultiCardBookFactSql = "INSERT INTO " + Constants.DWD_DB + ".T_DWD_MULTI_CARD_BOOK_FACT(PK_ID,BENEFI_TID,FK_BOOKING_USER_TID) " +
                            "SELECT PK_ID,\n" +
                            "CASE WHEN BENEFI_TID='" + tidOld + "' THEN '" + tidEarly + "' ELSE NULL END AS BENEFI_TID,\n" +
                            "CASE WHEN FK_BOOKING_USER_TID='" + tidOld + "' THEN '" + tidEarly + "' ELSE NULL END AS FK_BOOKING_USER_TID \n" +
                            "FROM " + Constants.DWD_DB + ".T_DWD_MULTI_CARD_BOOK_FACT WHERE BENEFI_TID='" + tidOld + "' OR FK_BOOKING_USER_TID='" + tidOld + "'";
                    logger.info("tDwdMultiCardBookFactSql:" + tDwdMultiCardBookFactSql);
                    DorisUtils.excuteDorisInsert(stmtDwd, tDwdMultiCardBookFactSql);
                    //明细事实表-掌尚飞-次卡退订T_DWD_MULTI_CARD_CANCEL_FACT
                    String tDwdMultiCardCancelFactSql = "INSERT INTO " + Constants.DWD_DB + ".T_DWD_MULTI_CARD_CANCEL_FACT(PK_ID,BENEFI_TID,FK_BOOKING_USER_TID) " +
                            "SELECT PK_ID,\n" +
                            "CASE WHEN BENEFI_TID='" + tidOld + "' THEN '" + tidEarly + "' ELSE NULL END AS BENEFI_TID,\n" +
                            "CASE WHEN FK_BOOKING_USER_TID='" + tidOld + "' THEN '" + tidEarly + "' ELSE NULL END AS FK_BOOKING_USER_TID \n" +
                            "FROM " + Constants.DWD_DB + ".T_DWD_MULTI_CARD_CANCEL_FACT WHERE BENEFI_TID='" + tidOld + "' OR FK_BOOKING_USER_TID='" + tidOld + "'";
                    logger.info("tDwdMultiCardCancelFactSql:" + tDwdMultiCardCancelFactSql);
                    DorisUtils.excuteDorisInsert(stmtDwd, tDwdMultiCardCancelFactSql);
                    //明细事实表-掌尚飞-次卡兑换机票T_DWD_MULTI_CARD_REDEEM_FACT
                    String tDwdMultiCardRedeemFactSql = "INSERT INTO " + Constants.DWD_DB + ".T_DWD_MULTI_CARD_REDEEM_FACT(PK_ID,PASSENGER_USER) " +
                            "SELECT PK_ID,'" + tidEarly + "' FROM " + Constants.DWD_DB + ".T_DWD_MULTI_CARD_REDEEM_FACT WHERE PASSENGER_USER='" + tidOld + "'";
                    logger.info("tDwdMultiCardRedeemFactSql:" + tDwdMultiCardRedeemFactSql);
                    DorisUtils.excuteDorisInsert(stmtDwd, tDwdMultiCardRedeemFactSql);
                    //机票退票航段级事实表T_DWD_REFUND_SEG_FACT
                    String tDwdRefundFactSql = "INSERT INTO " + Constants.DWD_DB + ".T_DWD_REFUND_SEG_FACT(PK_ID,FK_PASSENGER_USER_TID) " +
                            "SELECT PK_ID,\n" +
                            "CASE WHEN FK_PASSENGER_USER_TID='" + tidOld + "' THEN '" + tidEarly + "' ELSE NULL END AS FK_PASSENGER_USER_TID\n" +
                            "FROM " + Constants.DWD_DB + ".T_DWD_REFUND_SEG_FACT WHERE FK_PASSENGER_USER_TID='" + tidOld + "'";
                    logger.info("tDwdRefundFactSql:" + tDwdRefundFactSql);
                    DorisUtils.excuteDorisInsert(stmtDwd, tDwdRefundFactSql);
                    //附加服务_选座退票_航段事实表 T_DWD_SEAT_REFUND_FACT
                    String tDwdSeatRefundFactSql = "INSERT INTO " + Constants.DWD_DB + ".T_DWD_SEAT_REFUND_FACT(PK_ID,FK_PASSENGER_USER_TID) " +
                            "SELECT PK_ID,\n" +
                            "CASE WHEN FK_PASSENGER_USER_TID='" + tidOld + "' THEN '" + tidEarly + "' ELSE NULL END AS FK_PASSENGER_USER_TID\n" +
//                            "CASE WHEN FK_BOOKING_USER_TID='" + tidOld + "' THEN '" + tidEarly + "' ELSE NULL END AS FK_BOOKING_USER_TID \n" +
                            "FROM " + Constants.DWD_DB + ".T_DWD_SEAT_REFUND_FACT WHERE FK_PASSENGER_USER_TID='" + tidOld + "'";
                    logger.info("tDwdSeatRefundFactSql:" + tDwdSeatRefundFactSql);
                    DorisUtils.excuteDorisInsert(stmtDwd, tDwdSeatRefundFactSql);
                    //附加服务_选座出票_航段事实表T_DWD_SEAT_TIK_FACT
                    String tDwdSeatTikFactSql = "INSERT INTO " + Constants.DWD_DB + ".T_DWD_SEAT_TIK_FACT(PK_ID,FK_PASSENGER_USER_TID,FK_BOOKING_USER_TID) " +
                            "SELECT PK_ID,\n" +
                            "CASE WHEN FK_PASSENGER_USER_TID='" + tidOld + "' THEN '" + tidEarly + "' ELSE NULL END AS FK_PASSENGER_USER_TID,\n" +
                            "CASE WHEN FK_BOOKING_USER_TID='" + tidOld + "' THEN '" + tidEarly + "' ELSE NULL END AS FK_BOOKING_USER_TID \n" +
                            "FROM " + Constants.DWD_DB + ".T_DWD_SEAT_TIK_FACT WHERE FK_PASSENGER_USER_TID='" + tidOld + "' OR FK_BOOKING_USER_TID='" + tidOld + "'";
                    logger.info("tDwdSeatTikFactSql:" + tDwdSeatTikFactSql);
                    DorisUtils.excuteDorisInsert(stmtDwd, tDwdSeatTikFactSql);
                    //鲁雁行-服务预约-业务级事实表T_DWD_SERV_APPOINTMENT_FACT
                    String tDwdServAppointmentFactSql = "INSERT INTO " + Constants.DWD_DB + ".T_DWD_SERV_APPOINTMENT_FACT(PK_ID,PASSENGER_USER_TID) " +
                            "SELECT PK_ID,'" + tidEarly + "' FROM " + Constants.DWD_DB + ".T_DWD_SERV_APPOINTMENT_FACT WHERE PASSENGER_USER_TID='" + tidOld + "'";
                    logger.info("tDwdServAppointmentFactSql:" + tDwdServAppointmentFactSql);
                    DorisUtils.excuteDorisInsert(stmtDwd, tDwdServAppointmentFactSql);
                    //机票出票航段级事实表T_DWD_TICKING_SEG_FACT
                    String tDwdTickingSegFactSql = "INSERT INTO " + Constants.DWD_DB + ".T_DWD_TICKING_SEG_FACT(PK_ID,FK_PASSENGER_USER_TID,FK_BOOKING_USER_TID) " +
                            "SELECT PK_ID,\n" +
                            "CASE WHEN FK_PASSENGER_USER_TID='" + tidOld + "' THEN '" + tidEarly + "' ELSE NULL END AS FK_PASSENGER_USER_TID,\n" +
                            "CASE WHEN FK_BOOKING_USER_TID='" + tidOld + "' THEN '" + tidEarly + "' ELSE NULL END AS FK_BOOKING_USER_TID \n" +
                            "FROM " + Constants.DWD_DB + ".T_DWD_TICKING_SEG_FACT WHERE FK_PASSENGER_USER_TID='" + tidOld + "' OR FK_BOOKING_USER_TID='" + tidOld + "'";
                    logger.info("tDwdTickingSegFactSql:" + tDwdTickingSegFactSql);
                    DorisUtils.excuteDorisInsert(stmtDwd, tDwdTickingSegFactSql);
                    //机票出票客票级事实表T_DWD_TICKING_TIC_FACT
                    String tDwdTickingTikFactSql = "INSERT INTO " + Constants.DWD_DB + ".T_DWD_TICKING_TIC_FACT(PK_ID,FK_PASSENGER_USER_TID,FK_BOOKING_USER_TID) " +
                            "SELECT PK_ID,\n" +
                            "CASE WHEN FK_PASSENGER_USER_TID='" + tidOld + "' THEN '" + tidEarly + "' ELSE NULL END AS FK_PASSENGER_USER_TID,\n" +
                            "CASE WHEN FK_BOOKING_USER_TID='" + tidOld + "' THEN '" + tidEarly + "' ELSE NULL END AS FK_BOOKING_USER_TID \n" +
                            "FROM " + Constants.DWD_DB + ".T_DWD_TICKING_TIC_FACT WHERE FK_PASSENGER_USER_TID='" + tidOld + "' OR FK_BOOKING_USER_TID='" + tidOld + "'";
                    logger.info("tDwdTickingTikFactSql:" + tDwdTickingTikFactSql);
                    DorisUtils.excuteDorisInsert(stmtDwd, tDwdTickingTikFactSql);
                    //明细事实表-掌尚飞-抖音券码购买T_DWD_TIKTOK_VCHR_PUR_FACT
                    String tDwdTiktokVchrPurFactSql = "INSERT INTO " + Constants.DWD_DB + ".T_DWD_TIKTOK_VCHR_PUR_FACT(PK_ID,FK_BOOKING_USER_TID) " +
                            "SELECT PK_ID,'" + tidEarly + "' FROM " + Constants.DWD_DB + ".T_DWD_TIKTOK_VCHR_PUR_FACT WHERE FK_BOOKING_USER_TID='" + tidOld + "'";
                    logger.info("tDwdTiktokVchrPurFactSql:" + tDwdTiktokVchrPurFactSql);
                    DorisUtils.excuteDorisInsert(stmtDwd, tDwdTiktokVchrPurFactSql);
                    //附加服务无陪儿童出EMD票航段事实表T_DWD_UMCHILD_TIK_FACT
                    String tDwdUmchildTikFactSql = "INSERT INTO " + Constants.DWD_DB + ".T_DWD_UMCHILD_TIK_FACT(PK_ID,FK_PASSENGER_USER_TID) " +
                            "SELECT PK_ID,'" + tidEarly + "' FROM " + Constants.DWD_DB + ".T_DWD_UMCHILD_TIK_FACT WHERE FK_PASSENGER_USER_TID='" + tidOld + "'";
                    logger.info("tDwdUmchildTikFactSql:" + tDwdUmchildTikFactSql);
                    DorisUtils.excuteDorisInsert(stmtDwd, tDwdUmchildTikFactSql);
                    //附加服务升舱退票航段事实表T_DWD_UPGR_REFUND_FACT
                    String tDwdUpgrRefundFactSql = "INSERT INTO " + Constants.DWD_DB + ".T_DWD_UPGR_REFUND_FACT(PK_ID,FK_PASSENGER_USER_TID,FK_BOOKING_USER_TID) " +
                            "SELECT PK_ID,\n" +
                            "CASE WHEN FK_PASSENGER_USER_TID='" + tidOld + "' THEN '" + tidEarly + "' ELSE NULL END AS FK_PASSENGER_USER_TID,\n" +
                            "CASE WHEN FK_BOOKING_USER_TID='" + tidOld + "' THEN '" + tidEarly + "' ELSE NULL END AS FK_BOOKING_USER_TID \n" +
                            "FROM " + Constants.DWD_DB + ".T_DWD_UPGR_REFUND_FACT WHERE FK_PASSENGER_USER_TID='" + tidOld + "' OR FK_BOOKING_USER_TID='" + tidOld + "'";
                    logger.info("tDwdUpgrRefundFactSql:" + tDwdUpgrRefundFactSql);
                    DorisUtils.excuteDorisInsert(stmtDwd, tDwdUpgrRefundFactSql);
                    //附加服务升舱出票航段事实表T_DWD_UPGR_TIK_FACT
                    String tDwdUpgrTikFactSql = "INSERT INTO " + Constants.DWD_DB + ".T_DWD_UPGR_TIK_FACT(PK_ID,FK_PASSENGER_USER_TID) " +
                            "SELECT PK_ID,\n" +
                            "CASE WHEN FK_PASSENGER_USER_TID='" + tidOld + "' THEN '" + tidEarly + "' ELSE NULL END AS FK_PASSENGER_USER_TID\n" +
//                            "CASE WHEN FK_BOOKING_USER_TID='" + tidOld + "' THEN '" + tidEarly + "' ELSE NULL END AS FK_BOOKING_USER_TID \n" +
                            "FROM " + Constants.DWD_DB + ".T_DWD_UPGR_TIK_FACT WHERE FK_PASSENGER_USER_TID='" + tidOld + "'";
                    logger.info("tDwdUpgrTikFactSql:" + tDwdUpgrTikFactSql);
                    DorisUtils.excuteDorisInsert(stmtDwd, tDwdUpgrTikFactSql);
                    //常客-援疆卡发放-业务事实表 T_DWD_YUANXJ_CARD_FACT
                    String tDwdYuanxjCardFactSql = "INSERT INTO " + Constants.DWD_DB + ".T_DWD_YUANXJ_CARD_FACT(PK_ID,APPLICANTS_TID) " +
                            "SELECT PK_ID,\n" +
                            "'" + tidEarly + "'" +
                            "FROM " + Constants.DWD_DB + ".T_DWD_YUANXJ_CARD_FACT WHERE APPLICANTS_TID='" + tidOld + "'";
                    logger.info("tDwdUpgrTikFactSql:" + tDwdUpgrTikFactSql);
                    DorisUtils.excuteDorisInsert(stmtDwd, tDwdUpgrTikFactSql);
                    //修正维表
                    //证件信息维表 T_DIM_CERT_DIM
                    String tDimCertDimSql = "INSERT INTO " + Constants.DIM_DB + ".T_DIM_CERT_DIM\n" +
                            "(SYSTEM_KEY,\n" +
                            "T_ID,\n" +
                            "CERT_TYPE,\n" +
                            "CERT_NUMBER,\n" +
                            "IS_DIRECT_SALES_REAL_NAME_VERIFIED_CARD,\n" +
                            "IS_LY_REGIST_CARD,\n" +
                            "IS_FREQUENT_FLYER_REGISTRATION_CARD,\n" +
                            "IS_PURCHASERS_BENEFICIARY_CARD,\n" +
                            "IS_CODE_RULE_COMPLIANT,\n" +
                            "CERT_ISSUE_DATE,\n" +
                            "CERT_EXPIRE_DATE,\n" +
                            "CERT_ISSUING_COUNTRY,\n" +
                            "CERT_ISSUING_AUTHORITY,\n" +
//                            "MOST_TRUSTED_SOURCE,\n" +
                            "UPDATE_TIME,\n" +
                            "CREATE_TIME)\n" +
                            "SELECT REPLACE(SYSTEM_KEY,'" + tidOld + "','" + tidEarly + "') AS SYSTEM_KEY, \n" +
                            "'" + tidEarly + "' AS T_ID,\n" +
                            " CERT_TYPE,\n" +
                            " CERT_NUMBER, \n" +
                            "IS_DIRECT_SALES_REAL_NAME_VERIFIED_CARD,\n" +
                            " IS_LY_REGIST_CARD, IS_FREQUENT_FLYER_REGISTRATION_CARD,\n" +
                            " IS_PURCHASERS_BENEFICIARY_CARD,\n" +
                            " IS_CODE_RULE_COMPLIANT,\n" +
                            " CERT_ISSUE_DATE,\n" +
                            " CERT_EXPIRE_DATE,\n" +
                            " CERT_ISSUING_COUNTRY,\n" +
                            " CERT_ISSUING_AUTHORITY,\n" +
//                            " MOST_TRUSTED_SOURCE,\n" +
                            " CURRENT_TIMESTAMP(3),\n" +
                            " CREATE_TIME\n" +
                            "FROM " + Constants.DIM_DB + ".T_DIM_CERT_DIM\n" +
                            "WHERE T_ID = '" + tidOld + "'";
                    logger.info("tDimCertDimSql:" + tDimCertDimSql);
                    //TODO: doris 不支持删除，需要增加逻辑删除字段
/*                    String tDimCertDimDelSql = "DELETE FROM " + Constants.DIM_DB + ".T_DIM_CERT_DIM WHERE SYSTEM_KEY IN(\n" +
                            "SELECT SYSTEM_KEY \n" +
                            "FROM " + Constants.DIM_DB + ".T_DIM_CERT_DIM\n" +
                            "WHERE T_ID = '" + tidOld + "')";
                    logger.info("tDimCertDimDelSql:" + tDimCertDimDelSql);*/
                    DorisUtils.excuteDorisInsert(stmt, tDimCertDimSql);
//                    DorisUtils.excuteDorisInsert(stmt, tDimCertDimDelSql);
                    //直销用户维表 T_DIM_CUSTOM_DIM
                    String tDimCustomDimSql = "INSERT INTO " + Constants.DIM_DB + ".T_DIM_CUSTOM_DIM(\n" +
                            "SYSTEM_KEY\n" +
                            ",T_ID\n" +
                            ",CUSTOMER_ID\n" +
                            ",DIRECT_REGISTER_CHANNEL\n" +
                            ",DIRECT_USER_STATUS\n" +
                            ",LAST_LOGIN_TIME\n" +
                            ",DIRECT_REGISTER_DATE\n" +
                            ",DIRECT_VERIFIED_FLAG\n" +
                            ",DIRECT_VERIFIED_METHOD\n" +
                            ",DIRECT_VERIFY_DATE\n" +
                            ",IS_BLACKLIST_USER\n" +
                            ",STUDENT_FLAG\n" +
                            ",STUDENT_EXPIRY\n" +
                            ",TEACHER_FLAG\n" +
                            ",SHIP_ADDRESS\n" +
                            ",INVOICE_TITLE\n" +
                            ",WEB_VIRTUAL_STATUS\n" +
                            ",WEB_ACCOUNT_STATUS\n" +
                            ",UPDATE_TIME\n" +
                            ",CREATE_TIME)\n" +
                            "SELECT \n" +
                            "REPLACE(SYSTEM_KEY,'" + tidOld + "','" + tidEarly + "') AS SYSTEM_KEY\n" +
                            ",'" + tidEarly + "' AS T_ID\n" +
                            ",CUSTOMER_ID\n" +
                            ",DIRECT_REGISTER_CHANNEL\n" +
                            ",DIRECT_USER_STATUS\n" +
                            ",LAST_LOGIN_TIME\n" +
                            ",DIRECT_REGISTER_DATE\n" +
                            ",DIRECT_VERIFIED_FLAG\n" +
                            ",DIRECT_VERIFIED_METHOD\n" +
                            ",DIRECT_VERIFY_DATE\n" +
                            ",IS_BLACKLIST_USER\n" +
                            ",STUDENT_FLAG\n" +
                            ",STUDENT_EXPIRY\n" +
                            ",TEACHER_FLAG\n" +
                            ",SHIP_ADDRESS\n" +
                            ",INVOICE_TITLE\n" +
                            ",WEB_VIRTUAL_STATUS\n" +
                            ",WEB_ACCOUNT_STATUS\n" +
                            ",UPDATE_TIME\n" +
                            ",CREATE_TIME\n" +
                            "FROM " + Constants.DIM_DB + ".T_DIM_CUSTOM_DIM WHERE T_ID='" + tidOld + "'\n";
                    logger.info("tDimCustomDimSql:" + tDimCustomDimSql);
                    //TODO: doris 不支持删除，需要增加逻辑删除字段
/*                    String tDimCustomDimDelSql = "DELETE FROM " + Constants.DIM_DB + ".T_DIM_CUSTOM_DIM WHERE SYSTEM_KEY IN(\n" +
                            "SELECT SYSTEM_KEY \n" +
                            "FROM " + Constants.DIM_DB + ".T_DIM_CUSTOM_DIM\n" +
                            "WHERE T_ID = '" + tidOld + "')";
                    logger.info("tDimCustomDimDelSql:" + tDimCustomDimDelSql);*/
                    DorisUtils.excuteDorisInsert(stmt, tDimCustomDimSql);
//                    DorisUtils.excuteDorisInsert(stmt, tDimCustomDimDelSql);
                    //常客信息维表 T_DIM_FFP_DIM
                    String tDimFfpDimSql = "INSERT INTO " + Constants.DIM_DB + ".T_DIM_FFP_DIM(\n" +
                            "SYSTEM_KEY\n" +
                            ",T_ID\n" +
                            ",FFRF\n" +
                            ",FF_LEVEL\n" +
                            ",IS_PARENT_CARD_NUMBER\n" +
                            ",FT_REGISTER_TIME\n" +
                            ",YJ_CARD\n" +
                            ",YJ_CARD_EXPIREDATE\n" +
                            ",DEV_CHANNEL_ONE\n" +
                            ",DEV_CHANNEL_TWO\n" +
                            ",DEV_CHANNEL_THREE\n" +
                            ",DEV_CHANNEL_FOUR\n" +
                            ",FF_CERT_TYPE\n" +
                            ",FF_CERT_NUMBER\n" +
                            ",MOST_TRUSTED_SOURCE\n" +
                            ",UPDATE_TIME\n" +
                            ",CREATE_TIME)\n" +
                            "SELECT \n" +
                            "REPLACE(SYSTEM_KEY,'" + tidOld + "','" + tidEarly + "') AS SYSTEM_KEY\n" +
                            ",'" + tidEarly + "' AS T_ID\n" +
                            ",FFRF\n" +
                            ",FF_LEVEL\n" +
                            ",IS_PARENT_CARD_NUMBER\n" +
                            ",FT_REGISTER_TIME\n" +
                            ",YJ_CARD\n" +
                            ",YJ_CARD_EXPIREDATE\n" +
                            ",DEV_CHANNEL_ONE\n" +
                            ",DEV_CHANNEL_TWO\n" +
                            ",DEV_CHANNEL_THREE\n" +
                            ",DEV_CHANNEL_FOUR\n" +
                            ",FF_CERT_TYPE\n" +
                            ",FF_CERT_NUMBER\n" +
                            ",MOST_TRUSTED_SOURCE\n" +
                            ",UPDATE_TIME\n" +
                            ",CREATE_TIME\n" +
                            "FROM " + Constants.DIM_DB + ".T_DIM_FFP_DIM WHERE T_ID='" + tidOld + "'\n";
                    logger.info("tDimFfpDimSql:" + tDimFfpDimSql);
                    //TODO: doris 不支持删除，需要增加逻辑删除字段
  /*                  String tDimFfpDimDelSql = "DELETE FROM " + Constants.DIM_DB + ".T_DIM_FFP_DIM WHERE SYSTEM_KEY IN(\n" +
                            "SELECT SYSTEM_KEY \n" +
                            "FROM " + Constants.DIM_DB + ".T_DIM_FFP_DIM\n" +
                            "WHERE T_ID = '" + tidOld + "')";
                    logger.info("tDimFfpDimDelSql:" + tDimFfpDimDelSql);*/
                    DorisUtils.excuteDorisInsert(stmt, tDimFfpDimSql);
//                    DorisUtils.excuteDorisInsert(stmt, tDimFfpDimDelSql);
                    //高端旅客类型维表 T_DIM_GDLK_DIM
                    String tDimGdlkDimSql = "INSERT INTO " + Constants.DIM_DB + ".T_DIM_GDLK_DIM(\n" +
                            "SYSTEM_KEY\n" +
                            ",FK_USER_TID\n" +
                            ",HIGH_TRAVELER_TYPE\n" +
                            ",HIGH_TRAVELER_TIER\n" +
                            ",TIER_PRESTIGE_EXPIREDATE\n" +
                            ",TIER_HONOR_EXPIREDATE\n" +
                            ",HIGH_TRAVELER_DS\n" +
                            ",VIP_FLAG\n" +
                            ",CIP_FLAG\n" +
                            ",VVIP_FLAG\n" +
                            ",FOOD_PREFERENCE\n" +
                            ",SEAT_PREFERENCE\n" +
                            ",TEMP_FOOD_PREFERENCE\n" +
                            ",TEMP_SEAT_PREFERENCE\n" +
                            ",BEVERAGE_PREFERENCE\n" +
                            ",LONG_TERM_SEAT_PREFERENCE\n" +
                            ",FIRST_CLASS_LOUNGE_PREFERENCE\n" +
                            ",UPDATE_TIME\n" +
                            ",CREATE_TIME)\n" +
                            "SELECT \n" +
                            "REPLACE(SYSTEM_KEY,'" + tidOld + "','" + tidEarly + "') AS SYSTEM_KEY\n" +
                            ",'" + tidEarly + "' AS FK_USER_TID\n" +
                            ",HIGH_TRAVELER_TYPE\n" +
                            ",HIGH_TRAVELER_TIER\n" +
                            ",TIER_PRESTIGE_EXPIREDATE\n" +
                            ",TIER_HONOR_EXPIREDATE\n" +
                            ",HIGH_TRAVELER_DS\n" +
                            ",VIP_FLAG\n" +
                            ",CIP_FLAG\n" +
                            ",VVIP_FLAG\n" +
                            ",FOOD_PREFERENCE\n" +
                            ",SEAT_PREFERENCE\n" +
                            ",TEMP_FOOD_PREFERENCE\n" +
                            ",TEMP_SEAT_PREFERENCE\n" +
                            ",BEVERAGE_PREFERENCE\n" +
                            ",LONG_TERM_SEAT_PREFERENCE\n" +
                            ",FIRST_CLASS_LOUNGE_PREFERENCE\n" +
                            ",UPDATE_TIME\n" +
                            ",CREATE_TIME\n" +
                            "FROM " + Constants.DIM_DB + ".T_DIM_GDLK_DIM WHERE FK_USER_TID='" + tidOld + "'\n";
                    logger.info("tDimGdlkDimSql:" + tDimGdlkDimSql);
                    //TODO: doris 不支持删除，需要增加逻辑删除字段
/*                    String tDimGdlkDimDelSql = "DELETE FROM " + Constants.DIM_DB + ".T_DIM_GDLK_DIM WHERE SYSTEM_KEY IN(\n" +
                            "SELECT SYSTEM_KEY \n" +
                            "FROM " + Constants.DIM_DB + ".T_DIM_GDLK_DIM\n" +
                            "WHERE T_ID = '" + tidOld + "')";
                    logger.info("tDimGdlkDimDelSql:" + tDimGdlkDimDelSql);*/
                    DorisUtils.excuteDorisInsert(stmt, tDimGdlkDimSql);
//                    DorisUtils.excuteDorisInsert(stmt, tDimGdlkDimDelSql);
                    //鲁雁行用户维表 T_DIM_LYX_DIM
                    String tDimLyxDimSql = "INSERT INTO " + Constants.DIM_DB + ".T_DIM_LYX_DIM(\n" +
                            "SYSTEM_KEY\n" +
                            ",T_ID\n" +
                            ",LY_USER_ID\n" +
                            ",LY_REGISTER_TIME\n" +
                            ",LY_CARD_NUMBER\n" +
                            ",LY_USER_LEVEL\n" +
                            ",LY_USER_STATUS\n" +
                            ",LY_REGISTER_STATUS\n" +
                            ",LY_VERIFY_STATUS\n" +
                            ",LY_VERIFY_METHOD\n" +
                            ",LY_VERIFY_TIME\n" +
                            ",LY_NEED_LIP_VERIFY\n" +
                            ",LY_LIP_VERIFY_RESULT\n" +
                            ",LY_LEVEL_CHANGE_DATE\n" +
                            ",LY_PREV_LEVEL\n" +
                            ",LY_LIFETIME_POINTS\n" +
                            ",LY_AVAILABLE_POINTS\n" +
                            ",UPDATE_TIME\n" +
                            ",CREATE_TIME)\n" +
                            "SELECT \n" +
                            "REPLACE(SYSTEM_KEY,'" + tidOld + "','" + tidEarly + "') AS SYSTEM_KEY\n" +
                            ",'" + tidEarly + "' AS T_ID\n" +
                            ",LY_USER_ID\n" +
                            ",LY_REGISTER_TIME\n" +
                            ",LY_CARD_NUMBER\n" +
                            ",LY_USER_LEVEL\n" +
                            ",LY_USER_STATUS\n" +
                            ",LY_REGISTER_STATUS\n" +
                            ",LY_VERIFY_STATUS\n" +
                            ",LY_VERIFY_METHOD\n" +
                            ",LY_VERIFY_TIME\n" +
                            ",LY_NEED_LIP_VERIFY\n" +
                            ",LY_LIP_VERIFY_RESULT\n" +
                            ",LY_LEVEL_CHANGE_DATE\n" +
                            ",LY_PREV_LEVEL\n" +
                            ",LY_LIFETIME_POINTS\n" +
                            ",LY_AVAILABLE_POINTS\n" +
                            ",UPDATE_TIME\n" +
                            ",CREATE_TIME\n" +
                            "FROM " + Constants.DIM_DB + ".T_DIM_LYX_DIM WHERE T_ID='" + tidOld + "'\n";
                    logger.info("tDimLyxDimSql:" + tDimLyxDimSql);
                    //TODO: doris 不支持删除，需要增加逻辑删除字段
/*                    String tDimLyxDimDelSql = "DELETE FROM " + Constants.DIM_DB + ".T_DIM_LYX_DIM WHERE SYSTEM_KEY IN(\n" +
                            "SELECT SYSTEM_KEY \n" +
                            "FROM " + Constants.DIM_DB + ".T_DIM_LYX_DIM\n" +
                            "WHERE T_ID = '" + tidOld + "')";
                    logger.info("tDimLyxDimDelSql:" + tDimLyxDimDelSql);*/
                    DorisUtils.excuteDorisInsert(stmt, tDimLyxDimSql);
//                    DorisUtils.excuteDorisInsert(stmt, tDimLyxDimDelSql);
                    //手机号维表 T_DIM_MOBILE_DIM
                    String tDimMobileDimSql = "INSERT INTO " + Constants.DIM_DB + ".T_DIM_MOBILE_DIM(\n" +
                            "SYSTEM_KEY\n" +
                            ",T_ID\n" +
                            ",MOBILE_NUMBER\n" +
                            ",IS_DIRECT_SALE_MOBILE\n" +
                            ",IS_LYX_REALNAME_MOBILE\n" +
                            ",IS_DOUYIN_CARD_PURCHASERS_MOBILE\n" +
                            ",IS_FREQUENT_FLYER_MOBILE\n" +
                            ",IS_HIGH_MOBILE_NUMBER\n" +
                            ",CURRENT_PHONE_HIGHEST_PRIORITY\n" +
                            ",IS_VIRTUAL_MOBILE\n" +
                            ",UPDATE_TIME\n" +
                            ",CREATE_TIME)\n" +
                            "SELECT \n" +
                            "REPLACE(SYSTEM_KEY,'" + tidOld + "','" + tidEarly + "') AS SYSTEM_KEY\n" +
                            ",'" + tidEarly + "' AS T_ID\n" +
                            ",MOBILE_NUMBER\n" +
                            ",IS_DIRECT_SALE_MOBILE\n" +
                            ",IS_LYX_REALNAME_MOBILE\n" +
                            ",IS_DOUYIN_CARD_PURCHASERS_MOBILE\n" +
                            ",IS_FREQUENT_FLYER_MOBILE\n" +
                            ",IS_HIGH_MOBILE_NUMBER\n" +
                            ",CURRENT_PHONE_HIGHEST_PRIORITY\n" +
                            ",IS_VIRTUAL_MOBILE\n" +
                            ",UPDATE_TIME\n" +
                            ",CREATE_TIME\n" +
                            "FROM " + Constants.DIM_DB + ".T_DIM_MOBILE_DIM WHERE T_ID='" + tidOld + "'\n";
                    logger.info("tDimMobileDimSql:" + tDimMobileDimSql);
                    //TODO: doris 不支持删除，需要增加逻辑删除字段
/*                    String tDimMobileDimDelSql = "DELETE FROM " + Constants.DIM_DB + ".T_DIM_MOBILE_DIM WHERE SYSTEM_KEY IN(\n" +
                            "SELECT SYSTEM_KEY \n" +
                            "FROM " + Constants.DIM_DB + ".T_DIM_MOBILE_DIM\n" +
                            "WHERE T_ID = '" + tidOld + "')";
                    logger.info("tDimMobileDimDelSql:" + tDimMobileDimDelSql);*/
                    DorisUtils.excuteDorisInsert(stmt, tDimMobileDimSql);
//                    DorisUtils.excuteDorisInsert(stmt, tDimMobileDimDelSql);
                    //用户维表 T_DIM_USER_DIM 和 用户画像数据源及更新时间表 T_DIM_USER_DIM_SUMMARY
                    mergeUserDimAndUserDimSummary(conn, stmt, tidEarly, tidOld);
                    tidList.remove(j--);
                    tidList.set(i, finalTid);
                }
            }
        }

        DorisUtils.close(conn, stmt, rs);
    }

    /**
     * 合并用户维表 T_DIM_USER_DIM 和 用户画像数据源及更新时间表 T_DIM_USER_DIM_SUMMARY
     */
    public static void mergeUserDimAndUserDimSummary(Connection conn, Statement stmt, String tidEarly, String tidOld) throws SQLException {

        //用户维表 T_DIM_USER_DIM 和 用户画像数据源及更新时间表 T_DIM_USER_DIM_SUMMARY
        String getUserDimSql = "SELECT \n" +
                " T1.PK_ID                                                            AS PK_ID\n" +
                " ,T1.CRM_CUSTOMER_ID                                                  AS CRM_CUSTOMER_ID\n" +
                ",T1.LY_MEMBER_ID                                                     AS LY_MEMBER_ID\n" +
                ",T1.LY_VIP_ID                                                        AS LY_VIP_ID\n" +
                ",T1.SYS_REGISTER_ID                                                  AS SYS_REGISTER_ID\n" +
                ",T1.FFP_REGISTER_ID                                                  AS FFP_REGISTER_ID\n" +
                ",T1.HISTORICAL_FFP_ID                                                AS HISTORICAL_FFP_ID\n" +
                ",T1.ALIPAY_ID                                                        AS ALIPAY_ID\n" +
                ",T1.WECHAT_ID                                                        AS WECHAT_ID\n" +
                ",T1.DOUYIN_ID                                                        AS DOUYIN_ID\n" +
                ",T1.CN_NAME                                                          AS CN_NAME\n" +
                ",T1.EN_NAME                                                          AS EN_NAME\n" +
                ",T1.SEX                                                              AS SEX\n" +
                ",T1.USER_TYPE                                                        AS USER_TYPE\n" +
                ",T1.BIRTHDAY                                                         AS BIRTHDAY\n" +
                ",T1.PROVINCE                                                         AS PROVINCE\n" +
                ",T1.CITY                                                             AS CITY\n" +
                ",T1.NATIONALITY                                                      AS NATIONALITY\n" +
                ",T1.ETHNICITY                                                        AS ETHNICITY\n" +
                ",T1.EMPLOYER                                                         AS EMPLOYER\n" +
                ",T1.MOBILE_PHONE                                                     AS MOBILE_PHONE\n" +
                ",T1.MOBILE_NUMBER_RELIABILITY                                        AS MOBILE_NUMBER_RELIABILITY\n" +
                ",T1.EMAIL                                                            AS EMAIL\n" +
                ",T1.ID_CARD                                                          AS ID_CARD\n" +
                ",T1.PASSPORT                                                         AS PASSPORT\n" +
                ",T1.SEAMAN_ID                                                        AS SEAMAN_ID\n" +
                ",T1.ALIEN_PERMIT                                                     AS ALIEN_PERMIT\n" +
                ",T1.DIPLOMATIC_STAFF_CERTIFICATE                                     AS DIPLOMATIC_STAFF_CERTIFICATE\n" +
                ",T1.PERMANENT_RESIDENT_ID                                            AS PERMANENT_RESIDENT_ID\n" +
                ",T1.CIVILIAN_STAFF_ID                                                AS CIVILIAN_STAFF_ID\n" +
                ",T1.STAFF_ID                                                         AS STAFF_ID\n" +
                ",T1.OFFICER_ID_CARD                                                  AS OFFICER_ID_CARD\n" +
                ",T1.ARMED_POLICE_OFFICER                                             AS ARMED_POLICE_OFFICER\n" +
                ",T1.ARMED_POLICE_SOLDIER                                             AS ARMED_POLICE_SOLDIER\n" +
                ",T1.CIVILIAN_OFFICIAL_ID                                             AS CIVILIAN_OFFICIAL_ID\n" +
                ",T1.CONSCRIPT_SOLDIER_ID                                             AS CONSCRIPT_SOLDIER_ID\n" +
                ",T1.NON_COMMISSIONED_OFFICER_ID                                      AS NON_COMMISSIONED_OFFICER_ID\n" +
                ",T1.HK_MACAO_RESIDENT_PERMIT                                         AS HK_MACAO_RESIDENT_PERMIT\n" +
                ",T1.TAIWAN_RESIDENT_TRAVEL_PERMIT                                    AS TAIWAN_RESIDENT_TRAVEL_PERMIT\n" +
                ",T1.BLIND_PASSENGER                                                  AS BLIND_PASSENGER\n" +
                ",T1.DEAF_PASSENGER                                                   AS DEAF_PASSENGER\n" +
                ",T1.IS_DIRECT_USER                                                   AS IS_DIRECT_USER\n" +
                ",T1.IS_OFFICIAL_WEBSITE_NON_REGISTERED                               AS IS_OFFICIAL_WEBSITE_NON_REGISTERED\n" +
                ",T1.IS_DOUYIN_CARD_PURCHASER                                         AS IS_DOUYIN_CARD_PURCHASER\n" +
                ",T1.DIRECT_USER_STATUS                                               AS DIRECT_USER_STATUS\n" +
                ",T1.DIRECT_REGISTER_DATE                                             AS DIRECT_REGISTER_DATE\n" +
                ",T1.DIRECT_LASTLOGIN_DATE                                            AS DIRECT_LASTLOGIN_DATE\n" +
                ",T1.DIRECT_VERIFIED_FLAG                                             AS DIRECT_VERIFIED_FLAG\n" +
                ",T1.DIRECT_VERIFY_DATE                                               AS DIRECT_VERIFY_DATE\n" +
                ",T1.IS_BLACKLIST_USER                                                AS IS_BLACKLIST_USER\n" +
                ",T1.STUDENT_FLAG                                                     AS STUDENT_FLAG\n" +
                ",T1.TEACHER_FLAG                                                     AS TEACHER_FLAG\n" +
                ",T1.AGENT_FLAG                                                       AS AGENT_FLAG\n" +
                ",T1.IS_KEY_ACCOUNT                                                   AS IS_KEY_ACCOUNT\n" +
                ",T1.KEY_ACCOUNT_NUMBER                                               AS KEY_ACCOUNT_NUMBER\n" +
                ",T1.IS_FREQUENT_TRAVELER                                             AS IS_FREQUENT_TRAVELER\n" +
                ",T1.FREQUENT_TRAVELER_CARDNO                                         AS FREQUENT_TRAVELER_CARDNO\n" +
                ",T1.FREQUENT_TRAVELER_LEVEL                                          AS FREQUENT_TRAVELER_LEVEL\n" +
                ",T1.YJ_CARD_NUMBER                                                   AS YJ_CARD_NUMBER\n" +
                ",T1.YJ_CARD_EXPIREDATE                                               AS YJ_CARD_EXPIREDATE\n" +
                ",T1.DEV_CHANNEL_ONE                                                  AS DEV_CHANNEL_ONE\n" +
                ",T1.DEV_CHANNEL_TWO                                                  AS DEV_CHANNEL_TWO\n" +
                ",T1.DEV_CHANNEL_THREE                                                AS DEV_CHANNEL_THREE\n" +
                ",T1.DEV_CHANNEL_FOUR                                                 AS DEV_CHANNEL_FOUR\n" +
                ",T1.FT_REGISTER_TIME                                                 AS FT_REGISTER_TIME\n" +
                ",T1.ACCEPT_SMS_MARKETING                                             AS ACCEPT_SMS_MARKETING\n" +
                ",T1.ACCEPT_EMAIL_MARKETING                                           AS ACCEPT_EMAIL_MARKETING\n" +
                ",T1.PARENT_FT_CARDNO                                                 AS PARENT_FT_CARDNO\n" +
                ",T1.IS_LY_USER                                                       AS IS_LY_USER\n" +
                ",T1.LY_REGISTER_TIME                                                 AS LY_REGISTER_TIME\n" +
                ",T1.LY_CARD_NUMBER                                                   AS LY_CARD_NUMBER\n" +
                ",T1.LY_USER_LEVEL                                                    AS LY_USER_LEVEL\n" +
                ",T1.LY_USER_STATUS                                                   AS LY_USER_STATUS\n" +
                ",T1.LY_REGISTER_STATUS                                               AS LY_REGISTER_STATUS\n" +
                ",T1.LY_VERIFY_STATUS                                                 AS LY_VERIFY_STATUS\n" +
                ",T1.LY_LIFETIME_POINTS                                               AS LY_LIFETIME_POINTS\n" +
                ",T1.LY_AVAILABLE_POINTS                                              AS LY_AVAILABLE_POINTS\n" +
                ",T1.IS_HIGH_TRAVELER                                                 AS IS_HIGH_TRAVELER\n" +
                ",T1.HIGH_TRAVELER_TYPE                                               AS HIGH_TRAVELER_TYPE\n" +
                ",T1.HIGH_TRAVELER_TIER                                               AS HIGH_TRAVELER_TIER\n" +
                ",T1.TIER_PRESTIGE_EXPIREDATE                                         AS TIER_PRESTIGE_EXPIREDATE\n" +
                ",T1.TIER_HONOR_EXPIREDATE                                            AS TIER_HONOR_EXPIREDATE\n" +
                ",T1.HIGH_TRAVELER_DS                                                 AS HIGH_TRAVELER_DS\n" +
                ",T1.IS_YJ_PERSONNEL                                                  AS IS_YJ_PERSONNEL\n" +
                ",T1.VIP_FLAG                                                         AS VIP_FLAG\n" +
                ",T1.CIP_FLAG                                                         AS CIP_FLAG\n" +
                ",T1.VVIP_FLAG                                                        AS VVIP_FLAG\n" +
                ",T1.FOOD_PREFERENCE                                                  AS FOOD_PREFERENCE\n" +
                ",T1.SEAT_PREFERENCE                                                  AS SEAT_PREFERENCE\n" +
                ",T1.TEMP_FOOD_PREFERENCE                                             AS TEMP_FOOD_PREFERENCE\n" +
                ",T1.TEMP_SEAT_PREFERENCE                                             AS TEMP_SEAT_PREFERENCE\n" +
                ",T1.BEVERAGE_PREFERENCE                                              AS BEVERAGE_PREFERENCE\n" +
                ",T1.LONG_TERM_SEAT_PREFERENCE                                        AS LONG_TERM_SEAT_PREFERENCE\n" +
                ",T1.FIRST_CLASS_LOUNGE_PREFERENCE                                    AS FIRST_CLASS_LOUNGE_PREFERENCE\n" +
                ",T1.MERGED_TO_USERID                                                 AS MERGED_TO_USERID\n" +
                ",T1.IS_VALID_USER                                                    AS IS_VALID_USER\n" +
                ",T1.IS_FREQUENT_FLYER_NUMBER_VERIFIED                                AS IS_FREQUENT_FLYER_NUMBER_VERIFIED\n" +
                ",T1.CREATE_TIME                                                      AS CREATE_TIME\n" +
                ",T2.CRM_CUSTOMER_ID_SOURCE                                                         AS CRM_CUSTOMER_ID_SOURCE\n" +
                ",T2.LY_VIP_ID_SOURCE                                                               AS LY_VIP_ID_SOURCE\n" +
                ",T2.LY_MEMBER_ID_SOURCE                                                            AS LY_MEMBER_ID_SOURCE\n" +
                ",T2.SYS_REGISTER_ID_SOURCE                                                         AS SYS_REGISTER_ID_SOURCE\n" +
                ",T2.FFP_REGISTER_ID_SOURCE                                                         AS FFP_REGISTER_ID_SOURCE\n" +
                ",T2.HISTORICAL_FFP_ID_SOURCE                                                       AS HISTORICAL_FFP_ID_SOURCE\n" +
                ",T2.ALIPAY_ID_SOURCE                                                               AS ALIPAY_ID_SOURCE\n" +
                ",T2.WECHAT_ID_SOURCE                                                               AS WECHAT_ID_SOURCE\n" +
                ",T2.DOUYIN_ID_SOURCE                                                               AS DOUYIN_ID_SOURCE\n" +
                ",T2.CN_NAME_SOURCE                                                                 AS CN_NAME_SOURCE\n" +
                ",T2.EN_NAME_SOURCE                                                                 AS EN_NAME_SOURCE\n" +
                ",T2.SEX_SOURCE                                                                     AS SEX_SOURCE\n" +
                ",T2.USER_TYPE_SOURCE                                                               AS USER_TYPE_SOURCE\n" +
                ",T2.BIRTHDAY_SOURCE                                                                AS BIRTHDAY_SOURCE\n" +
                ",T2.PROVINCE_SOURCE                                                                AS PROVINCE_SOURCE\n" +
                ",T2.CITY_SOURCE                                                                    AS CITY_SOURCE\n" +
                ",T2.NATIONALITY_SOURCE                                                             AS NATIONALITY_SOURCE\n" +
                ",T2.ETHNICITY_SOURCE                                                               AS ETHNICITY_SOURCE\n" +
                ",T2.EMPLOYER_SOURCE                                                                AS EMPLOYER_SOURCE\n" +
                ",T2.MOBILE_PHONE_SOURCE                                                            AS MOBILE_PHONE_SOURCE\n" +
                ",T2.MOBILE_NUMBER_RELIABILITY_SOURCE                                               AS MOBILE_NUMBER_RELIABILITY_SOURCE\n" +
                ",T2.EMAIL_SOURCE                                                                   AS EMAIL_SOURCE\n" +
                ",T2.ID_CARD_SOURCE                                                                 AS ID_CARD_SOURCE\n" +
                ",T2.PASSPORT_SOURCE                                                                AS PASSPORT_SOURCE\n" +
                ",T2.SEAMAN_ID_SOURCE                                                               AS SEAMAN_ID_SOURCE\n" +
                ",T2.ALIEN_PERMIT_SOURCE                                                            AS ALIEN_PERMIT_SOURCE\n" +
                ",T2.DIPLOMATIC_STAFF_CERTIFICATE_SOURCE                                            AS DIPLOMATIC_STAFF_CERTIFICATE_SOURCE\n" +
                ",T2.PERMANENT_RESIDENT_ID_SOURCE                                                   AS PERMANENT_RESIDENT_ID_SOURCE\n" +
                ",T2.CIVILIAN_STAFF_ID_SOURCE                                                       AS CIVILIAN_STAFF_ID_SOURCE\n" +
                ",T2.STAFF_ID_SOURCE                                                                AS STAFF_ID_SOURCE\n" +
                ",T2.OFFICER_ID_CARD_SOURCE                                                         AS OFFICER_ID_CARD_SOURCE\n" +
                ",T2.ARMED_POLICE_OFFICER_SOURCE                                                    AS ARMED_POLICE_OFFICER_SOURCE\n" +
                ",T2.ARMED_POLICE_SOLDIER_SOURCE                                                    AS ARMED_POLICE_SOLDIER_SOURCE\n" +
                ",T2.CIVILIAN_OFFICIAL_ID_SOURCE                                                    AS CIVILIAN_OFFICIAL_ID_SOURCE\n" +
                ",T2.CONSCRIPT_SOLDIER_ID_SOURCE                                                    AS CONSCRIPT_SOLDIER_ID_SOURCE\n" +
                ",T2.NON_COMMISSIONED_OFFICER_ID_SOURCE                                             AS NON_COMMISSIONED_OFFICER_ID_SOURCE\n" +
                ",T2.HK_MACAO_RESIDENT_PERMIT_SOURCE                                                AS HK_MACAO_RESIDENT_PERMIT_SOURCE\n" +
                ",T2.TAIWAN_RESIDENT_TRAVEL_PERMIT_SOURCE                                           AS TAIWAN_RESIDENT_TRAVEL_PERMIT_SOURCE\n" +
                ",T2.BLIND_PASSENGER_SOURCE                                                         AS BLIND_PASSENGER_SOURCE\n" +
                ",T2.DEAF_PASSENGER_SOURCE                                                          AS DEAF_PASSENGER_SOURCE\n" +
                ",T2.IS_DIRECT_USER_SOURCE                                                          AS IS_DIRECT_USER_SOURCE\n" +
                ",T2.IS_OFFICIAL_WEBSITE_NON_REGISTERED_SOURCE                                      AS IS_OFFICIAL_WEBSITE_NON_REGISTERED_SOURCE\n" +
                ",T2.IS_DOUYIN_CARD_PURCHASER_SOURCE                                                AS IS_DOUYIN_CARD_PURCHASER_SOURCE\n" +
                ",T2.DIRECT_USER_STATUS_SOURCE                                                      AS DIRECT_USER_STATUS_SOURCE\n" +
                ",T2.DIRECT_REGISTER_DATE_SOURCE                                                    AS DIRECT_REGISTER_DATE_SOURCE\n" +
                ",T2.DIRECT_LASTLOGIN_DATE_SOURCE                                                   AS DIRECT_LASTLOGIN_DATE_SOURCE\n" +
                ",T2.DIRECT_VERIFIED_FLAG_SOURCE                                                    AS DIRECT_VERIFIED_FLAG_SOURCE\n" +
                ",T2.DIRECT_VERIFY_DATE_SOURCE                                                      AS DIRECT_VERIFY_DATE_SOURCE\n" +
                ",T2.IS_BLACKLIST_USER_SOURCE                                                       AS IS_BLACKLIST_USER_SOURCE\n" +
                ",T2.STUDENT_FLAG_SOURCE                                                            AS STUDENT_FLAG_SOURCE\n" +
                ",T2.TEACHER_FLAG_SOURCE                                                            AS TEACHER_FLAG_SOURCE\n" +
                ",T2.AGENT_FLAG_SOURCE                                                              AS AGENT_FLAG_SOURCE\n" +
                ",T2.IS_KEY_ACCOUNT_SOURCE                                                          AS IS_KEY_ACCOUNT_SOURCE\n" +
                ",T2.KEY_ACCOUNT_NUMBER_SOURCE                                                      AS KEY_ACCOUNT_NUMBER_SOURCE\n" +
                ",T2.IS_FREQUENT_TRAVELER_SOURCE                                                    AS IS_FREQUENT_TRAVELER_SOURCE\n" +
                ",T2.FREQUENT_TRAVELER_CARDNO_SOURCE                                                AS FREQUENT_TRAVELER_CARDNO_SOURCE\n" +
                ",T2.FREQUENT_TRAVELER_LEVEL_SOURCE                                                 AS FREQUENT_TRAVELER_LEVEL_SOURCE\n" +
                ",T2.YJ_CARD_NUMBER_SOURCE                                                          AS YJ_CARD_NUMBER_SOURCE\n" +
                ",T2.YJ_CARD_EXPIREDATE_SOURCE                                                      AS YJ_CARD_EXPIREDATE_SOURCE\n" +
                ",T2.DEV_CHANNEL_ONE_SOURCE                                                         AS DEV_CHANNEL_ONE_SOURCE\n" +
                ",T2.DEV_CHANNEL_TWO_SOURCE                                                         AS DEV_CHANNEL_TWO_SOURCE\n" +
                ",T2.DEV_CHANNEL_THREE_SOURCE                                                       AS DEV_CHANNEL_THREE_SOURCE\n" +
                ",T2.DEV_CHANNEL_FOUR_SOURCE                                                        AS DEV_CHANNEL_FOUR_SOURCE\n" +
                ",T2.FT_REGISTER_TIME_SOURCE                                                        AS FT_REGISTER_TIME_SOURCE\n" +
                ",T2.ACCEPT_SMS_MARKETING_SOURCE                                                    AS ACCEPT_SMS_MARKETING_SOURCE\n" +
                ",T2.ACCEPT_EMAIL_MARKETING_SOURCE                                                  AS ACCEPT_EMAIL_MARKETING_SOURCE\n" +
                ",T2.PARENT_FT_CARDNO_SOURCE                                                        AS PARENT_FT_CARDNO_SOURCE\n" +
                ",T2.IS_LY_USER_SOURCE                                                              AS IS_LY_USER_SOURCE\n" +
                ",T2.LY_REGISTER_TIME_SOURCE                                                        AS LY_REGISTER_TIME_SOURCE\n" +
                ",T2.LY_CARD_NUMBER_SOURCE                                                          AS LY_CARD_NUMBER_SOURCE\n" +
                ",T2.LY_USER_LEVEL_SOURCE                                                           AS LY_USER_LEVEL_SOURCE\n" +
                ",T2.LY_USER_STATUS_SOURCE                                                          AS LY_USER_STATUS_SOURCE\n" +
                ",T2.LY_REGISTER_STATUS_SOURCE                                                      AS LY_REGISTER_STATUS_SOURCE\n" +
                ",T2.LY_VERIFY_STATUS_SOURCE                                                        AS LY_VERIFY_STATUS_SOURCE\n" +
                ",T2.LY_LIFETIME_POINTS_SOURCE                                                      AS LY_LIFETIME_POINTS_SOURCE\n" +
                ",T2.LY_AVAILABLE_POINTS_SOURCE                                                     AS LY_AVAILABLE_POINTS_SOURCE\n" +
                ",T2.IS_HIGH_TRAVELER_SOURCE                                                        AS IS_HIGH_TRAVELER_SOURCE\n" +
                ",T2.HIGH_TRAVELER_TYPE_SOURCE                                                      AS HIGH_TRAVELER_TYPE_SOURCE\n" +
                ",T2.HIGH_TRAVELER_TIER_SOURCE                                                      AS HIGH_TRAVELER_TIER_SOURCE\n" +
                ",T2.TIER_PRESTIGE_EXPIREDATE_SOURCE                                                AS TIER_PRESTIGE_EXPIREDATE_SOURCE\n" +
                ",T2.TIER_HONOR_EXPIREDATE_SOURCE                                                   AS TIER_HONOR_EXPIREDATE_SOURCE\n" +
                ",T2.HIGH_TRAVELER_DS_SOURCE                                                        AS HIGH_TRAVELER_DS_SOURCE\n" +
                ",T2.IS_YJ_PERSONNEL_SOURCE                                                         AS IS_YJ_PERSONNEL_SOURCE\n" +
                ",T2.VIP_FLAG_SOURCE                                                                AS VIP_FLAG_SOURCE\n" +
                ",T2.CIP_FLAG_SOURCE                                                                AS CIP_FLAG_SOURCE\n" +
                ",T2.VVIP_FLAG_SOURCE                                                               AS VVIP_FLAG_SOURCE\n" +
                ",T2.FOOD_PREFERENCE_SOURCE                                                         AS FOOD_PREFERENCE_SOURCE\n" +
                ",T2.SEAT_PREFERENCE_SOURCE                                                         AS SEAT_PREFERENCE_SOURCE\n" +
                ",T2.TEMP_FOOD_PREFERENCE_SOURCE                                                    AS TEMP_FOOD_PREFERENCE_SOURCE\n" +
                ",T2.TEMP_SEAT_PREFERENCE_SOURCE                                                    AS TEMP_SEAT_PREFERENCE_SOURCE\n" +
                ",T2.BEVERAGE_PREFERENCE_SOURCE                                                     AS BEVERAGE_PREFERENCE_SOURCE\n" +
                ",T2.LONG_TERM_SEAT_PREFERENCE_SOURCE                                               AS LONG_TERM_SEAT_PREFERENCE_SOURCE\n" +
                ",T2.FIRST_CLASS_LOUNGE_PREFERENCE_SOURCE                                           AS FIRST_CLASS_LOUNGE_PREFERENCE_SOURCE\n" +
                ",T2.MERGED_TO_USERID_SOURCE                                                        AS MERGED_TO_USERID_SOURCE\n" +
                ",T2.IS_VALID_USER_SOURCE                                                           AS IS_VALID_USER_SOURCE\n" +
                ",T2.IS_FREQUENT_FLYER_NUMBER_VERIFIED_SOURCE                                       AS IS_FREQUENT_FLYER_NUMBER_VERIFIED_SOURCE\n" +
                "FROM DIM_TEST.T_DIM_USER_DIM T1 \n" +
                "LEFT JOIN DIM_TEST.T_DIM_USER_DIM_SUMMARY T2 ON T1.PK_ID=T2.PK_ID\n" +
                "WHERE T1.PK_ID IN ('" + tidEarly + "','" + tidOld + "')\n";
        System.out.println(getUserDimSql);
        ResultSet userDimRS = DorisUtils.getDorisResult(stmt, getUserDimSql);

        //用户维表数据合并
        UserDimModel userDimModelOld = new UserDimModel();
        UserDimModel model = new UserDimModel();
        UserDimSummaryModel userDimSummaryModelOld = new UserDimSummaryModel();
        UserDimSummaryModel summaryModel = new UserDimSummaryModel();
        String currentDateTime = DateTimeUtils.getCurrentDateTime();
        while (userDimRS != null && userDimRS.next()) {
            String pkId = userDimRS.getString("PK_ID");
            String crmCustomerId = userDimRS.getString("CRM_CUSTOMER_ID");
            String lyVipId = userDimRS.getString("LY_VIP_ID");
            String lyMemberId = userDimRS.getString("LY_MEMBER_ID");
            String sysRegisterId = userDimRS.getString("SYS_REGISTER_ID");
            String ffpRegisterId = userDimRS.getString("FFP_REGISTER_ID");
            String historicalFfpId = userDimRS.getString("HISTORICAL_FFP_ID");
            String alipayId = userDimRS.getString("ALIPAY_ID");
            String wechatId = userDimRS.getString("WECHAT_ID");
            String douyinId = userDimRS.getString("DOUYIN_ID");
            String cnName = userDimRS.getString("CN_NAME");
            String enName = userDimRS.getString("EN_NAME");
            String sex = userDimRS.getString("SEX");
            String userType = userDimRS.getString("USER_TYPE");
            String birthday = userDimRS.getString("BIRTHDAY");
            String province = userDimRS.getString("PROVINCE");
            String city = userDimRS.getString("CITY");
            String nationality = userDimRS.getString("NATIONALITY");
            String ethnicity = userDimRS.getString("ETHNICITY");
            String employer = userDimRS.getString("EMPLOYER");
            String mobilePhone = userDimRS.getString("MOBILE_PHONE");
            String mobileNumberReliability = userDimRS.getString("MOBILE_NUMBER_RELIABILITY");
            String email = userDimRS.getString("EMAIL");
            String idCard = userDimRS.getString("ID_CARD");
            String passport = userDimRS.getString("PASSPORT");
            String seamanId = userDimRS.getString("SEAMAN_ID");
            String alienPermit = userDimRS.getString("ALIEN_PERMIT");
            String diplomaticStaffCertificate = userDimRS.getString("DIPLOMATIC_STAFF_CERTIFICATE");
            String permanentResidentId = userDimRS.getString("PERMANENT_RESIDENT_ID");
            String civilianStaffId = userDimRS.getString("CIVILIAN_STAFF_ID");
            String staffId = userDimRS.getString("STAFF_ID");
            String officerIdCard = userDimRS.getString("OFFICER_ID_CARD");
            String armedPoliceOfficer = userDimRS.getString("ARMED_POLICE_OFFICER");
            String armedPoliceSoldier = userDimRS.getString("ARMED_POLICE_SOLDIER");
            String civilianOfficialId = userDimRS.getString("CIVILIAN_OFFICIAL_ID");
            String conscriptSoldierId = userDimRS.getString("CONSCRIPT_SOLDIER_ID");
            String nonCommissionedOfficerId = userDimRS.getString("NON_COMMISSIONED_OFFICER_ID");
            String hkMacaoResidentPermit = userDimRS.getString("HK_MACAO_RESIDENT_PERMIT");
            String taiwanResidentTravelPermit = userDimRS.getString("TAIWAN_RESIDENT_TRAVEL_PERMIT");
            Boolean blindPassenger = userDimRS.getBoolean("BLIND_PASSENGER");
            Boolean deafPassenger = userDimRS.getBoolean("DEAF_PASSENGER");
            Boolean isDirectUser = userDimRS.getBoolean("IS_DIRECT_USER");
            Boolean isOfficialWebsiteNonRegistered = userDimRS.getBoolean("IS_OFFICIAL_WEBSITE_NON_REGISTERED");
            Boolean isDouyinCardPurchaser = userDimRS.getBoolean("IS_DOUYIN_CARD_PURCHASER");
            String directUserStatus = userDimRS.getString("DIRECT_USER_STATUS");
            String directRegisterDate = userDimRS.getString("DIRECT_REGISTER_DATE");
            String directLastloginDate = userDimRS.getString("DIRECT_LASTLOGIN_DATE");
            Boolean directVerifiedFlag = userDimRS.getBoolean("DIRECT_VERIFIED_FLAG");
            String directVerifyDate = userDimRS.getString("DIRECT_VERIFY_DATE");
            Boolean isBlacklistUser = userDimRS.getBoolean("IS_BLACKLIST_USER");
            Boolean studentFlag = userDimRS.getBoolean("STUDENT_FLAG");
            Boolean teacherFlag = userDimRS.getBoolean("TEACHER_FLAG");
            Boolean agentFlag = userDimRS.getBoolean("AGENT_FLAG");
            Boolean isKeyAccount = userDimRS.getBoolean("IS_KEY_ACCOUNT");
            String keyAccountNumber = userDimRS.getString("KEY_ACCOUNT_NUMBER");
            Boolean isFrequentTraveler = userDimRS.getBoolean("IS_FREQUENT_TRAVELER");
            String frequentTravelerCardno = userDimRS.getString("FREQUENT_TRAVELER_CARDNO");
            String frequentTravelerLevel = userDimRS.getString("FREQUENT_TRAVELER_LEVEL");
            String yjCardNumber = userDimRS.getString("YJ_CARD_NUMBER");
            String yjCardExpiredate = userDimRS.getString("YJ_CARD_EXPIREDATE");
            String devChannelOne = userDimRS.getString("DEV_CHANNEL_ONE");
            String devChannelTwo = userDimRS.getString("DEV_CHANNEL_TWO");
            String devChannelThree = userDimRS.getString("DEV_CHANNEL_THREE");
            String devChannelFour = userDimRS.getString("DEV_CHANNEL_FOUR");
            String ftRegisterTime = userDimRS.getString("FT_REGISTER_TIME");
            Boolean acceptSmsMarketing = userDimRS.getBoolean("ACCEPT_SMS_MARKETING");
            Boolean acceptEmailMarketing = userDimRS.getBoolean("ACCEPT_EMAIL_MARKETING");
            String parentFtCardno = userDimRS.getString("PARENT_FT_CARDNO");
            Boolean isLyUser = userDimRS.getBoolean("IS_LY_USER");
            String lyRegisterTime = userDimRS.getString("LY_REGISTER_TIME");
            String lyCardNumber = userDimRS.getString("LY_CARD_NUMBER");
            String lyUserLevel = userDimRS.getString("LY_USER_LEVEL");
            String lyUserStatus = userDimRS.getString("LY_USER_STATUS");
            String lyRegisterStatus = userDimRS.getString("LY_REGISTER_STATUS");
            String lyVerifyStatus = userDimRS.getString("LY_VERIFY_STATUS");
            Integer lyLifetimePoints = userDimRS.getInt("LY_LIFETIME_POINTS");
            Integer lyAvailablePoints = userDimRS.getInt("LY_AVAILABLE_POINTS");
            Boolean isHighTraveler = userDimRS.getBoolean("IS_HIGH_TRAVELER");
            Boolean highTravelerType = userDimRS.getBoolean("HIGH_TRAVELER_TYPE");
            String highTravelerTier = userDimRS.getString("HIGH_TRAVELER_TIER");
            String tierPrestigeExpiredate = userDimRS.getString("TIER_PRESTIGE_EXPIREDATE");
            String tierHonorExpiredate = userDimRS.getString("TIER_HONOR_EXPIREDATE");
            String highTravelerDs = userDimRS.getString("HIGH_TRAVELER_DS");
            Boolean isYjPersonnel = userDimRS.getBoolean("IS_YJ_PERSONNEL");
            Boolean vipFlag = userDimRS.getBoolean("VIP_FLAG");
            Boolean cipFlag = userDimRS.getBoolean("CIP_FLAG");
            Boolean vvipFlag = userDimRS.getBoolean("VVIP_FLAG");
            String foodPreference = userDimRS.getString("FOOD_PREFERENCE");
            String seatPreference = userDimRS.getString("SEAT_PREFERENCE");
            String tempFoodPreference = userDimRS.getString("TEMP_FOOD_PREFERENCE");
            String tempSeatPreference = userDimRS.getString("TEMP_SEAT_PREFERENCE");
            String beveragePreference = userDimRS.getString("BEVERAGE_PREFERENCE");
            String longTermSeatPreference = userDimRS.getString("LONG_TERM_SEAT_PREFERENCE");
            String firstClassLoungePreference = userDimRS.getString("FIRST_CLASS_LOUNGE_PREFERENCE");
            String mergedToUserid = userDimRS.getString("MERGED_TO_USERID");
            int isValidUser = userDimRS.getInt("IS_VALID_USER");
            Boolean isFrequentFlyerNumberVerified = userDimRS.getBoolean("IS_FREQUENT_FLYER_NUMBER_VERIFIED");
            String createTime = userDimRS.getString("CREATE_TIME");
            String crmCustomerIdSource = userDimRS.getString("CRM_CUSTOMER_ID_SOURCE");
            String lyVipIdSource = userDimRS.getString("LY_VIP_ID_SOURCE");
            String lyMemberIdSource = userDimRS.getString("LY_MEMBER_ID_SOURCE");
            String sysRegisterIdSource = userDimRS.getString("SYS_REGISTER_ID_SOURCE");
            String ffpRegisterIdSource = userDimRS.getString("FFP_REGISTER_ID_SOURCE");
            String historicalFfpIdSource = userDimRS.getString("HISTORICAL_FFP_ID_SOURCE");
            String alipayIdSource = userDimRS.getString("ALIPAY_ID_SOURCE");
            String wechatIdSource = userDimRS.getString("WECHAT_ID_SOURCE");
            String douyinIdSource = userDimRS.getString("DOUYIN_ID_SOURCE");
            String cnNameSource = userDimRS.getString("CN_NAME_SOURCE");
            String enNameSource = userDimRS.getString("EN_NAME_SOURCE");
            String sexSource = userDimRS.getString("SEX_SOURCE");
            String userTypeSource = userDimRS.getString("USER_TYPE_SOURCE");
            String birthdaySource = userDimRS.getString("BIRTHDAY_SOURCE");
            String provinceSource = userDimRS.getString("PROVINCE_SOURCE");
            String citySource = userDimRS.getString("CITY_SOURCE");
            String nationalitySource = userDimRS.getString("NATIONALITY_SOURCE");
            String ethnicitySource = userDimRS.getString("ETHNICITY_SOURCE");
            String employerSource = userDimRS.getString("EMPLOYER_SOURCE");
            String mobilePhoneSource = userDimRS.getString("MOBILE_PHONE_SOURCE");
            String mobileNumberReliabilitySource = userDimRS.getString("MOBILE_NUMBER_RELIABILITY_SOURCE");
            String emailSource = userDimRS.getString("EMAIL_SOURCE");
            String idCardSource = userDimRS.getString("ID_CARD_SOURCE");
            String passportSource = userDimRS.getString("PASSPORT_SOURCE");
            String seamanIdSource = userDimRS.getString("SEAMAN_ID_SOURCE");
            String alienPermitSource = userDimRS.getString("ALIEN_PERMIT_SOURCE");
            String diplomaticStaffCertificateSource = userDimRS.getString("DIPLOMATIC_STAFF_CERTIFICATE_SOURCE");
            String permanentResidentIdSource = userDimRS.getString("PERMANENT_RESIDENT_ID_SOURCE");
            String civilianStaffIdSource = userDimRS.getString("CIVILIAN_STAFF_ID_SOURCE");
            String staffIdSource = userDimRS.getString("STAFF_ID_SOURCE");
            String officerIdCardSource = userDimRS.getString("OFFICER_ID_CARD_SOURCE");
            String armedPoliceOfficerSource = userDimRS.getString("ARMED_POLICE_OFFICER_SOURCE");
            String armedPoliceSoldierSource = userDimRS.getString("ARMED_POLICE_SOLDIER_SOURCE");
            String civilianOfficialIdSource = userDimRS.getString("CIVILIAN_OFFICIAL_ID_SOURCE");
            String conscriptSoldierIdSource = userDimRS.getString("CONSCRIPT_SOLDIER_ID_SOURCE");
            String nonCommissionedOfficerIdSource = userDimRS.getString("NON_COMMISSIONED_OFFICER_ID_SOURCE");
            String hkMacaoResidentPermitSource = userDimRS.getString("HK_MACAO_RESIDENT_PERMIT_SOURCE");
            String taiwanResidentTravelPermitSource = userDimRS.getString("TAIWAN_RESIDENT_TRAVEL_PERMIT_SOURCE");
            String blindPassengerSource = userDimRS.getString("BLIND_PASSENGER_SOURCE");
            String deafPassengerSource = userDimRS.getString("DEAF_PASSENGER_SOURCE");
            String isDirectUserSource = userDimRS.getString("IS_DIRECT_USER_SOURCE");
            String isOfficialWebsiteNonRegisteredSource = userDimRS.getString("IS_OFFICIAL_WEBSITE_NON_REGISTERED_SOURCE");
            String isDouyinCardPurchaserSource = userDimRS.getString("IS_DOUYIN_CARD_PURCHASER_SOURCE");
            String directUserStatusSource = userDimRS.getString("DIRECT_USER_STATUS_SOURCE");
            String directRegisterDateSource = userDimRS.getString("DIRECT_REGISTER_DATE_SOURCE");
            String directLastloginDateSource = userDimRS.getString("DIRECT_LASTLOGIN_DATE_SOURCE");
            String directVerifiedFlagSource = userDimRS.getString("DIRECT_VERIFIED_FLAG_SOURCE");
            String directVerifyDateSource = userDimRS.getString("DIRECT_VERIFY_DATE_SOURCE");
            String isBlacklistUserSource = userDimRS.getString("IS_BLACKLIST_USER_SOURCE");
            String studentFlagSource = userDimRS.getString("STUDENT_FLAG_SOURCE");
            String teacherFlagSource = userDimRS.getString("TEACHER_FLAG_SOURCE");
            String agentFlagSource = userDimRS.getString("AGENT_FLAG_SOURCE");
            String isKeyAccountSource = userDimRS.getString("IS_KEY_ACCOUNT_SOURCE");
            String keyAccountNumberSource = userDimRS.getString("KEY_ACCOUNT_NUMBER_SOURCE");
            String isFrequentTravelerSource = userDimRS.getString("IS_FREQUENT_TRAVELER_SOURCE");
            String frequentTravelerCardnoSource = userDimRS.getString("FREQUENT_TRAVELER_CARDNO_SOURCE");
            String frequentTravelerLevelSource = userDimRS.getString("FREQUENT_TRAVELER_LEVEL_SOURCE");
            String yjCardNumberSource = userDimRS.getString("YJ_CARD_NUMBER_SOURCE");
            String yjCardExpiredateSource = userDimRS.getString("YJ_CARD_EXPIREDATE_SOURCE");
            String devChannelOneSource = userDimRS.getString("DEV_CHANNEL_ONE_SOURCE");
            String devChannelTwoSource = userDimRS.getString("DEV_CHANNEL_TWO_SOURCE");
            String devChannelThreeSource = userDimRS.getString("DEV_CHANNEL_THREE_SOURCE");
            String devChannelFourSource = userDimRS.getString("DEV_CHANNEL_FOUR_SOURCE");
            String ftRegisterTimeSource = userDimRS.getString("FT_REGISTER_TIME_SOURCE");
            String acceptSmsMarketingSource = userDimRS.getString("ACCEPT_SMS_MARKETING_SOURCE");
            String acceptEmailMarketingSource = userDimRS.getString("ACCEPT_EMAIL_MARKETING_SOURCE");
            String parentFtCardnoSource = userDimRS.getString("PARENT_FT_CARDNO_SOURCE");
            String isLyUserSource = userDimRS.getString("IS_LY_USER_SOURCE");
            String lyRegisterTimeSource = userDimRS.getString("LY_REGISTER_TIME_SOURCE");
            String lyCardNumberSource = userDimRS.getString("LY_CARD_NUMBER_SOURCE");
            String lyUserLevelSource = userDimRS.getString("LY_USER_LEVEL_SOURCE");
            String lyUserStatusSource = userDimRS.getString("LY_USER_STATUS_SOURCE");
            String lyRegisterStatusSource = userDimRS.getString("LY_REGISTER_STATUS_SOURCE");
            String lyVerifyStatusSource = userDimRS.getString("LY_VERIFY_STATUS_SOURCE");
            String lyLifetimePointsSource = userDimRS.getString("LY_LIFETIME_POINTS_SOURCE");
            String lyAvailablePointsSource = userDimRS.getString("LY_AVAILABLE_POINTS_SOURCE");
            String isHighTravelerSource = userDimRS.getString("IS_HIGH_TRAVELER_SOURCE");
            String highTravelerTypeSource = userDimRS.getString("HIGH_TRAVELER_TYPE_SOURCE");
            String highTravelerTierSource = userDimRS.getString("HIGH_TRAVELER_TIER_SOURCE");
            String tierPrestigeExpiredateSource = userDimRS.getString("TIER_PRESTIGE_EXPIREDATE_SOURCE");
            String tierHonorExpiredateSource = userDimRS.getString("TIER_HONOR_EXPIREDATE_SOURCE");
            String highTravelerDsSource = userDimRS.getString("HIGH_TRAVELER_DS_SOURCE");
            String isYjPersonnelSource = userDimRS.getString("IS_YJ_PERSONNEL_SOURCE");
            String vipFlagSource = userDimRS.getString("VIP_FLAG_SOURCE");
            String cipFlagSource = userDimRS.getString("CIP_FLAG_SOURCE");
            String vvipFlagSource = userDimRS.getString("VVIP_FLAG_SOURCE");
            String foodPreferenceSource = userDimRS.getString("FOOD_PREFERENCE_SOURCE");
            String seatPreferenceSource = userDimRS.getString("SEAT_PREFERENCE_SOURCE");
            String tempFoodPreferenceSource = userDimRS.getString("TEMP_FOOD_PREFERENCE_SOURCE");
            String tempSeatPreferenceSource = userDimRS.getString("TEMP_SEAT_PREFERENCE_SOURCE");
            String beveragePreferenceSource = userDimRS.getString("BEVERAGE_PREFERENCE_SOURCE");
            String longTermSeatPreferenceSource = userDimRS.getString("LONG_TERM_SEAT_PREFERENCE_SOURCE");
            String firstClassLoungePreferenceSource = userDimRS.getString("FIRST_CLASS_LOUNGE_PREFERENCE_SOURCE");
            String mergedToUseridSource = userDimRS.getString("MERGED_TO_USERID_SOURCE");
            String isFrequentFlyerNumberVerifiedSource = userDimRS.getString("IS_FREQUENT_FLYER_NUMBER_VERIFIED_SOURCE");
            if (pkId.equals(tidOld)) {
                userDimModelOld.setPkId(pkId);
                userDimModelOld.setCrmCustomerId(crmCustomerId);
                userDimModelOld.setLyVipId(lyVipId);
                userDimModelOld.setLyMemberId(lyMemberId);
                userDimModelOld.setSysRegisterId(sysRegisterId);
                userDimModelOld.setFfpRegisterId(ffpRegisterId);
                userDimModelOld.setHistoricalFfpId(historicalFfpId);
                userDimModelOld.setAlipayId(alipayId);
                userDimModelOld.setWechatId(wechatId);
                userDimModelOld.setDouyinId(douyinId);
                userDimModelOld.setCnName(cnName);
                userDimModelOld.setEnName(enName);
                userDimModelOld.setSex(sex);
                userDimModelOld.setUserType(userType);
                userDimModelOld.setBirthday(birthday);
                userDimModelOld.setProvince(province);
                userDimModelOld.setCity(city);
                userDimModelOld.setNationality(nationality);
                userDimModelOld.setEthnicity(ethnicity);
                userDimModelOld.setEmployer(employer);
                userDimModelOld.setMobilePhone(mobilePhone);
                userDimModelOld.setMobileNumberReliability(mobileNumberReliability);
                userDimModelOld.setEmail(email);
                userDimModelOld.setIdCard(idCard);
                userDimModelOld.setPassport(passport);
                userDimModelOld.setSeamanId(seamanId);
                userDimModelOld.setAlienPermit(alienPermit);
                userDimModelOld.setDiplomaticStaffCertificate(diplomaticStaffCertificate);
                userDimModelOld.setPermanentResidentId(permanentResidentId);
                userDimModelOld.setCivilianStaffId(civilianStaffId);
                userDimModelOld.setStaffId(staffId);
                userDimModelOld.setOfficerIdCard(officerIdCard);
                userDimModelOld.setArmedPoliceOfficer(armedPoliceOfficer);
                userDimModelOld.setArmedPoliceSoldier(armedPoliceSoldier);
                userDimModelOld.setCivilianOfficialId(civilianOfficialId);
                userDimModelOld.setConscriptSoldierId(conscriptSoldierId);
                userDimModelOld.setNonCommissionedOfficerId(nonCommissionedOfficerId);
                userDimModelOld.setHkMacaoResidentPermit(hkMacaoResidentPermit);
                userDimModelOld.setTaiwanResidentTravelPermit(taiwanResidentTravelPermit);
                userDimModelOld.setBlindPassenger(blindPassenger);
                userDimModelOld.setDeafPassenger(deafPassenger);
                userDimModelOld.setDirectUser(isDirectUser);
                userDimModelOld.setOfficialWebsiteNonRegistered(isOfficialWebsiteNonRegistered);
                userDimModelOld.setDouyinCardPurchaser(isDouyinCardPurchaser);
                userDimModelOld.setDirectUserStatus(directUserStatus);
                userDimModelOld.setDirectRegisterDate(directRegisterDate);
                userDimModelOld.setDirectLastloginDate(directLastloginDate);
                userDimModelOld.setDirectVerifiedFlag(directVerifiedFlag);
                userDimModelOld.setDirectVerifyDate(directVerifyDate);
                userDimModelOld.setBlacklistUser(isBlacklistUser);
                userDimModelOld.setStudentFlag(studentFlag);
                userDimModelOld.setTeacherFlag(teacherFlag);
                userDimModelOld.setAgentFlag(agentFlag);
                userDimModelOld.setKeyAccount(isKeyAccount);
                userDimModelOld.setKeyAccountNumber(keyAccountNumber);
                userDimModelOld.setFrequentTraveler(isFrequentTraveler);
                userDimModelOld.setFrequentTravelerCardno(frequentTravelerCardno);
                userDimModelOld.setFrequentTravelerLevel(frequentTravelerLevel);
                userDimModelOld.setYjCardNumber(yjCardNumber);
                userDimModelOld.setYjCardExpiredate(yjCardExpiredate);
                userDimModelOld.setDevChannelOne(devChannelOne);
                userDimModelOld.setDevChannelTwo(devChannelTwo);
                userDimModelOld.setDevChannelThree(devChannelThree);
                userDimModelOld.setDevChannelFour(devChannelFour);
                userDimModelOld.setFtRegisterTime(ftRegisterTime);
                userDimModelOld.setAcceptSmsMarketing(acceptSmsMarketing);
                userDimModelOld.setAcceptEmailMarketing(acceptEmailMarketing);
                userDimModelOld.setParentFtCardno(parentFtCardno);
                userDimModelOld.setLyUser(isLyUser);
                userDimModelOld.setLyRegisterTime(lyRegisterTime);
                userDimModelOld.setLyCardNumber(lyCardNumber);
                userDimModelOld.setLyUserLevel(lyUserLevel);
                userDimModelOld.setLyUserStatus(lyUserStatus);
                userDimModelOld.setLyRegisterStatus(lyRegisterStatus);
                userDimModelOld.setLyVerifyStatus(lyVerifyStatus);
                userDimModelOld.setLyLifetimePoints(lyLifetimePoints);
                userDimModelOld.setLyAvailablePoints(lyAvailablePoints);
                userDimModelOld.setHighTraveler(isHighTraveler);
                userDimModelOld.setHighTravelerType("");
                userDimModelOld.setHighTravelerTier(highTravelerTier);
                userDimModelOld.setTierPrestigeExpiredate(tierPrestigeExpiredate);
                userDimModelOld.setTierHonorExpiredate(tierHonorExpiredate);
                userDimModelOld.setHighTravelerDs(highTravelerDs);
                userDimModelOld.setYjPersonnel(isYjPersonnel);
                userDimModelOld.setVipFlag(vipFlag);
                userDimModelOld.setCipFlag(cipFlag);
                userDimModelOld.setVvipFlag(vvipFlag);
                userDimModelOld.setFoodPreference(foodPreference);
                userDimModelOld.setSeatPreference(seatPreference);
                userDimModelOld.setTempFoodPreference(tempFoodPreference);
                userDimModelOld.setTempSeatPreference(tempSeatPreference);
                userDimModelOld.setBeveragePreference(beveragePreference);
                userDimModelOld.setLongTermSeatPreference(longTermSeatPreference);
                userDimModelOld.setFirstClassLoungePreference(firstClassLoungePreference);
                userDimModelOld.setMergedToUserid(mergedToUserid);
                userDimModelOld.setValidUser(isValidUser);
                userDimModelOld.setFrequentFlyerNumberVerified(isFrequentFlyerNumberVerified);
                userDimModelOld.setCreateTime(createTime);
                userDimSummaryModelOld.setCrmCustomerIdSource(crmCustomerIdSource);
                userDimSummaryModelOld.setLyVipIdSource(lyVipIdSource);
                userDimSummaryModelOld.setLyMemberIdSource(lyMemberIdSource);
                userDimSummaryModelOld.setSysRegisterIdSource(sysRegisterIdSource);
                userDimSummaryModelOld.setFfpRegisterIdSource(ffpRegisterIdSource);
                userDimSummaryModelOld.setHistoricalFfpIdSource(historicalFfpIdSource);
                userDimSummaryModelOld.setAlipayIdSource(alipayIdSource);
                userDimSummaryModelOld.setWechatIdSource(wechatIdSource);
                userDimSummaryModelOld.setDouyinIdSource(douyinIdSource);
                userDimSummaryModelOld.setCnNameSource(cnNameSource);
                userDimSummaryModelOld.setEnNameSource(enNameSource);
                userDimSummaryModelOld.setSexSource(sexSource);
                userDimSummaryModelOld.setUserTypeSource(userTypeSource);
                userDimSummaryModelOld.setBirthdaySource(birthdaySource);
                userDimSummaryModelOld.setProvinceSource(provinceSource);
                userDimSummaryModelOld.setCitySource(citySource);
                userDimSummaryModelOld.setNationalitySource(nationalitySource);
                userDimSummaryModelOld.setEthnicitySource(ethnicitySource);
                userDimSummaryModelOld.setEmployerSource(employerSource);
                userDimSummaryModelOld.setMobilePhoneSource(mobilePhoneSource);
                userDimSummaryModelOld.setMobileNumberReliabilitySource(mobileNumberReliabilitySource);
                userDimSummaryModelOld.setEmailSource(emailSource);
                userDimSummaryModelOld.setIdCardSource(idCardSource);
                userDimSummaryModelOld.setPassportSource(passportSource);
                userDimSummaryModelOld.setSeamanIdSource(seamanIdSource);
                userDimSummaryModelOld.setAlienPermitSource(alienPermitSource);
                userDimSummaryModelOld.setDiplomaticStaffCertificateSource(diplomaticStaffCertificateSource);
                userDimSummaryModelOld.setPermanentResidentIdSource(permanentResidentIdSource);
                userDimSummaryModelOld.setCivilianStaffIdSource(civilianStaffIdSource);
                userDimSummaryModelOld.setStaffIdSource(staffIdSource);
                userDimSummaryModelOld.setOfficerIdCardSource(officerIdCardSource);
                userDimSummaryModelOld.setArmedPoliceOfficerSource(armedPoliceOfficerSource);
                userDimSummaryModelOld.setArmedPoliceSoldierSource(armedPoliceSoldierSource);
                userDimSummaryModelOld.setCivilianOfficialIdSource(civilianOfficialIdSource);
                userDimSummaryModelOld.setConscriptSoldierIdSource(conscriptSoldierIdSource);
                userDimSummaryModelOld.setNonCommissionedOfficerIdSource(nonCommissionedOfficerIdSource);
                userDimSummaryModelOld.setHkMacaoResidentPermitSource(hkMacaoResidentPermitSource);
                userDimSummaryModelOld.setTaiwanResidentTravelPermitSource(taiwanResidentTravelPermitSource);
                userDimSummaryModelOld.setBlindPassengerSource(blindPassengerSource);
                userDimSummaryModelOld.setDeafPassengerSource(deafPassengerSource);
                userDimSummaryModelOld.setIsDirectUserSource(isDirectUserSource);
                userDimSummaryModelOld.setIsOfficialWebsiteNonRegisteredSource(isOfficialWebsiteNonRegisteredSource);
                userDimSummaryModelOld.setIsDouyinCardPurchaserSource(isDouyinCardPurchaserSource);
                userDimSummaryModelOld.setDirectUserStatusSource(directUserStatusSource);
                userDimSummaryModelOld.setDirectRegisterDateSource(directRegisterDateSource);
                userDimSummaryModelOld.setDirectLastLoginDateSource(directLastloginDateSource);
                userDimSummaryModelOld.setDirectVerifiedFlagSource(directVerifiedFlagSource);
                userDimSummaryModelOld.setDirectVerifyDateSource(directVerifyDateSource);
                userDimSummaryModelOld.setIsBlacklistUserSource(isBlacklistUserSource);
                userDimSummaryModelOld.setStudentFlagSource(studentFlagSource);
                userDimSummaryModelOld.setTeacherFlagSource(teacherFlagSource);
                userDimSummaryModelOld.setAgentFlagSource(agentFlagSource);
                userDimSummaryModelOld.setIsKeyAccountSource(isKeyAccountSource);
                userDimSummaryModelOld.setKeyAccountNumberSource(keyAccountNumberSource);
                userDimSummaryModelOld.setIsFrequentTravelerSource(isFrequentTravelerSource);
                userDimSummaryModelOld.setFrequentTravelerCardnoSource(frequentTravelerCardnoSource);
                userDimSummaryModelOld.setFrequentTravelerLevelSource(frequentTravelerLevelSource);
                userDimSummaryModelOld.setYjCardNumberSource(yjCardNumberSource);
                userDimSummaryModelOld.setYjCardExpiredateSource(yjCardExpiredateSource);
                userDimSummaryModelOld.setDevChannelOneSource(devChannelOneSource);
                userDimSummaryModelOld.setDevChannelTwoSource(devChannelTwoSource);
                userDimSummaryModelOld.setDevChannelThreeSource(devChannelThreeSource);
                userDimSummaryModelOld.setDevChannelFourSource(devChannelFourSource);
                userDimSummaryModelOld.setFtRegisterTimeSource(ftRegisterTimeSource);
                userDimSummaryModelOld.setAcceptSmsMarketingSource(acceptSmsMarketingSource);
                userDimSummaryModelOld.setAcceptEmailMarketingSource(acceptEmailMarketingSource);
                userDimSummaryModelOld.setParentFtCardnoSource(parentFtCardnoSource);
                userDimSummaryModelOld.setIsLyUserSource(isLyUserSource);
                userDimSummaryModelOld.setLyRegisterTimeSource(lyRegisterTimeSource);
                userDimSummaryModelOld.setLyCardNumberSource(lyCardNumberSource);
                userDimSummaryModelOld.setLyUserLevelSource(lyUserLevelSource);
                userDimSummaryModelOld.setLyUserStatusSource(lyUserStatusSource);
                userDimSummaryModelOld.setLyRegisterStatusSource(lyRegisterStatusSource);
                userDimSummaryModelOld.setLyVerifyStatusSource(lyVerifyStatusSource);
                userDimSummaryModelOld.setLyLifetimePointsSource(lyLifetimePointsSource);
                userDimSummaryModelOld.setLyAvailablePointsSource(lyAvailablePointsSource);
                userDimSummaryModelOld.setIsHighTravelerSource(isHighTravelerSource);
                userDimSummaryModelOld.setHighTravelerTypeSource(highTravelerTypeSource);
                userDimSummaryModelOld.setHighTravelerTierSource(highTravelerTierSource);
                userDimSummaryModelOld.setTierPrestigeExpiredateSource(tierPrestigeExpiredateSource);
                userDimSummaryModelOld.setTierHonorExpiredateSource(tierHonorExpiredateSource);
                userDimSummaryModelOld.setHighTravelerDsSource(highTravelerDsSource);
                userDimSummaryModelOld.setIsYjPersonnelSource(isYjPersonnelSource);
                userDimSummaryModelOld.setVipFlagSource(vipFlagSource);
                userDimSummaryModelOld.setCipFlagSource(cipFlagSource);
                userDimSummaryModelOld.setVvipFlagSource(vvipFlagSource);
                userDimSummaryModelOld.setFoodPreferenceSource(foodPreferenceSource);
                userDimSummaryModelOld.setSeatPreferenceSource(seatPreferenceSource);
                userDimSummaryModelOld.setTempFoodPreferenceSource(tempFoodPreferenceSource);
                userDimSummaryModelOld.setTempSeatPreferenceSource(tempSeatPreferenceSource);
                userDimSummaryModelOld.setBeveragePreferenceSource(beveragePreferenceSource);
                userDimSummaryModelOld.setLongTermSeatPreferenceSource(longTermSeatPreferenceSource);
                userDimSummaryModelOld.setFirstClassLoungePreferenceSource(firstClassLoungePreferenceSource);
                userDimSummaryModelOld.setMergedToUseridSource(mergedToUseridSource);
                userDimSummaryModelOld.setIsFrequentFlyerNumberVerifiedSource(isFrequentFlyerNumberVerifiedSource);
                userDimSummaryModelOld.setPkId(pkId);
            } else {
                model.setPkId(pkId);
                model.setCrmCustomerId(crmCustomerId);
                model.setLyVipId(lyVipId);
                model.setLyMemberId(lyMemberId);
                model.setSysRegisterId(sysRegisterId);
                model.setFfpRegisterId(ffpRegisterId);
                model.setHistoricalFfpId(historicalFfpId);
                model.setAlipayId(alipayId);
                model.setWechatId(wechatId);
                model.setDouyinId(douyinId);
                model.setCnName(cnName);
                model.setEnName(enName);
                model.setSex(sex);
                model.setUserType(userType);
                model.setBirthday(birthday);
                model.setProvince(province);
                model.setCity(city);
                model.setNationality(nationality);
                model.setEthnicity(ethnicity);
                model.setEmployer(employer);
                model.setMobilePhone(mobilePhone);
                model.setMobileNumberReliability(mobileNumberReliability);
                model.setEmail(email);
                model.setIdCard(idCard);
                model.setPassport(passport);
                model.setSeamanId(seamanId);
                model.setAlienPermit(alienPermit);
                model.setDiplomaticStaffCertificate(diplomaticStaffCertificate);
                model.setPermanentResidentId(permanentResidentId);
                model.setCivilianStaffId(civilianStaffId);
                model.setStaffId(staffId);
                model.setOfficerIdCard(officerIdCard);
                model.setArmedPoliceOfficer(armedPoliceOfficer);
                model.setArmedPoliceSoldier(armedPoliceSoldier);
                model.setCivilianOfficialId(civilianOfficialId);
                model.setConscriptSoldierId(conscriptSoldierId);
                model.setNonCommissionedOfficerId(nonCommissionedOfficerId);
                model.setHkMacaoResidentPermit(hkMacaoResidentPermit);
                model.setTaiwanResidentTravelPermit(taiwanResidentTravelPermit);
                model.setBlindPassenger(blindPassenger);
                model.setDeafPassenger(deafPassenger);
                model.setDirectUser(isDirectUser);
                model.setOfficialWebsiteNonRegistered(isOfficialWebsiteNonRegistered);
                model.setDouyinCardPurchaser(isDouyinCardPurchaser);
                model.setDirectUserStatus(directUserStatus);
                model.setDirectRegisterDate(directRegisterDate);
                model.setDirectLastloginDate(directLastloginDate);
                model.setDirectVerifiedFlag(directVerifiedFlag);
                model.setDirectVerifyDate(directVerifyDate);
                model.setBlacklistUser(isBlacklistUser);
                model.setStudentFlag(studentFlag);
                model.setTeacherFlag(teacherFlag);
                model.setAgentFlag(agentFlag);
                model.setKeyAccount(isKeyAccount);
                model.setKeyAccountNumber(keyAccountNumber);
                model.setFrequentTraveler(isFrequentTraveler);
                model.setFrequentTravelerCardno(frequentTravelerCardno);
                model.setFrequentTravelerLevel(frequentTravelerLevel);
                model.setYjCardNumber(yjCardNumber);
                model.setYjCardExpiredate(yjCardExpiredate);
                model.setDevChannelOne(devChannelOne);
                model.setDevChannelTwo(devChannelTwo);
                model.setDevChannelThree(devChannelThree);
                model.setDevChannelFour(devChannelFour);
                model.setFtRegisterTime(ftRegisterTime);
                model.setAcceptSmsMarketing(acceptSmsMarketing);
                model.setAcceptEmailMarketing(acceptEmailMarketing);
                model.setParentFtCardno(parentFtCardno);
                model.setLyUser(isLyUser);
                model.setLyRegisterTime(lyRegisterTime);
                model.setLyCardNumber(lyCardNumber);
                model.setLyUserLevel(lyUserLevel);
                model.setLyUserStatus(lyUserStatus);
                model.setLyRegisterStatus(lyRegisterStatus);
                model.setLyVerifyStatus(lyVerifyStatus);
                model.setLyLifetimePoints(lyLifetimePoints);
                model.setLyAvailablePoints(lyAvailablePoints);
                model.setHighTraveler(isHighTraveler);
                model.setHighTravelerType("");
                model.setHighTravelerTier(highTravelerTier);
                model.setTierPrestigeExpiredate(tierPrestigeExpiredate);
                model.setTierHonorExpiredate(tierHonorExpiredate);
                model.setHighTravelerDs(highTravelerDs);
                model.setYjPersonnel(isYjPersonnel);
                model.setVipFlag(vipFlag);
                model.setCipFlag(cipFlag);
                model.setVvipFlag(vvipFlag);
                model.setFoodPreference(foodPreference);
                model.setSeatPreference(seatPreference);
                model.setTempFoodPreference(tempFoodPreference);
                model.setTempSeatPreference(tempSeatPreference);
                model.setBeveragePreference(beveragePreference);
                model.setLongTermSeatPreference(longTermSeatPreference);
                model.setFirstClassLoungePreference(firstClassLoungePreference);
                model.setMergedToUserid(mergedToUserid);
                model.setValidUser(isValidUser);
                model.setFrequentFlyerNumberVerified(isFrequentFlyerNumberVerified);
                model.setCreateTime(createTime);
                summaryModel.setCrmCustomerIdSource(crmCustomerIdSource);
                summaryModel.setLyVipIdSource(lyVipIdSource);
                summaryModel.setLyMemberIdSource(lyMemberIdSource);
                summaryModel.setSysRegisterIdSource(sysRegisterIdSource);
                summaryModel.setFfpRegisterIdSource(ffpRegisterIdSource);
                summaryModel.setHistoricalFfpIdSource(historicalFfpIdSource);
                summaryModel.setAlipayIdSource(alipayIdSource);
                summaryModel.setWechatIdSource(wechatIdSource);
                summaryModel.setDouyinIdSource(douyinIdSource);
                summaryModel.setCnNameSource(cnNameSource);
                summaryModel.setEnNameSource(enNameSource);
                summaryModel.setSexSource(sexSource);
                summaryModel.setUserTypeSource(userTypeSource);
                summaryModel.setBirthdaySource(birthdaySource);
                summaryModel.setProvinceSource(provinceSource);
                summaryModel.setCitySource(citySource);
                summaryModel.setNationalitySource(nationalitySource);
                summaryModel.setEthnicitySource(ethnicitySource);
                summaryModel.setEmployerSource(employerSource);
                summaryModel.setMobilePhoneSource(mobilePhoneSource);
                summaryModel.setMobileNumberReliabilitySource(mobileNumberReliabilitySource);
                summaryModel.setEmailSource(emailSource);
                summaryModel.setIdCardSource(idCardSource);
                summaryModel.setPassportSource(passportSource);
                summaryModel.setSeamanIdSource(seamanIdSource);
                summaryModel.setAlienPermitSource(alienPermitSource);
                summaryModel.setDiplomaticStaffCertificateSource(diplomaticStaffCertificateSource);
                summaryModel.setPermanentResidentIdSource(permanentResidentIdSource);
                summaryModel.setCivilianStaffIdSource(civilianStaffIdSource);
                summaryModel.setStaffIdSource(staffIdSource);
                summaryModel.setOfficerIdCardSource(officerIdCardSource);
                summaryModel.setArmedPoliceOfficerSource(armedPoliceOfficerSource);
                summaryModel.setArmedPoliceSoldierSource(armedPoliceSoldierSource);
                summaryModel.setCivilianOfficialIdSource(civilianOfficialIdSource);
                summaryModel.setConscriptSoldierIdSource(conscriptSoldierIdSource);
                summaryModel.setNonCommissionedOfficerIdSource(nonCommissionedOfficerIdSource);
                summaryModel.setHkMacaoResidentPermitSource(hkMacaoResidentPermitSource);
                summaryModel.setTaiwanResidentTravelPermitSource(taiwanResidentTravelPermitSource);
                summaryModel.setBlindPassengerSource(blindPassengerSource);
                summaryModel.setDeafPassengerSource(deafPassengerSource);
                summaryModel.setIsDirectUserSource(isDirectUserSource);
                summaryModel.setIsOfficialWebsiteNonRegisteredSource(isOfficialWebsiteNonRegisteredSource);
                summaryModel.setIsDouyinCardPurchaserSource(isDouyinCardPurchaserSource);
                summaryModel.setDirectUserStatusSource(directUserStatusSource);
                summaryModel.setDirectRegisterDateSource(directRegisterDateSource);
                summaryModel.setDirectLastLoginDateSource(directLastloginDateSource);
                summaryModel.setDirectVerifiedFlagSource(directVerifiedFlagSource);
                summaryModel.setDirectVerifyDateSource(directVerifyDateSource);
                summaryModel.setIsBlacklistUserSource(isBlacklistUserSource);
                summaryModel.setStudentFlagSource(studentFlagSource);
                summaryModel.setTeacherFlagSource(teacherFlagSource);
                summaryModel.setAgentFlagSource(agentFlagSource);
                summaryModel.setIsKeyAccountSource(isKeyAccountSource);
                summaryModel.setKeyAccountNumberSource(keyAccountNumberSource);
                summaryModel.setIsFrequentTravelerSource(isFrequentTravelerSource);
                summaryModel.setFrequentTravelerCardnoSource(frequentTravelerCardnoSource);
                summaryModel.setFrequentTravelerLevelSource(frequentTravelerLevelSource);
                summaryModel.setYjCardNumberSource(yjCardNumberSource);
                summaryModel.setYjCardExpiredateSource(yjCardExpiredateSource);
                summaryModel.setDevChannelOneSource(devChannelOneSource);
                summaryModel.setDevChannelTwoSource(devChannelTwoSource);
                summaryModel.setDevChannelThreeSource(devChannelThreeSource);
                summaryModel.setDevChannelFourSource(devChannelFourSource);
                summaryModel.setFtRegisterTimeSource(ftRegisterTimeSource);
                summaryModel.setAcceptSmsMarketingSource(acceptSmsMarketingSource);
                summaryModel.setAcceptEmailMarketingSource(acceptEmailMarketingSource);
                summaryModel.setParentFtCardnoSource(parentFtCardnoSource);
                summaryModel.setIsLyUserSource(isLyUserSource);
                summaryModel.setLyRegisterTimeSource(lyRegisterTimeSource);
                summaryModel.setLyCardNumberSource(lyCardNumberSource);
                summaryModel.setLyUserLevelSource(lyUserLevelSource);
                summaryModel.setLyUserStatusSource(lyUserStatusSource);
                summaryModel.setLyRegisterStatusSource(lyRegisterStatusSource);
                summaryModel.setLyVerifyStatusSource(lyVerifyStatusSource);
                summaryModel.setLyLifetimePointsSource(lyLifetimePointsSource);
                summaryModel.setLyAvailablePointsSource(lyAvailablePointsSource);
                summaryModel.setIsHighTravelerSource(isHighTravelerSource);
                summaryModel.setHighTravelerTypeSource(highTravelerTypeSource);
                summaryModel.setHighTravelerTierSource(highTravelerTierSource);
                summaryModel.setTierPrestigeExpiredateSource(tierPrestigeExpiredateSource);
                summaryModel.setTierHonorExpiredateSource(tierHonorExpiredateSource);
                summaryModel.setHighTravelerDsSource(highTravelerDsSource);
                summaryModel.setIsYjPersonnelSource(isYjPersonnelSource);
                summaryModel.setVipFlagSource(vipFlagSource);
                summaryModel.setCipFlagSource(cipFlagSource);
                summaryModel.setVvipFlagSource(vvipFlagSource);
                summaryModel.setFoodPreferenceSource(foodPreferenceSource);
                summaryModel.setSeatPreferenceSource(seatPreferenceSource);
                summaryModel.setTempFoodPreferenceSource(tempFoodPreferenceSource);
                summaryModel.setTempSeatPreferenceSource(tempSeatPreferenceSource);
                summaryModel.setBeveragePreferenceSource(beveragePreferenceSource);
                summaryModel.setLongTermSeatPreferenceSource(longTermSeatPreferenceSource);
                summaryModel.setFirstClassLoungePreferenceSource(firstClassLoungePreferenceSource);
                summaryModel.setMergedToUseridSource(mergedToUseridSource);
                summaryModel.setIsFrequentFlyerNumberVerifiedSource(isFrequentFlyerNumberVerifiedSource);
                summaryModel.setPkId(pkId);
            }
        }
        if (StringUtils.isBlank(model.getPkId())) {
            System.out.println("tid: " + tidEarly + " 在用户维表不存在");
            return;
        }
        if (StringUtils.isBlank(userDimModelOld.getPkId())) {
            System.out.println("tid: " + tidOld + " 在用户维表不存在");
            return;
        }
        //合并直销系统用户ID
        String newCrmCustomerIdSource = summaryModel.getCrmCustomerIdSource();
        String oldCrmCustomerIdSource = userDimSummaryModelOld.getCrmCustomerIdSource();
        if (StringUtils.isNotBlank(oldCrmCustomerIdSource)) {
            if (StringUtils.isBlank(newCrmCustomerIdSource)) {
                //原数据不为空，新数据为空，保留原数据
                model.setCrmCustomerId(userDimModelOld.getCrmCustomerId());
                summaryModel.setCrmCustomerIdSource(oldCrmCustomerIdSource);
                summaryModel.setCrmCustomerIdUpdatetime(currentDateTime);
            }
        }
        //合并鲁雁管家高端旅客用户ID
        String newLyVipIdSource = summaryModel.getLyVipIdSource();
        String oldLyVipIdSource = userDimSummaryModelOld.getLyVipIdSource();
        if (StringUtils.isNotBlank(oldLyVipIdSource)) {
            if (StringUtils.isBlank(newLyVipIdSource)) {
                //原数据不为空，新数据为空，保留原数据
                model.setLyVipId(userDimModelOld.getLyVipId());
                summaryModel.setLyVipIdSource(oldLyVipIdSource);
                summaryModel.setLyVipIdUpdatetime(currentDateTime);
            }
        }
        //合并鲁雁行用户ID
        String newLyMemberIdSource = summaryModel.getLyMemberIdSource();
        String oldLyMemberIdSource = userDimSummaryModelOld.getLyMemberIdSource();
        if (StringUtils.isNotBlank(oldLyMemberIdSource)) {
            if (StringUtils.isBlank(newLyMemberIdSource)) {
                //原数据不为空，新数据为空，保留原数据
                model.setLyMemberId(userDimModelOld.getLyMemberId());
                summaryModel.setLyMemberIdSource(oldLyMemberIdSource);
                summaryModel.setLyMemberIdUpdatetime(currentDateTime);
            }
        }
        //合并会员天地用户id
        String newSysRegisterIdSource = summaryModel.getSysRegisterIdSource();
        String oldSysRegisterIdSource = userDimSummaryModelOld.getSysRegisterIdSource();
        if (StringUtils.isNotBlank(oldSysRegisterIdSource)) {
            if (StringUtils.isBlank(newSysRegisterIdSource)) {
                //原数据不为空，新数据为空，保留原数据
                model.setSysRegisterId(userDimModelOld.getSysRegisterId());
                summaryModel.setSysRegisterIdSource(oldSysRegisterIdSource);
                summaryModel.setSysRegisterIdUpdatetime(currentDateTime);
            }
        }
        //合并机上注册常客用户id
        String newFfpRegisterIdSource = summaryModel.getFfpRegisterIdSource();
        String oldFfpRegisterIdSource = userDimSummaryModelOld.getFfpRegisterIdSource();
        if (StringUtils.isNotBlank(oldFfpRegisterIdSource)) {
            if (StringUtils.isBlank(newFfpRegisterIdSource)) {
                //原数据不为空，新数据为空，保留原数据
                model.setFfpRegisterId(userDimModelOld.getFfpRegisterId());
                summaryModel.setFfpRegisterIdSource(oldFfpRegisterIdSource);
                summaryModel.setFfpRegisterIdUpdatetime(currentDateTime);
            }
        }
        //合并2023年12月31日前常客用户id
        String newHistoricalFfpIdSource = summaryModel.getHistoricalFfpIdSource();
        String oldHistoricalFfpIdSource = userDimSummaryModelOld.getHistoricalFfpIdSource();
        if (StringUtils.isNotBlank(oldHistoricalFfpIdSource)) {
            if (StringUtils.isBlank(newHistoricalFfpIdSource)) {
                //原数据不为空，新数据为空，保留原数据
                model.setHistoricalFfpId(userDimModelOld.getHistoricalFfpId());
                summaryModel.setHistoricalFfpIdSource(oldHistoricalFfpIdSource);
                summaryModel.setHistoricalFfpIdUpdatetime(currentDateTime);
            }
        }
        //合并外部账号-支付宝
        String newAlipayIdSource = summaryModel.getAlipayIdSource();
        String oldAlipayIdSource = userDimSummaryModelOld.getAlipayIdSource();
        if (StringUtils.isNotBlank(oldAlipayIdSource)) {
            if (StringUtils.isBlank(newAlipayIdSource)) {
                //原数据不为空，新数据为空，保留原数据
                model.setAlipayId(userDimModelOld.getAlipayId());
                summaryModel.setAlipayIdSource(oldAlipayIdSource);
                summaryModel.setAlipayIdUpdatetime(currentDateTime);
            }
        }
        //合并外部账号-微信
        String newWechatIdSource = summaryModel.getWechatIdSource();
        String oldWechatIdSource = userDimSummaryModelOld.getWechatIdSource();
        if (StringUtils.isNotBlank(oldWechatIdSource)) {
            if (StringUtils.isBlank(newWechatIdSource)) {
                //原数据不为空，新数据为空，保留原数据
                model.setWechatId(userDimModelOld.getWechatId());
                summaryModel.setWechatIdSource(oldWechatIdSource);
                summaryModel.setWechatIdUpdatetime(currentDateTime);
            }
        }
        //合并外部账号-抖音
        String newDouyinIdSource = summaryModel.getDouyinIdSource();
        String oldDouyinIdSource = userDimSummaryModelOld.getDouyinIdSource();
        if (StringUtils.isNotBlank(oldDouyinIdSource)) {
            if (StringUtils.isBlank(newDouyinIdSource)) {
                //原数据不为空，新数据为空，保留原数据
                model.setDouyinId(userDimModelOld.getDouyinId());
                summaryModel.setDouyinIdSource(oldDouyinIdSource);
                summaryModel.setDouyinIdUpdatetime(currentDateTime);
            }
        }

        //处理中文名
        String newCnNameSource = summaryModel.getCnNameSource();
        String oldCnNameSource = userDimSummaryModelOld.getCnNameSource();
        if (StringUtils.isNotBlank(oldCnNameSource)) {
            if (StringUtils.isNotBlank(newCnNameSource)) {
                boolean remain = UserDimPriorityLevel.cnName(oldCnNameSource, newCnNameSource);
                if (!remain) {
                    //原数据优先级较高，使用原数据
                    model.setCnName(userDimModelOld.getCnName());
                    summaryModel.setCnNameSource(oldCnNameSource);
                    summaryModel.setCnNameUpdatetime(currentDateTime);
                }
            }
        }
        //处理英文名
        String newEnNameSource = summaryModel.getEnNameSource();
        String oldEnNameSource = userDimSummaryModelOld.getCnNameSource();
        if (StringUtils.isNotBlank(oldEnNameSource)) {
            if (StringUtils.isNotBlank(newEnNameSource)) {
                boolean remain = UserDimPriorityLevel.enName(oldCnNameSource, newCnNameSource);
                if (!remain) {
                    //原数据优先级较高，使用原数据
                    model.setEnName(userDimModelOld.getEnName());
                    summaryModel.setEnNameSource(oldEnNameSource);
                    summaryModel.setEnNameUpdatetime(currentDateTime);
                }
            }
        }
        //合并性别
        String newSexSource = summaryModel.getSexSource();
        String oldSexSource = userDimSummaryModelOld.getSexSource();
        if (StringUtils.isNotBlank(oldSexSource)) {
            if (StringUtils.isNotBlank(newSexSource)) {
                boolean remain = UserDimPriorityLevel.sex(oldSexSource, newSexSource);
                if (!remain) {
                    //原数据优先级较高，使用原数据
                    model.setSex(userDimModelOld.getSex());
                    summaryModel.setSexSource(oldSexSource);
                    summaryModel.setSexUpdatetime(currentDateTime);
                }
            }
        }

        //处理省份
        String newProvinceSource = summaryModel.getProvinceSource();
        String oldProvinceSource = userDimSummaryModelOld.getProvinceSource();
        if (StringUtils.isNotBlank(oldProvinceSource)) {
            if (StringUtils.isNotBlank(newProvinceSource)) {
                boolean remain = UserDimPriorityLevel.province(oldProvinceSource, newProvinceSource);
                if (!remain) {
                    //原数据优先级较高，使用原数据
                    model.setProvince(userDimModelOld.getProvince());
                    summaryModel.setProvinceSource(oldProvinceSource);
                    summaryModel.setProvinceUpdatetime(currentDateTime);
                }
            }
        }
        //处理城市
        String newCitySource = summaryModel.getCitySource();
        String oldCitySource = userDimSummaryModelOld.getCitySource();
        if (StringUtils.isNotBlank(oldCitySource)) {
            if (StringUtils.isNotBlank(newCitySource)) {
                boolean remain = UserDimPriorityLevel.city(oldCitySource, newCitySource);
                if (!remain) {
                    //原数据优先级较高，使用原数据
                    model.setCity(userDimModelOld.getCity());
                    summaryModel.setCitySource(oldCitySource);
                    summaryModel.setCityUpdatetime(currentDateTime);
                }
            }
        }
        //处理国籍
        String newNationalitySource = summaryModel.getNationalitySource();
        String oldNationalitySource = userDimSummaryModelOld.getNationalitySource();
        if (StringUtils.isNotBlank(oldNationalitySource)) {
            if (StringUtils.isNotBlank(newNationalitySource)) {
                boolean remain = UserDimPriorityLevel.nationality(oldNationalitySource, newNationalitySource);
                if (!remain) {
                    //原数据优先级较高，使用原数据
                    model.setNationality(userDimModelOld.getNationality());
                    summaryModel.setNationalitySource(oldNationalitySource);
                    summaryModel.setNationalityUpdatetime(currentDateTime);
                }
            }
        }
        //处理民族
        String newEthnicitySource = summaryModel.getEthnicitySource();
        String oldEthnicitySource = userDimSummaryModelOld.getEthnicitySource();
        if (StringUtils.isNotBlank(oldEthnicitySource)) {
            if (StringUtils.isNotBlank(newEthnicitySource)) {
                boolean remain = UserDimPriorityLevel.ethnicity(oldEthnicitySource, newEthnicitySource);
                if (!remain) {
                    //原数据优先级较高，使用原数据
                    model.setEthnicity(userDimModelOld.getEthnicity());
                    summaryModel.setEthnicitySource(oldEthnicitySource);
                    summaryModel.setEthnicityUpdatetime(currentDateTime);
                }
            }
        }

        //处理工作单位
        String newEmployerSource = summaryModel.getEmployerSource();
        String oldEmployerSource = userDimSummaryModelOld.getEmployerSource();
        if (StringUtils.isNotBlank(oldEmployerSource)) {
            if (StringUtils.isNotBlank(newEmployerSource)) {
                boolean remain = UserDimPriorityLevel.employer(oldEmployerSource, newEmployerSource);
                if (!remain) {
                    //原数据优先级较高，使用原数据
                    model.setEmployer(userDimModelOld.getEmployer());
                    summaryModel.setEmployerSource(oldEmployerSource);
                    summaryModel.setEmployerUpdatetime(currentDateTime);
                }
            }
        }
        //处理手机号
        String newMobilePhoneSource = summaryModel.getMobilePhoneSource();
        String oldMobilePhoneSource = userDimSummaryModelOld.getMobilePhoneSource();
        if (StringUtils.isNotBlank(oldMobilePhoneSource)) {
            if (StringUtils.isNotBlank(newMobilePhoneSource)) {
                boolean remain = UserDimPriorityLevel.mobilePhone(oldMobilePhoneSource, newMobilePhoneSource);
                if (!remain) {
                    //原数据优先级较高，使用原数据
                    model.setMobilePhone(userDimModelOld.getMobilePhone());
                    summaryModel.setMobilePhoneSource(oldMobilePhoneSource);
                    summaryModel.setMobilePhoneUpdatetime(currentDateTime);
                }
            }
        }
        //合并主手机号可信度
        String newMobileNumberReliabilitySource = summaryModel.getMobileNumberReliabilitySource();
        String oldMobileNumberReliabilitySource = userDimSummaryModelOld.getMobileNumberReliabilitySource();
        if (StringUtils.isNotBlank(oldMobileNumberReliabilitySource)) {
            if (StringUtils.isBlank(newMobileNumberReliabilitySource)) {
                //原数据不为空，新数据为空，保留原数据
                model.setMobileNumberReliability(userDimModelOld.getMobileNumberReliability());
                summaryModel.setMobileNumberReliabilitySource(oldMobileNumberReliabilitySource);
                summaryModel.setMobileNumberReliabilityUpdatetime(currentDateTime);
            }
        }
        //处理邮箱
        String newEmailSource = summaryModel.getEmailSource();
        String oldEmailSource = userDimSummaryModelOld.getEmailSource();
        if (StringUtils.isNotBlank(oldEmailSource)) {
            if (StringUtils.isNotBlank(newEmailSource)) {
                boolean remain = UserDimPriorityLevel.email(oldEmailSource, newEmailSource);
                if (!remain) {
                    //原数据优先级较高，使用原数据
                    model.setEmail(userDimModelOld.getEmail());
                    summaryModel.setEmailSource(oldEmailSource);
                    summaryModel.setEmailUpdatetime(currentDateTime);
                }
            }
        }
        //处理身份证
        String newIdCardSource = summaryModel.getIdCardSource();
        String oldIdCardSource = userDimSummaryModelOld.getIdCardSource();
        if (StringUtils.isNotBlank(oldIdCardSource)) {
            if (StringUtils.isNotBlank(newIdCardSource)) {
                boolean remain = UserDimPriorityLevel.credential(oldIdCardSource, newIdCardSource);
                if (!remain) {
                    //原数据优先级较高，使用原数据
                    model.setIdCard(userDimModelOld.getIdCard());
                    summaryModel.setIdCardSource(oldIdCardSource);
                    summaryModel.setIdCardUpdatetime(currentDateTime);
                }
            }
        }
        //处理护照
        String newPassportSource = summaryModel.getPassportSource();
        String oldPassportSource = userDimSummaryModelOld.getPassportSource();
        if (StringUtils.isNotBlank(oldPassportSource)) {
            if (StringUtils.isNotBlank(newPassportSource)) {
                boolean remain = UserDimPriorityLevel.credential(oldPassportSource, newPassportSource);
                if (!remain) {
                    //原数据优先级较高，使用原数据
                    model.setPassport(userDimModelOld.getPassport());
                    summaryModel.setPassportSource(oldPassportSource);
                    summaryModel.setPassportUpdatetime(currentDateTime);
                }
            }
        }
        //处理海员证
        String newSeamanIdSource = summaryModel.getSeamanIdSource();
        String oldSeamanIdSource = userDimSummaryModelOld.getSeamanIdSource();
        if (StringUtils.isNotBlank(oldSeamanIdSource)) {
            if (StringUtils.isNotBlank(newSeamanIdSource)) {
                boolean remain = UserDimPriorityLevel.credential(oldSeamanIdSource, newSeamanIdSource);
                if (!remain) {
                    //原数据优先级较高，使用原数据
                    model.setSeamanId(userDimModelOld.getSeamanId());
                    summaryModel.setSeamanIdSource(oldSeamanIdSource);
                    summaryModel.setSeamanIdUpdatetime(currentDateTime);
                }
            }
        }
        //处理外国人出入境证
        String newAlienPermitSource = summaryModel.getAlienPermitSource();
        String oldAlienPermitSource = userDimSummaryModelOld.getAlienPermitSource();
        if (StringUtils.isNotBlank(oldAlienPermitSource)) {
            if (StringUtils.isNotBlank(newAlienPermitSource)) {
                boolean remain = UserDimPriorityLevel.credential(oldAlienPermitSource, newAlienPermitSource);
                if (!remain) {
                    //原数据优先级较高，使用原数据
                    model.setAlienPermit(userDimModelOld.getAlienPermit());
                    summaryModel.setAlienPermitSource(oldAlienPermitSource);
                    summaryModel.setAlienPermitUpdatetime(currentDateTime);
                }
            }
        }
        // 处理外交部签发的驻华外交人员证 (diplomaticStaffCertificate)
        String newDiplomaticStaffCertificateSource = summaryModel.getDiplomaticStaffCertificateSource();
        String oldDiplomaticStaffCertificateSource = userDimSummaryModelOld.getDiplomaticStaffCertificateSource();
        if (StringUtils.isNotBlank(oldDiplomaticStaffCertificateSource)) {
            if (StringUtils.isNotBlank(newDiplomaticStaffCertificateSource)) {
                boolean remain = UserDimPriorityLevel.credential(oldDiplomaticStaffCertificateSource, newDiplomaticStaffCertificateSource);
                if (!remain) {
                    //原数据优先级较高，使用原数据
                    model.setDiplomaticStaffCertificate(userDimModelOld.getDiplomaticStaffCertificate());
                    summaryModel.setDiplomaticStaffCertificateSource(oldDiplomaticStaffCertificateSource);
                    summaryModel.setDiplomaticStaffCertificateUpdatetime(currentDateTime);
                }
            }
        }

        // 处理外国人永久居留证 (permanentResidentId)
        String newPermanentResidentIdSource = summaryModel.getPermanentResidentIdSource();
        String oldPermanentResidentIdSource = userDimSummaryModelOld.getPermanentResidentIdSource();
        if (StringUtils.isNotBlank(oldPermanentResidentIdSource)) {
            if (StringUtils.isNotBlank(newPermanentResidentIdSource)) {
                boolean remain = UserDimPriorityLevel.credential(oldPermanentResidentIdSource, newPermanentResidentIdSource);
                if (!remain) {
                    model.setPermanentResidentId(userDimModelOld.getPermanentResidentId());
                    summaryModel.setPermanentResidentIdSource(oldPermanentResidentIdSource);
                    summaryModel.setPermanentResidentIdUpdatetime(currentDateTime);
                }
            }
        }

        // 处理文职人员证 (civilianStaffId)
        String newCivilianStaffIdSource = summaryModel.getCivilianStaffIdSource();
        String oldCivilianStaffIdSource = userDimSummaryModelOld.getCivilianStaffIdSource();
        if (StringUtils.isNotBlank(oldCivilianStaffIdSource)) {
            if (StringUtils.isNotBlank(newCivilianStaffIdSource)) {
                boolean remain = UserDimPriorityLevel.credential(oldCivilianStaffIdSource, newCivilianStaffIdSource);
                if (!remain) {
                    model.setCivilianStaffId(userDimModelOld.getCivilianStaffId());
                    summaryModel.setCivilianStaffIdSource(oldCivilianStaffIdSource);
                    summaryModel.setCivilianStaffIdUpdatetime(currentDateTime);
                }
            }
        }

        // 处理职工证 (staffId)
        String newStaffIdSource = summaryModel.getStaffIdSource();
        String oldStaffIdSource = userDimSummaryModelOld.getStaffIdSource();
        if (StringUtils.isNotBlank(oldStaffIdSource)) {
            if (StringUtils.isNotBlank(newStaffIdSource)) {
                boolean remain = UserDimPriorityLevel.credential(oldStaffIdSource, newStaffIdSource);
                if (!remain) {
                    model.setStaffId(userDimModelOld.getStaffId());
                    summaryModel.setStaffIdSource(oldStaffIdSource);
                    summaryModel.setStaffIdUpdatetime(currentDateTime);
                }
            }
        }

        // 处理军官证 (officerIdCard)
        String newOfficerIdCardSource = summaryModel.getOfficerIdCardSource();
        String oldOfficerIdCardSource = userDimSummaryModelOld.getOfficerIdCardSource();
        if (StringUtils.isNotBlank(oldOfficerIdCardSource)) {
            if (StringUtils.isNotBlank(newOfficerIdCardSource)) {
                boolean remain = UserDimPriorityLevel.credential(oldOfficerIdCardSource, newOfficerIdCardSource);
                if (!remain) {
                    model.setOfficerIdCard(userDimModelOld.getOfficerIdCard());
                    summaryModel.setOfficerIdCardSource(oldOfficerIdCardSource);
                    summaryModel.setOfficerIdCardUpdatetime(currentDateTime);
                }
            }
        }

        // 处理武警警官证 (armedPoliceOfficer)
        String newArmedPoliceOfficerSource = summaryModel.getArmedPoliceOfficerSource();
        String oldArmedPoliceOfficerSource = userDimSummaryModelOld.getArmedPoliceOfficerSource();
        if (StringUtils.isNotBlank(oldArmedPoliceOfficerSource)) {
            if (StringUtils.isNotBlank(newArmedPoliceOfficerSource)) {
                boolean remain = UserDimPriorityLevel.credential(oldArmedPoliceOfficerSource, newArmedPoliceOfficerSource);
                if (!remain) {
                    model.setArmedPoliceOfficer(userDimModelOld.getArmedPoliceOfficer());
                    summaryModel.setArmedPoliceOfficerSource(oldArmedPoliceOfficerSource);
                    summaryModel.setArmedPoliceOfficerUpdatetime(currentDateTime);
                }
            }
        }

        // 处理武警士兵证 (armedPoliceSoldier)
        String newArmedPoliceSoldierSource = summaryModel.getArmedPoliceSoldierSource();
        String oldArmedPoliceSoldierSource = userDimSummaryModelOld.getArmedPoliceSoldierSource();
        if (StringUtils.isNotBlank(oldArmedPoliceSoldierSource)) {
            if (StringUtils.isNotBlank(newArmedPoliceSoldierSource)) {
                boolean remain = UserDimPriorityLevel.credential(oldArmedPoliceSoldierSource, newArmedPoliceSoldierSource);
                if (!remain) {
                    model.setArmedPoliceSoldier(userDimModelOld.getArmedPoliceSoldier());
                    summaryModel.setArmedPoliceSoldierSource(oldArmedPoliceSoldierSource);
                    summaryModel.setArmedPoliceSoldierUpdatetime(currentDateTime);
                }
            }
        }

        // 处理文职干部证 (civilianOfficialId)
        String newCivilianOfficialIdSource = summaryModel.getCivilianOfficialIdSource();
        String oldCivilianOfficialIdSource = userDimSummaryModelOld.getCivilianOfficialIdSource();
        if (StringUtils.isNotBlank(oldCivilianOfficialIdSource)) {
            if (StringUtils.isNotBlank(newCivilianOfficialIdSource)) {
                boolean remain = UserDimPriorityLevel.credential(oldCivilianOfficialIdSource, newCivilianOfficialIdSource);
                if (!remain) {
                    model.setCivilianOfficialId(userDimModelOld.getCivilianOfficialId());
                    summaryModel.setCivilianOfficialIdSource(oldCivilianOfficialIdSource);
                    summaryModel.setCivilianOfficialIdUpdatetime(currentDateTime);
                }
            }
        }

        // 处理义务兵证 (conscriptSoldierId)
        String newConscriptSoldierIdSource = summaryModel.getConscriptSoldierIdSource();
        String oldConscriptSoldierIdSource = userDimSummaryModelOld.getConscriptSoldierIdSource();
        if (StringUtils.isNotBlank(oldConscriptSoldierIdSource)) {
            if (StringUtils.isNotBlank(newConscriptSoldierIdSource)) {
                boolean remain = UserDimPriorityLevel.credential(oldConscriptSoldierIdSource, newConscriptSoldierIdSource);
                if (!remain) {
                    model.setConscriptSoldierId(userDimModelOld.getConscriptSoldierId());
                    summaryModel.setConscriptSoldierIdSource(oldConscriptSoldierIdSource);
                    summaryModel.setConscriptSoldierIdUpdatetime(currentDateTime);
                }
            }
        }

        // 处理士官证 (nonCommissionedOfficerId)
        String newNonCommissionedOfficerIdSource = summaryModel.getNonCommissionedOfficerIdSource();
        String oldNonCommissionedOfficerIdSource = userDimSummaryModelOld.getNonCommissionedOfficerIdSource();
        if (StringUtils.isNotBlank(oldNonCommissionedOfficerIdSource)) {
            if (StringUtils.isNotBlank(newNonCommissionedOfficerIdSource)) {
                boolean remain = UserDimPriorityLevel.credential(oldNonCommissionedOfficerIdSource, newNonCommissionedOfficerIdSource);
                if (!remain) {
                    model.setNonCommissionedOfficerId(userDimModelOld.getNonCommissionedOfficerId());
                    summaryModel.setNonCommissionedOfficerIdSource(oldNonCommissionedOfficerIdSource);
                    summaryModel.setNonCommissionedOfficerIdUpdatetime(currentDateTime);
                }
            }
        }

        // 处理港澳居民来往内地通行证 (hkMacaoResidentPermit)
        String newHkMacaoResidentPermitSource = summaryModel.getHkMacaoResidentPermitSource();
        String oldHkMacaoResidentPermitSource = userDimSummaryModelOld.getHkMacaoResidentPermitSource();
        if (StringUtils.isNotBlank(oldHkMacaoResidentPermitSource)) {
            if (StringUtils.isNotBlank(newHkMacaoResidentPermitSource)) {
                boolean remain = UserDimPriorityLevel.credential(oldHkMacaoResidentPermitSource, newHkMacaoResidentPermitSource);
                if (!remain) {
                    model.setHkMacaoResidentPermit(userDimModelOld.getHkMacaoResidentPermit());
                    summaryModel.setHkMacaoResidentPermitSource(oldHkMacaoResidentPermitSource);
                    summaryModel.setHkMacaoResidentPermitUpdatetime(currentDateTime);
                }
            }
        }

        // 处理台湾居民来往大陆通行证 (taiwanResidentTravelPermit)
        String newTaiwanResidentTravelPermitSource = summaryModel.getTaiwanResidentTravelPermitSource();
        String oldTaiwanResidentTravelPermitSource = userDimSummaryModelOld.getTaiwanResidentTravelPermitSource();
        if (StringUtils.isNotBlank(oldTaiwanResidentTravelPermitSource)) {
            if (StringUtils.isNotBlank(newTaiwanResidentTravelPermitSource)) {
                boolean remain = UserDimPriorityLevel.credential(oldTaiwanResidentTravelPermitSource, newTaiwanResidentTravelPermitSource);
                if (!remain) {
                    model.setTaiwanResidentTravelPermit(userDimModelOld.getTaiwanResidentTravelPermit());
                    summaryModel.setTaiwanResidentTravelPermitSource(oldTaiwanResidentTravelPermitSource);
                    summaryModel.setTaiwanResidentTravelPermitUpdatetime(currentDateTime);
                }
            }
        }
        // 合并盲人旅客标识 (blindPassenger)
        String newBlindPassengerSource = summaryModel.getBlindPassengerSource();
        String oldBlindPassengerSource = userDimSummaryModelOld.getBlindPassengerSource();
        if (StringUtils.isNotBlank(oldBlindPassengerSource)) {
            if (StringUtils.isBlank(newBlindPassengerSource)) {
                model.setBlindPassenger(userDimModelOld.getBlindPassenger());
                summaryModel.setBlindPassengerSource(oldBlindPassengerSource);
                summaryModel.setBlindPassengerUpdatetime(currentDateTime);
            }
        }

        // 合并失聪旅客标识 (deafPassenger)
        String newDeafPassengerSource = summaryModel.getDeafPassengerSource();
        String oldDeafPassengerSource = userDimSummaryModelOld.getDeafPassengerSource();
        if (StringUtils.isNotBlank(oldDeafPassengerSource)) {
            if (StringUtils.isBlank(newDeafPassengerSource)) {
                model.setDeafPassenger(userDimModelOld.getDeafPassenger());
                summaryModel.setDeafPassengerSource(oldDeafPassengerSource);
                summaryModel.setDeafPassengerUpdatetime(currentDateTime);
            }
        }

        // 合并是否直销用户 (isDirectUser)
        String newIsDirectUserSource = summaryModel.getIsDirectUserSource();
        String oldIsDirectUserSource = userDimSummaryModelOld.getIsDirectUserSource();
        if (StringUtils.isNotBlank(oldIsDirectUserSource)) {
            if (StringUtils.isBlank(newIsDirectUserSource)) {
                model.setDirectUser(userDimModelOld.getDirectUser());
                summaryModel.setIsDirectUserSource(oldIsDirectUserSource);
                summaryModel.setIsDirectUserUpdatetime(currentDateTime);
            }
        }

        // 合并是否官网非注册用户 (isOfficialWebsiteNonRegistered)
        String newIsOfficialWebsiteNonRegisteredSource = summaryModel.getIsOfficialWebsiteNonRegisteredSource();
        String oldIsOfficialWebsiteNonRegisteredSource = userDimSummaryModelOld.getIsOfficialWebsiteNonRegisteredSource();
        if (StringUtils.isNotBlank(oldIsOfficialWebsiteNonRegisteredSource)) {
            if (StringUtils.isBlank(newIsOfficialWebsiteNonRegisteredSource)) {
                model.setOfficialWebsiteNonRegistered(userDimModelOld.getOfficialWebsiteNonRegistered());
                summaryModel.setIsOfficialWebsiteNonRegisteredSource(oldIsOfficialWebsiteNonRegisteredSource);
                summaryModel.setIsOfficialWebsiteNonRegisteredUpdatetime(currentDateTime);
            }
        }

        // 合并是否抖音次卡购买人 (isDouyinCardPurchaser)
        String newIsDouyinCardPurchaserSource = summaryModel.getIsDouyinCardPurchaserSource();
        String oldIsDouyinCardPurchaserSource = userDimSummaryModelOld.getIsDouyinCardPurchaserSource();
        if (StringUtils.isNotBlank(oldIsDouyinCardPurchaserSource)) {
            if (StringUtils.isBlank(newIsDouyinCardPurchaserSource)) {
                model.setDouyinCardPurchaser(userDimModelOld.getDouyinCardPurchaser());
                summaryModel.setIsDouyinCardPurchaserSource(oldIsDouyinCardPurchaserSource);
                summaryModel.setIsDouyinCardPurchaserUpdatetime(currentDateTime);
            }
        }

        // 合并直销用户状态 (directUserStatus)
        String newDirectUserStatusSource = summaryModel.getDirectUserStatusSource();
        String oldDirectUserStatusSource = userDimSummaryModelOld.getDirectUserStatusSource();
        if (StringUtils.isNotBlank(oldDirectUserStatusSource)) {
            if (StringUtils.isBlank(newDirectUserStatusSource)) {
                model.setDirectUserStatus(userDimModelOld.getDirectUserStatus());
                summaryModel.setDirectUserStatusSource(oldDirectUserStatusSource);
                summaryModel.setDirectUserStatusUpdatetime(currentDateTime);
            }
        }

        // 合并直销用户注册日期 (directRegisterDate)
        String newDirectRegisterDateSource = summaryModel.getDirectRegisterDateSource();
        String oldDirectRegisterDateSource = userDimSummaryModelOld.getDirectRegisterDateSource();
        if (StringUtils.isNotBlank(oldDirectRegisterDateSource)) {
            if (StringUtils.isBlank(newDirectRegisterDateSource)) {
                model.setDirectRegisterDate(userDimModelOld.getDirectRegisterDate());
                summaryModel.setDirectRegisterDateSource(oldDirectRegisterDateSource);
                summaryModel.setDirectRegisterDateUpdatetime(currentDateTime);
            }
        }

        // 合并直销最后一次登录日期 (directLastLoginDate)
        String newDirectLastLoginDateSource = summaryModel.getDirectLastLoginDateSource();
        String oldDirectLastLoginDateSource = userDimSummaryModelOld.getDirectLastLoginDateSource();
        if (StringUtils.isNotBlank(oldDirectLastLoginDateSource)) {
            if (StringUtils.isBlank(newDirectLastLoginDateSource)) {
                model.setDirectLastloginDate(userDimModelOld.getDirectLastloginDate());
                summaryModel.setDirectLastLoginDateSource(oldDirectLastLoginDateSource);
                summaryModel.setDirectLastLoginDateUpdatetime(currentDateTime);
            }
        }

        // 合并直销实名认证标识 (directVerifiedFlag)
        String newDirectVerifiedFlagSource = summaryModel.getDirectVerifiedFlagSource();
        String oldDirectVerifiedFlagSource = userDimSummaryModelOld.getDirectVerifiedFlagSource();
        if (StringUtils.isNotBlank(oldDirectVerifiedFlagSource)) {
            if (StringUtils.isBlank(newDirectVerifiedFlagSource)) {
                model.setDirectVerifiedFlag(userDimModelOld.getDirectVerifiedFlag());
                summaryModel.setDirectVerifiedFlagSource(oldDirectVerifiedFlagSource);
                summaryModel.setDirectVerifiedFlagUpdatetime(currentDateTime);
            }
        }

        // 合并直销实名认证日期 (directVerifyDate)
        String newDirectVerifyDateSource = summaryModel.getDirectVerifyDateSource();
        String oldDirectVerifyDateSource = userDimSummaryModelOld.getDirectVerifyDateSource();
        if (StringUtils.isNotBlank(oldDirectVerifyDateSource)) {
            if (StringUtils.isBlank(newDirectVerifyDateSource)) {
                model.setDirectVerifyDate(userDimModelOld.getDirectVerifyDate());
                summaryModel.setDirectVerifyDateSource(oldDirectVerifyDateSource);
                summaryModel.setDirectVerifyDateUpdatetime(currentDateTime);
            }
        }

        // 合并是否直销黑名单用户 (isBlacklistUser)
        String newIsBlacklistUserSource = summaryModel.getIsBlacklistUserSource();
        String oldIsBlacklistUserSource = userDimSummaryModelOld.getIsBlacklistUserSource();
        if (StringUtils.isNotBlank(oldIsBlacklistUserSource)) {
            if (StringUtils.isBlank(newIsBlacklistUserSource)) {
                model.setBlacklistUser(userDimModelOld.getBlacklistUser());
                summaryModel.setIsBlacklistUserSource(oldIsBlacklistUserSource);
                summaryModel.setIsBlacklistUserUpdatetime(currentDateTime);
            }
        }

        // 合并学生标识 (studentFlag)
        String newStudentFlagSource = summaryModel.getStudentFlagSource();
        String oldStudentFlagSource = userDimSummaryModelOld.getStudentFlagSource();
        if (StringUtils.isNotBlank(oldStudentFlagSource)) {
            if (StringUtils.isBlank(newStudentFlagSource)) {
                model.setStudentFlag(userDimModelOld.getStudentFlag());
                summaryModel.setStudentFlagSource(oldStudentFlagSource);
                summaryModel.setStudentFlagUpdatetime(currentDateTime);
            }
        }

        // 合并教师标识 (teacherFlag)
        String newTeacherFlagSource = summaryModel.getTeacherFlagSource();
        String oldTeacherFlagSource = userDimSummaryModelOld.getTeacherFlagSource();
        if (StringUtils.isNotBlank(oldTeacherFlagSource)) {
            if (StringUtils.isBlank(newTeacherFlagSource)) {
                model.setTeacherFlag(userDimModelOld.getTeacherFlag());
                summaryModel.setTeacherFlagSource(oldTeacherFlagSource);
                summaryModel.setTeacherFlagUpdatetime(currentDateTime);
            }
        }

        // 合并大客户标识 (isKeyAccount)
        String newIsKeyAccountSource = summaryModel.getIsKeyAccountSource();
        String oldIsKeyAccountSource = userDimSummaryModelOld.getIsKeyAccountSource();
        if (StringUtils.isNotBlank(oldIsKeyAccountSource)) {
            if (StringUtils.isBlank(newIsKeyAccountSource)) {
                model.setKeyAccount(userDimModelOld.getKeyAccount());
                summaryModel.setIsKeyAccountSource(oldIsKeyAccountSource);
                summaryModel.setIsKeyAccountUpdatetime(currentDateTime);
            }
        }

        // 合并大客户号 (keyAccountNumber)
        String newKeyAccountNumberSource = summaryModel.getKeyAccountNumberSource();
        String oldKeyAccountNumberSource = userDimSummaryModelOld.getKeyAccountNumberSource();
        if (StringUtils.isNotBlank(oldKeyAccountNumberSource)) {
            if (StringUtils.isBlank(newKeyAccountNumberSource)) {
                model.setKeyAccountNumber(userDimModelOld.getKeyAccountNumber());
                summaryModel.setKeyAccountNumberSource(oldKeyAccountNumberSource);
                summaryModel.setKeyAccountNumberUpdatetime(currentDateTime);
            }
        }

        // 合并是否常旅客 (isFrequentTraveler)
        String newIsFrequentTravelerSource = summaryModel.getIsFrequentTravelerSource();
        String oldIsFrequentTravelerSource = userDimSummaryModelOld.getIsFrequentTravelerSource();
        if (StringUtils.isNotBlank(oldIsFrequentTravelerSource)) {
            if (StringUtils.isBlank(newIsFrequentTravelerSource)) {
                model.setFrequentTraveler(userDimModelOld.getFrequentTraveler());
                summaryModel.setIsFrequentTravelerSource(oldIsFrequentTravelerSource);
                summaryModel.setIsFrequentTravelerUpdatetime(currentDateTime);
            }
        }

        //处理常客卡号
        String newFrequentTravelerCardnoSource = summaryModel.getFrequentTravelerCardnoSource();
        String oldFrequentTravelerCardnoSource = userDimSummaryModelOld.getFrequentTravelerCardnoSource();
        if (StringUtils.isNotBlank(oldFrequentTravelerCardnoSource)) {
            if (StringUtils.isNotBlank(newFrequentTravelerCardnoSource)) {
                boolean remain = UserDimPriorityLevel.frequentTravelerCardno(oldFrequentTravelerCardnoSource, newFrequentTravelerCardnoSource);
                if (!remain) {
                    //原数据优先级较高，使用原数据
                    model.setFrequentTravelerCardno(userDimModelOld.getFrequentTravelerCardno());
                    summaryModel.setFrequentTravelerCardnoSource(oldFrequentTravelerCardnoSource);
                    summaryModel.setFrequentTravelerCardnoUpdatetime(currentDateTime);
                }
            }
        }

        // 合并常旅客等级 (frequentTravelerLevel)
        String newFrequentTravelerLevelSource = summaryModel.getFrequentTravelerLevelSource();
        String oldFrequentTravelerLevelSource = userDimSummaryModelOld.getFrequentTravelerLevelSource();
        if (StringUtils.isNotBlank(oldFrequentTravelerLevelSource)) {
            if (StringUtils.isBlank(newFrequentTravelerLevelSource)) {
                model.setFrequentTravelerLevel(userDimModelOld.getFrequentTravelerLevel());
                summaryModel.setFrequentTravelerLevelSource(oldFrequentTravelerLevelSource);
                summaryModel.setFrequentTravelerLevelUpdatetime(currentDateTime);
            }
        }

        // 合并援疆卡卡号 (yjCardNumber)
        String newYjCardNumberSource = summaryModel.getYjCardNumberSource();
        String oldYjCardNumberSource = userDimSummaryModelOld.getYjCardNumberSource();
        if (StringUtils.isNotBlank(oldYjCardNumberSource)) {
            if (StringUtils.isBlank(newYjCardNumberSource)) {
                model.setYjCardNumber(userDimModelOld.getYjCardNumber());
                summaryModel.setYjCardNumberSource(oldYjCardNumberSource);
                summaryModel.setYjCardNumberUpdatetime(currentDateTime);
            }
        }

        // 合并援疆卡有效截止期 (yjCardExpiredate)
        String newYjCardExpiredateSource = summaryModel.getYjCardExpiredateSource();
        String oldYjCardExpiredateSource = userDimSummaryModelOld.getYjCardExpiredateSource();
        if (StringUtils.isNotBlank(oldYjCardExpiredateSource)) {
            if (StringUtils.isBlank(newYjCardExpiredateSource)) {
                model.setYjCardExpiredate(userDimModelOld.getYjCardExpiredate());
                summaryModel.setYjCardExpiredateSource(oldYjCardExpiredateSource);
                summaryModel.setYjCardExpiredateUpdatetime(currentDateTime);
            }
        }

        // 合并常旅客发展渠道（一级） (devChannelOne)
        String newDevChannelOneSource = summaryModel.getDevChannelOneSource();
        String oldDevChannelOneSource = userDimSummaryModelOld.getDevChannelOneSource();
        if (StringUtils.isNotBlank(oldDevChannelOneSource)) {
            if (StringUtils.isBlank(newDevChannelOneSource)) {
                model.setDevChannelOne(userDimModelOld.getDevChannelOne());
                summaryModel.setDevChannelOneSource(oldDevChannelOneSource);
                summaryModel.setDevChannelOneUpdatetime(currentDateTime);
            }
        }

        // 合并常旅客发展渠道（二级） (devChannelTwo)
        String newDevChannelTwoSource = summaryModel.getDevChannelTwoSource();
        String oldDevChannelTwoSource = userDimSummaryModelOld.getDevChannelTwoSource();
        if (StringUtils.isNotBlank(oldDevChannelTwoSource)) {
            if (StringUtils.isBlank(newDevChannelTwoSource)) {
                model.setDevChannelTwo(userDimModelOld.getDevChannelTwo());
                summaryModel.setDevChannelTwoSource(oldDevChannelTwoSource);
                summaryModel.setDevChannelTwoUpdatetime(currentDateTime);
            }
        }

        // 合并常旅客发展渠道（三级） (devChannelThree)
        String newDevChannelThreeSource = summaryModel.getDevChannelThreeSource();
        String oldDevChannelThreeSource = userDimSummaryModelOld.getDevChannelThreeSource();
        if (StringUtils.isNotBlank(oldDevChannelThreeSource)) {
            if (StringUtils.isBlank(newDevChannelThreeSource)) {
                model.setDevChannelThree(userDimModelOld.getDevChannelThree());
                summaryModel.setDevChannelThreeSource(oldDevChannelThreeSource);
                summaryModel.setDevChannelThreeUpdatetime(currentDateTime);
            }
        }

        // 合并常旅客发展渠道（四级） (devChannelFour)
        String newDevChannelFourSource = summaryModel.getDevChannelFourSource();
        String oldDevChannelFourSource = userDimSummaryModelOld.getDevChannelFourSource();
        if (StringUtils.isNotBlank(oldDevChannelFourSource)) {
            if (StringUtils.isBlank(newDevChannelFourSource)) {
                model.setDevChannelFour(userDimModelOld.getDevChannelFour());
                summaryModel.setDevChannelFourSource(oldDevChannelFourSource);
                summaryModel.setDevChannelFourUpdatetime(currentDateTime);
            }
        }

        // 合并注册常旅客时间 (ftRegisterTime)
        String newFtRegisterTimeSource = summaryModel.getFtRegisterTimeSource();
        String oldFtRegisterTimeSource = userDimSummaryModelOld.getFtRegisterTimeSource();
        if (StringUtils.isNotBlank(oldFtRegisterTimeSource)) {
            if (StringUtils.isBlank(newFtRegisterTimeSource)) {
                model.setFtRegisterTime(userDimModelOld.getFtRegisterTime());
                summaryModel.setFtRegisterTimeSource(oldFtRegisterTimeSource);
                summaryModel.setFtRegisterTimeUpdatetime(currentDateTime);
            }
        }

        // 合并是否接受短信营销 (acceptSmsMarketing)
        String newAcceptSmsMarketingSource = summaryModel.getAcceptSmsMarketingSource();
        String oldAcceptSmsMarketingSource = userDimSummaryModelOld.getAcceptSmsMarketingSource();
        if (StringUtils.isNotBlank(oldAcceptSmsMarketingSource)) {
            if (StringUtils.isBlank(newAcceptSmsMarketingSource)) {
                model.setAcceptSmsMarketing(userDimModelOld.getAcceptSmsMarketing());
                summaryModel.setAcceptSmsMarketingSource(oldAcceptSmsMarketingSource);
                summaryModel.setAcceptSmsMarketingUpdatetime(currentDateTime);
            }
        }

        // 合并是否接受邮件营销 (acceptEmailMarketing)
        String newAcceptEmailMarketingSource = summaryModel.getAcceptEmailMarketingSource();
        String oldAcceptEmailMarketingSource = userDimSummaryModelOld.getAcceptEmailMarketingSource();
        if (StringUtils.isNotBlank(oldAcceptEmailMarketingSource)) {
            if (StringUtils.isBlank(newAcceptEmailMarketingSource)) {
                model.setAcceptEmailMarketing(userDimModelOld.getAcceptEmailMarketing());
                summaryModel.setAcceptEmailMarketingSource(oldAcceptEmailMarketingSource);
                summaryModel.setAcceptEmailMarketingUpdatetime(currentDateTime);
            }
        }

        // 合并父母常客卡号 (parentFtCardno)
        String newParentFtCardnoSource = summaryModel.getParentFtCardnoSource();
        String oldParentFtCardnoSource = userDimSummaryModelOld.getParentFtCardnoSource();
        if (StringUtils.isNotBlank(oldParentFtCardnoSource)) {
            if (StringUtils.isBlank(newParentFtCardnoSource)) {
                model.setParentFtCardno(userDimModelOld.getParentFtCardno());
                summaryModel.setParentFtCardnoSource(oldParentFtCardnoSource);
                summaryModel.setParentFtCardnoUpdatetime(currentDateTime);
            }
        }

        // 合并是否鲁雁行用户 (isLyUser)
        String newIsLyUserSource = summaryModel.getIsLyUserSource();
        String oldIsLyUserSource = userDimSummaryModelOld.getIsLyUserSource();
        if (StringUtils.isNotBlank(oldIsLyUserSource)) {
            if (StringUtils.isBlank(newIsLyUserSource)) {
                model.setLyUser(userDimModelOld.getLyUser());
                summaryModel.setIsLyUserSource(oldIsLyUserSource);
                summaryModel.setIsLyUserUpdatetime(currentDateTime);
            }
        }

        // 合并鲁雁行注册时间 (lyRegisterTime)
        String newLyRegisterTimeSource = summaryModel.getLyRegisterTimeSource();
        String oldLyRegisterTimeSource = userDimSummaryModelOld.getLyRegisterTimeSource();
        if (StringUtils.isNotBlank(oldLyRegisterTimeSource)) {
            if (StringUtils.isBlank(newLyRegisterTimeSource)) {
                model.setLyRegisterTime(userDimModelOld.getLyRegisterTime());
                summaryModel.setLyRegisterTimeSource(oldLyRegisterTimeSource);
                summaryModel.setLyRegisterTimeUpdatetime(currentDateTime);
            }
        }

        // 合并鲁雁行卡号 (lyCardNumber)
        String newLyCardNumberSource = summaryModel.getLyCardNumberSource();
        String oldLyCardNumberSource = userDimSummaryModelOld.getLyCardNumberSource();
        if (StringUtils.isNotBlank(oldLyCardNumberSource)) {
            if (StringUtils.isBlank(newLyCardNumberSource)) {
                model.setLyCardNumber(userDimModelOld.getLyCardNumber());
                summaryModel.setLyCardNumberSource(oldLyCardNumberSource);
                summaryModel.setLyCardNumberUpdatetime(currentDateTime);
            }
        }

        // 合并鲁雁行用户等级 (lyUserLevel)
        String newLyUserLevelSource = summaryModel.getLyUserLevelSource();
        String oldLyUserLevelSource = userDimSummaryModelOld.getLyUserLevelSource();
        if (StringUtils.isNotBlank(oldLyUserLevelSource)) {
            if (StringUtils.isBlank(newLyUserLevelSource)) {
                model.setLyUserLevel(userDimModelOld.getLyUserLevel());
                summaryModel.setLyUserLevelSource(oldLyUserLevelSource);
                summaryModel.setLyUserLevelUpdatetime(currentDateTime);
            }
        }

        // 合并鲁雁行用户状态 (lyUserStatus)
        String newLyUserStatusSource = summaryModel.getLyUserStatusSource();
        String oldLyUserStatusSource = userDimSummaryModelOld.getLyUserStatusSource();
        if (StringUtils.isNotBlank(oldLyUserStatusSource)) {
            if (StringUtils.isBlank(newLyUserStatusSource)) {
                model.setLyUserStatus(userDimModelOld.getLyUserStatus());
                summaryModel.setLyUserStatusSource(oldLyUserStatusSource);
                summaryModel.setLyUserStatusUpdatetime(currentDateTime);
            }
        }

        // 合并鲁雁行注册状态 (lyRegisterStatus)
        String newLyRegisterStatusSource = summaryModel.getLyRegisterStatusSource();
        String oldLyRegisterStatusSource = userDimSummaryModelOld.getLyRegisterStatusSource();
        if (StringUtils.isNotBlank(oldLyRegisterStatusSource)) {
            if (StringUtils.isBlank(newLyRegisterStatusSource)) {
                model.setLyRegisterStatus(userDimModelOld.getLyRegisterStatus());
                summaryModel.setLyRegisterStatusSource(oldLyRegisterStatusSource);
                summaryModel.setLyRegisterStatusUpdatetime(currentDateTime);
            }
        }

        // 合并鲁雁行实名认证状态 (lyVerifyStatus)
        String newLyVerifyStatusSource = summaryModel.getLyVerifyStatusSource();
        String oldLyVerifyStatusSource = userDimSummaryModelOld.getLyVerifyStatusSource();
        if (StringUtils.isNotBlank(oldLyVerifyStatusSource)) {
            if (StringUtils.isBlank(newLyVerifyStatusSource)) {
                model.setLyVerifyStatus(userDimModelOld.getLyVerifyStatus());
                summaryModel.setLyVerifyStatusSource(oldLyVerifyStatusSource);
                summaryModel.setLyVerifyStatusUpdatetime(currentDateTime);
            }
        }

        // 合并生命周期鲁雁值 (lyLifetimePoints)
        String newLyLifetimePointsSource = summaryModel.getLyLifetimePointsSource();
        String oldLyLifetimePointsSource = userDimSummaryModelOld.getLyLifetimePointsSource();
        if (StringUtils.isNotBlank(oldLyLifetimePointsSource)) {
            if (StringUtils.isBlank(newLyLifetimePointsSource)) {
                model.setLyLifetimePoints(userDimModelOld.getLyLifetimePoints());
                summaryModel.setLyLifetimePointsSource(oldLyLifetimePointsSource);
                summaryModel.setLyLifetimePointsUpdatetime(currentDateTime);
            }
        }

        // 合并可用鲁雁值 (lyAvailablePoints)
        String newLyAvailablePointsSource = summaryModel.getLyAvailablePointsSource();
        String oldLyAvailablePointsSource = userDimSummaryModelOld.getLyAvailablePointsSource();
        if (StringUtils.isNotBlank(oldLyAvailablePointsSource)) {
            if (StringUtils.isBlank(newLyAvailablePointsSource)) {
                model.setLyAvailablePoints(userDimModelOld.getLyAvailablePoints());
                summaryModel.setLyAvailablePointsSource(oldLyAvailablePointsSource);
                summaryModel.setLyAvailablePointsUpdatetime(currentDateTime);
            }
        }

        // 合并是否高端旅客 (isHighTraveler)
        String newIsHighTravelerSource = summaryModel.getIsHighTravelerSource();
        String oldIsHighTravelerSource = userDimSummaryModelOld.getIsHighTravelerSource();
        if (StringUtils.isNotBlank(oldIsHighTravelerSource)) {
            if (StringUtils.isBlank(newIsHighTravelerSource)) {
                model.setHighTraveler(userDimModelOld.getHighTraveler());
                summaryModel.setIsHighTravelerSource(oldIsHighTravelerSource);
                summaryModel.setIsHighTravelerUpdatetime(currentDateTime);
            }
        }

        // 合并高端旅客类型 (highTravelerType)
        String newHighTravelerTypeSource = summaryModel.getHighTravelerTypeSource();
        String oldHighTravelerTypeSource = userDimSummaryModelOld.getHighTravelerTypeSource();
        if (StringUtils.isNotBlank(oldHighTravelerTypeSource)) {
            if (StringUtils.isBlank(newHighTravelerTypeSource)) {
                model.setHighTravelerType(userDimModelOld.getHighTravelerType());
                summaryModel.setHighTravelerTypeSource(oldHighTravelerTypeSource);
                summaryModel.setHighTravelerTypeUpdatetime(currentDateTime);
            }
        }

        // 合并高端旅客分层类型 (highTravelerTier)
        String newHighTravelerTierSource = summaryModel.getHighTravelerTierSource();
        String oldHighTravelerTierSource = userDimSummaryModelOld.getHighTravelerTierSource();
        if (StringUtils.isNotBlank(oldHighTravelerTierSource)) {
            if (StringUtils.isBlank(newHighTravelerTierSource)) {
                model.setHighTravelerTier(userDimModelOld.getHighTravelerTier());
                summaryModel.setHighTravelerTierSource(oldHighTravelerTierSource);
                summaryModel.setHighTravelerTierUpdatetime(currentDateTime);
            }
        }

        // 合并至尊身份分层有效期 (tierPrestigeExpiredate)
        String newTierPrestigeExpiredateSource = summaryModel.getTierPrestigeExpiredateSource();
        String oldTierPrestigeExpiredateSource = userDimSummaryModelOld.getTierPrestigeExpiredateSource();
        if (StringUtils.isNotBlank(oldTierPrestigeExpiredateSource)) {
            if (StringUtils.isBlank(newTierPrestigeExpiredateSource)) {
                model.setTierPrestigeExpiredate(userDimModelOld.getTierPrestigeExpiredate());
                summaryModel.setTierPrestigeExpiredateSource(oldTierPrestigeExpiredateSource);
                summaryModel.setTierPrestigeExpiredateUpdatetime(currentDateTime);
            }
        }

        // 合并荣耀身份分层有效期 (tierHonorExpiredate)
        String newTierHonorExpiredateSource = summaryModel.getTierHonorExpiredateSource();
        String oldTierHonorExpiredateSource = userDimSummaryModelOld.getTierHonorExpiredateSource();
        if (StringUtils.isNotBlank(oldTierHonorExpiredateSource)) {
            if (StringUtils.isBlank(newTierHonorExpiredateSource)) {
                model.setTierHonorExpiredate(userDimModelOld.getTierHonorExpiredate());
                summaryModel.setTierHonorExpiredateSource(oldTierHonorExpiredateSource);
                summaryModel.setTierHonorExpiredateUpdatetime(currentDateTime);
            }
        }

        // 合并高端旅客数据来源 (highTravelerDs)
        String newHighTravelerDsSource = summaryModel.getHighTravelerDsSource();
        String oldHighTravelerDsSource = userDimSummaryModelOld.getHighTravelerDsSource();
        if (StringUtils.isNotBlank(oldHighTravelerDsSource)) {
            if (StringUtils.isBlank(newHighTravelerDsSource)) {
                model.setHighTravelerDs(userDimModelOld.getHighTravelerDs());
                summaryModel.setHighTravelerDsSource(oldHighTravelerDsSource);
                summaryModel.setHighTravelerDsUpdatetime(currentDateTime);
            }
        }

        // 合并是否援疆人员 (isYjPersonnel)
        String newIsYjPersonnelSource = summaryModel.getIsYjPersonnelSource();
        String oldIsYjPersonnelSource = userDimSummaryModelOld.getIsYjPersonnelSource();
        if (StringUtils.isNotBlank(oldIsYjPersonnelSource)) {
            if (StringUtils.isBlank(newIsYjPersonnelSource)) {
                model.setYjPersonnel(userDimModelOld.getYjPersonnel());
                summaryModel.setIsYjPersonnelSource(oldIsYjPersonnelSource);
                summaryModel.setIsYjPersonnelUpdatetime(currentDateTime);
            }
        }

        // 合并VIP标识 (vipFlag)
        String newVipFlagSource = summaryModel.getVipFlagSource();
        String oldVipFlagSource = userDimSummaryModelOld.getVipFlagSource();
        if (StringUtils.isNotBlank(oldVipFlagSource)) {
            if (StringUtils.isBlank(newVipFlagSource)) {
                model.setVipFlag(userDimModelOld.getVipFlag());
                summaryModel.setVipFlagSource(oldVipFlagSource);
                summaryModel.setVipFlagUpdatetime(currentDateTime);
            }
        }

        // 合并CIP标识 (cipFlag)
        String newCipFlagSource = summaryModel.getCipFlagSource();
        String oldCipFlagSource = userDimSummaryModelOld.getCipFlagSource();
        if (StringUtils.isNotBlank(oldCipFlagSource)) {
            if (StringUtils.isBlank(newCipFlagSource)) {
                model.setCipFlag(userDimModelOld.getCipFlag());
                summaryModel.setCipFlagSource(oldCipFlagSource);
                summaryModel.setCipFlagUpdatetime(currentDateTime);
            }
        }

        // 合并VVIP标识 (vvipFlag)
        String newVvipFlagSource = summaryModel.getVvipFlagSource();
        String oldVvipFlagSource = userDimSummaryModelOld.getVvipFlagSource();
        if (StringUtils.isNotBlank(oldVvipFlagSource)) {
            if (StringUtils.isBlank(newVvipFlagSource)) {
                model.setVvipFlag(userDimModelOld.getVvipFlag());
                summaryModel.setVvipFlagSource(oldVvipFlagSource);
                summaryModel.setVvipFlagUpdatetime(currentDateTime);
            }
        }

        // 合并餐食喜好 (foodPreference)
        String newFoodPreferenceSource = summaryModel.getFoodPreferenceSource();
        String oldFoodPreferenceSource = userDimSummaryModelOld.getFoodPreferenceSource();
        if (StringUtils.isNotBlank(oldFoodPreferenceSource)) {
            if (StringUtils.isBlank(newFoodPreferenceSource)) {
                model.setFoodPreference(userDimModelOld.getFoodPreference());
                summaryModel.setFoodPreferenceSource(oldFoodPreferenceSource);
                summaryModel.setFoodPreferenceUpdatetime(currentDateTime);
            }
        }

        // 合并座位喜好 (seatPreference)
        String newSeatPreferenceSource = summaryModel.getSeatPreferenceSource();
        String oldSeatPreferenceSource = userDimSummaryModelOld.getSeatPreferenceSource();
        if (StringUtils.isNotBlank(oldSeatPreferenceSource)) {
            if (StringUtils.isBlank(newSeatPreferenceSource)) {
                model.setSeatPreference(userDimModelOld.getSeatPreference());
                summaryModel.setSeatPreferenceSource(oldSeatPreferenceSource);
                summaryModel.setSeatPreferenceUpdatetime(currentDateTime);
            }
        }

        // 合并临时餐食喜好 (tempFoodPreference)
        String newTempFoodPreferenceSource = summaryModel.getTempFoodPreferenceSource();
        String oldTempFoodPreferenceSource = userDimSummaryModelOld.getTempFoodPreferenceSource();
        if (StringUtils.isNotBlank(oldTempFoodPreferenceSource)) {
            if (StringUtils.isBlank(newTempFoodPreferenceSource)) {
                model.setTempFoodPreference(userDimModelOld.getTempFoodPreference());
                summaryModel.setTempFoodPreferenceSource(oldTempFoodPreferenceSource);
                summaryModel.setTempFoodPreferenceUpdatetime(currentDateTime);
            }
        }

        // 合并临时座位喜好 (tempSeatPreference)
        String newTempSeatPreferenceSource = summaryModel.getTempSeatPreferenceSource();
        String oldTempSeatPreferenceSource = userDimSummaryModelOld.getTempSeatPreferenceSource();
        if (StringUtils.isNotBlank(oldTempSeatPreferenceSource)) {
            if (StringUtils.isBlank(newTempSeatPreferenceSource)) {
                model.setTempSeatPreference(userDimModelOld.getTempSeatPreference());
                summaryModel.setTempSeatPreferenceSource(oldTempSeatPreferenceSource);
                summaryModel.setTempSeatPreferenceUpdatetime(currentDateTime);
            }
        }

        // 合并饮品喜好 (beveragePreference)
        String newBeveragePreferenceSource = summaryModel.getBeveragePreferenceSource();
        String oldBeveragePreferenceSource = userDimSummaryModelOld.getBeveragePreferenceSource();
        if (StringUtils.isNotBlank(oldBeveragePreferenceSource)) {
            if (StringUtils.isBlank(newBeveragePreferenceSource)) {
                model.setBeveragePreference(userDimModelOld.getBeveragePreference());
                summaryModel.setBeveragePreferenceSource(oldBeveragePreferenceSource);
                summaryModel.setBeveragePreferenceUpdatetime(currentDateTime);
            }
        }

        // 合并长期座位喜好 (longTermSeatPreference)
        String newLongTermSeatPreferenceSource = summaryModel.getLongTermSeatPreferenceSource();
        String oldLongTermSeatPreferenceSource = userDimSummaryModelOld.getLongTermSeatPreferenceSource();
        if (StringUtils.isNotBlank(oldLongTermSeatPreferenceSource)) {
            if (StringUtils.isBlank(newLongTermSeatPreferenceSource)) {
                model.setLongTermSeatPreference(userDimModelOld.getLongTermSeatPreference());
                summaryModel.setLongTermSeatPreferenceSource(oldLongTermSeatPreferenceSource);
                summaryModel.setLongTermSeatPreferenceUpdatetime(currentDateTime);
            }
        }

        // 合并头等舱休息室喜好 (firstClassLoungePreference)
        String newFirstClassLoungePreferenceSource = summaryModel.getFirstClassLoungePreferenceSource();
        String oldFirstClassLoungePreferenceSource = userDimSummaryModelOld.getFirstClassLoungePreferenceSource();
        if (StringUtils.isNotBlank(oldFirstClassLoungePreferenceSource)) {
            if (StringUtils.isBlank(newFirstClassLoungePreferenceSource)) {
                model.setFirstClassLoungePreference(userDimModelOld.getFirstClassLoungePreference());
                summaryModel.setFirstClassLoungePreferenceSource(oldFirstClassLoungePreferenceSource);
                summaryModel.setFirstClassLoungePreferenceUpdatetime(currentDateTime);
            }
        }

        // 合并常客卡号是否经过验证 (isFrequentFlyerNumberVerified)
        String newIsFrequentFlyerNumberVerifiedSource = summaryModel.getIsFrequentFlyerNumberVerifiedSource();
        String oldIsFrequentFlyerNumberVerifiedSource = userDimSummaryModelOld.getIsFrequentFlyerNumberVerifiedSource();
        if (StringUtils.isNotBlank(oldIsFrequentFlyerNumberVerifiedSource)) {
            if (StringUtils.isBlank(newIsFrequentFlyerNumberVerifiedSource)) {
                model.setFrequentFlyerNumberVerified(userDimModelOld.getFrequentFlyerNumberVerified());
                summaryModel.setIsFrequentFlyerNumberVerifiedSource(oldIsFrequentFlyerNumberVerifiedSource);
                summaryModel.setIsFrequentFlyerNumberVerifiedUpdatetime(currentDateTime);
            }
        }
        String updateMergeUserDimSql = "insert into " + Constants.DIM_DB + ".T_DIM_USER_DIM (\n" +
                "PK_ID\n" +
                ",CRM_CUSTOMER_ID\n" +
                ",LY_VIP_ID\n" +
                ",LY_MEMBER_ID\n" +
                ",SYS_REGISTER_ID\n" +
                ",FFP_REGISTER_ID\n" +
                ",HISTORICAL_FFP_ID\n" +
                ",ALIPAY_ID\n" +
                ",WECHAT_ID\n" +
                ",DOUYIN_ID\n" +
                ",CN_NAME\n" +
                ",EN_NAME\n" +
                ",SEX\n" +
                ",USER_TYPE\n" +
                ",BIRTHDAY\n" +
                ",PROVINCE\n" +
                ",CITY\n" +
                ",NATIONALITY\n" +
                ",ETHNICITY\n" +
                ",EMPLOYER\n" +
                ",MOBILE_PHONE\n" +
                ",MOBILE_NUMBER_RELIABILITY\n" +
                ",EMAIL\n" +
                ",ID_CARD\n" +
                ",PASSPORT\n" +
                ",SEAMAN_ID\n" +
                ",ALIEN_PERMIT\n" +
                ",DIPLOMATIC_STAFF_CERTIFICATE\n" +
                ",PERMANENT_RESIDENT_ID\n" +
                ",CIVILIAN_STAFF_ID\n" +
                ",STAFF_ID\n" +
                ",OFFICER_ID_CARD\n" +
                ",ARMED_POLICE_OFFICER\n" +
                ",ARMED_POLICE_SOLDIER\n" +
                ",CIVILIAN_OFFICIAL_ID\n" +
                ",CONSCRIPT_SOLDIER_ID\n" +
                ",NON_COMMISSIONED_OFFICER_ID\n" +
                ",HK_MACAO_RESIDENT_PERMIT\n" +
                ",TAIWAN_RESIDENT_TRAVEL_PERMIT\n" +
                ",BLIND_PASSENGER\n" +
                ",DEAF_PASSENGER\n" +
                ",IS_DIRECT_USER\n" +
                ",IS_OFFICIAL_WEBSITE_NON_REGISTERED\n" +
                ",IS_DOUYIN_CARD_PURCHASER\n" +
                ",DIRECT_USER_STATUS\n" +
                ",DIRECT_REGISTER_DATE\n" +
                ",DIRECT_LASTLOGIN_DATE\n" +
                ",DIRECT_VERIFIED_FLAG\n" +
                ",DIRECT_VERIFY_DATE\n" +
                ",IS_BLACKLIST_USER\n" +
                ",STUDENT_FLAG\n" +
                ",TEACHER_FLAG\n" +
                ",AGENT_FLAG\n" +
                ",IS_KEY_ACCOUNT\n" +
                ",KEY_ACCOUNT_NUMBER\n" +
                ",IS_FREQUENT_TRAVELER\n" +
                ",FREQUENT_TRAVELER_CARDNO\n" +
                ",FREQUENT_TRAVELER_LEVEL\n" +
                ",YJ_CARD_NUMBER\n" +
                ",YJ_CARD_EXPIREDATE\n" +
                ",DEV_CHANNEL_ONE\n" +
                ",DEV_CHANNEL_TWO\n" +
                ",DEV_CHANNEL_THREE\n" +
                ",DEV_CHANNEL_FOUR\n" +
                ",FT_REGISTER_TIME\n" +
                ",ACCEPT_SMS_MARKETING\n" +
                ",ACCEPT_EMAIL_MARKETING\n" +
                ",PARENT_FT_CARDNO\n" +
                ",IS_LY_USER\n" +
                ",LY_REGISTER_TIME\n" +
                ",LY_CARD_NUMBER\n" +
                ",LY_USER_LEVEL\n" +
                ",LY_USER_STATUS\n" +
                ",LY_REGISTER_STATUS\n" +
                ",LY_VERIFY_STATUS\n" +
                ",LY_LIFETIME_POINTS\n" +
                ",LY_AVAILABLE_POINTS\n" +
                ",IS_HIGH_TRAVELER\n" +
                ",HIGH_TRAVELER_TYPE\n" +
                ",HIGH_TRAVELER_TIER\n" +
                ",TIER_PRESTIGE_EXPIREDATE\n" +
                ",TIER_HONOR_EXPIREDATE\n" +
                ",HIGH_TRAVELER_DS\n" +
                ",IS_YJ_PERSONNEL\n" +
                ",VIP_FLAG\n" +
                ",CIP_FLAG\n" +
                ",VVIP_FLAG\n" +
                ",FOOD_PREFERENCE\n" +
                ",SEAT_PREFERENCE\n" +
                ",TEMP_FOOD_PREFERENCE\n" +
                ",TEMP_SEAT_PREFERENCE\n" +
                ",BEVERAGE_PREFERENCE\n" +
                ",LONG_TERM_SEAT_PREFERENCE\n" +
                ",FIRST_CLASS_LOUNGE_PREFERENCE\n" +
                ",IS_FREQUENT_FLYER_NUMBER_VERIFIED\n" +
                ",UPDATE_TIME) values " +
                " (" +
                GetTableSql.getFieldStr(model.getPkId()) +
                GetTableSql.getFieldStr(model.getCrmCustomerId()) +
                GetTableSql.getFieldStr(model.getLyVipId()) +
                GetTableSql.getFieldStr(model.getLyMemberId()) +
                GetTableSql.getFieldStr(model.getSysRegisterId()) +
                GetTableSql.getFieldStr(model.getFfpRegisterId()) +
                GetTableSql.getFieldStr(model.getHistoricalFfpId()) +
                GetTableSql.getFieldStr(model.getAlipayId()) +
                GetTableSql.getFieldStr(model.getWechatId()) +
                GetTableSql.getFieldStr(model.getDouyinId()) +
                GetTableSql.getFieldStr(model.getCnName()) +
                GetTableSql.getFieldStr(model.getEnName()) +
                GetTableSql.getFieldStr(model.getSex()) +
                GetTableSql.getFieldStr(model.getUserType()) +
                GetTableSql.getFieldStr(model.getBirthday()) +
                GetTableSql.getFieldStr(model.getProvince()) +
                GetTableSql.getFieldStr(model.getCity()) +
                GetTableSql.getFieldStr(model.getNationality()) +
                GetTableSql.getFieldStr(model.getEthnicity()) +
                GetTableSql.getFieldStr(model.getEmployer()) +
                GetTableSql.getFieldStr(model.getMobilePhone()) +
                GetTableSql.getFieldStr(model.getMobileNumberReliability()) +
                GetTableSql.getFieldStr(model.getEmail()) +
                GetTableSql.getFieldStr(model.getIdCard()) +
                GetTableSql.getFieldStr(model.getPassport()) +
                GetTableSql.getFieldStr(model.getSeamanId()) +
                GetTableSql.getFieldStr(model.getAlienPermit()) +
                GetTableSql.getFieldStr(model.getDiplomaticStaffCertificate()) +
                GetTableSql.getFieldStr(model.getPermanentResidentId()) +
                GetTableSql.getFieldStr(model.getCivilianStaffId()) +
                GetTableSql.getFieldStr(model.getStaffId()) +
                GetTableSql.getFieldStr(model.getOfficerIdCard()) +
                GetTableSql.getFieldStr(model.getArmedPoliceOfficer()) +
                GetTableSql.getFieldStr(model.getArmedPoliceSoldier()) +
                GetTableSql.getFieldStr(model.getCivilianOfficialId()) +
                GetTableSql.getFieldStr(model.getConscriptSoldierId()) +
                GetTableSql.getFieldStr(model.getNonCommissionedOfficerId()) +
                GetTableSql.getFieldStr(model.getHkMacaoResidentPermit()) +
                GetTableSql.getFieldStr(model.getTaiwanResidentTravelPermit()) +
                GetTableSql.getFieldStr(model.getBlindPassenger()) +
                GetTableSql.getFieldStr(model.getDeafPassenger()) +
                GetTableSql.getFieldStr(model.getDirectUser()) +
                GetTableSql.getFieldStr(model.getOfficialWebsiteNonRegistered()) +
                GetTableSql.getFieldStr(model.getDouyinCardPurchaser()) +
                GetTableSql.getFieldStr(model.getDirectUserStatus()) +
                GetTableSql.getFieldStr(model.getDirectRegisterDate()) +
                GetTableSql.getFieldStr(model.getDirectLastloginDate()) +
                GetTableSql.getFieldStr(model.getDirectVerifiedFlag()) +
                GetTableSql.getFieldStr(model.getDirectVerifyDate()) +
                GetTableSql.getFieldStr(model.getBlacklistUser()) +
                GetTableSql.getFieldStr(model.getStudentFlag()) +
                GetTableSql.getFieldStr(model.getTeacherFlag()) +
                GetTableSql.getFieldStr(model.getAgentFlag()) +
                GetTableSql.getFieldStr(model.getKeyAccount()) +
                GetTableSql.getFieldStr(model.getKeyAccountNumber()) +
                GetTableSql.getFieldStr(model.getFrequentTraveler()) +
                GetTableSql.getFieldStr(model.getFrequentTravelerCardno()) +
                GetTableSql.getFieldStr(model.getFrequentTravelerLevel()) +
                GetTableSql.getFieldStr(model.getYjCardNumber()) +
                GetTableSql.getFieldStr(model.getYjCardExpiredate()) +
                GetTableSql.getFieldStr(model.getDevChannelOne()) +
                GetTableSql.getFieldStr(model.getDevChannelTwo()) +
                GetTableSql.getFieldStr(model.getDevChannelThree()) +
                GetTableSql.getFieldStr(model.getDevChannelFour()) +
                GetTableSql.getFieldStr(model.getFtRegisterTime()) +
                GetTableSql.getFieldStr(model.getAcceptSmsMarketing()) +
                GetTableSql.getFieldStr(model.getAcceptEmailMarketing()) +
                GetTableSql.getFieldStr(model.getParentFtCardno()) +
                GetTableSql.getFieldStr(model.getLyUser()) +
                GetTableSql.getFieldStr(model.getLyRegisterTime()) +
                GetTableSql.getFieldStr(model.getLyCardNumber()) +
                GetTableSql.getFieldStr(model.getLyUserLevel()) +
                GetTableSql.getFieldStr(model.getLyUserStatus()) +
                GetTableSql.getFieldStr(model.getLyRegisterStatus()) +
                GetTableSql.getFieldStr(model.getLyVerifyStatus()) +
                GetTableSql.getFieldStr(model.getLyLifetimePoints()) +
                GetTableSql.getFieldStr(model.getLyAvailablePoints()) +
                GetTableSql.getFieldStr(model.getHighTraveler()) +
                GetTableSql.getFieldStr(model.getHighTravelerType()) +
                GetTableSql.getFieldStr(model.getHighTravelerTier()) +
                GetTableSql.getFieldStr(model.getTierPrestigeExpiredate()) +
                GetTableSql.getFieldStr(model.getTierHonorExpiredate()) +
                GetTableSql.getFieldStr(model.getHighTravelerDs()) +
                GetTableSql.getFieldStr(model.getYjPersonnel()) +
                GetTableSql.getFieldStr(model.getVipFlag()) +
                GetTableSql.getFieldStr(model.getCipFlag()) +
                GetTableSql.getFieldStr(model.getVvipFlag()) +
                GetTableSql.getFieldStr(model.getFoodPreference()) +
                GetTableSql.getFieldStr(model.getSeatPreference()) +
                GetTableSql.getFieldStr(model.getTempFoodPreference()) +
                GetTableSql.getFieldStr(model.getTempSeatPreference()) +
                GetTableSql.getFieldStr(model.getBeveragePreference()) +
                GetTableSql.getFieldStr(model.getLongTermSeatPreference()) +
                GetTableSql.getFieldStr(model.getFirstClassLoungePreference()) +
                GetTableSql.getFieldStr(model.getFrequentFlyerNumberVerified()) +
                "CURRENT_TIMESTAMP(3)" +
                ")";
        String updateMergedUserDimSql = "insert into DIM_TEST.T_DIM_USER_DIM (" +
                "PK_ID\n" +
                ",MERGED_TO_USERID\n" +
                ",IS_VALID_USER\n" +
                ",UPDATE_TIME) VALUES ( \n" +
                "'" + tidOld + "',\n" +
                "'" + tidEarly + "',\n" +
                "false,\n" +
                "CURRENT_TIMESTAMP(3)\n" +
                ")";
        String updateUserDimSummarySql = "insert into DIM_TEST.T_DIM_USER_DIM_SUMMARY (\n" +
                "PK_ID\n" +
                ",CRM_CUSTOMER_ID_SOURCE\n" +
                ",LY_VIP_ID_SOURCE\n" +
                ",LY_MEMBER_ID_SOURCE\n" +
                ",SYS_REGISTER_ID_SOURCE\n" +
                ",FFP_REGISTER_ID_SOURCE\n" +
                ",HISTORICAL_FFP_ID_SOURCE\n" +
                ",ALIPAY_ID_SOURCE\n" +
                ",WECHAT_ID_SOURCE\n" +
                ",DOUYIN_ID_SOURCE\n" +
                ",CN_NAME_SOURCE\n" +
                ",EN_NAME_SOURCE\n" +
                ",SEX_SOURCE\n" +
                ",USER_TYPE_SOURCE\n" +
                ",BIRTHDAY_SOURCE\n" +
                ",PROVINCE_SOURCE\n" +
                ",CITY_SOURCE\n" +
                ",NATIONALITY_SOURCE\n" +
                ",ETHNICITY_SOURCE\n" +
                ",EMPLOYER_SOURCE\n" +
                ",MOBILE_PHONE_SOURCE\n" +
                ",MOBILE_NUMBER_RELIABILITY_SOURCE\n" +
                ",EMAIL_SOURCE\n" +
                ",ID_CARD_SOURCE\n" +
                ",PASSPORT_SOURCE\n" +
                ",SEAMAN_ID_SOURCE\n" +
                ",ALIEN_PERMIT_SOURCE\n" +
                ",DIPLOMATIC_STAFF_CERTIFICATE_SOURCE\n" +
                ",PERMANENT_RESIDENT_ID_SOURCE\n" +
                ",CIVILIAN_STAFF_ID_SOURCE\n" +
                ",STAFF_ID_SOURCE\n" +
                ",OFFICER_ID_CARD_SOURCE\n" +
                ",ARMED_POLICE_OFFICER_SOURCE\n" +
                ",ARMED_POLICE_SOLDIER_SOURCE\n" +
                ",CIVILIAN_OFFICIAL_ID_SOURCE\n" +
                ",CONSCRIPT_SOLDIER_ID_SOURCE\n" +
                ",NON_COMMISSIONED_OFFICER_ID_SOURCE\n" +
                ",HK_MACAO_RESIDENT_PERMIT_SOURCE\n" +
                ",TAIWAN_RESIDENT_TRAVEL_PERMIT_SOURCE\n" +
                ",BLIND_PASSENGER_SOURCE\n" +
                ",DEAF_PASSENGER_SOURCE\n" +
                ",IS_DIRECT_USER_SOURCE\n" +
                ",IS_OFFICIAL_WEBSITE_NON_REGISTERED_SOURCE\n" +
                ",IS_DOUYIN_CARD_PURCHASER_SOURCE\n" +
                ",DIRECT_USER_STATUS_SOURCE\n" +
                ",DIRECT_REGISTER_DATE_SOURCE\n" +
                ",DIRECT_LASTLOGIN_DATE_SOURCE\n" +
                ",DIRECT_VERIFIED_FLAG_SOURCE\n" +
                ",DIRECT_VERIFY_DATE_SOURCE\n" +
                ",IS_BLACKLIST_USER_SOURCE\n" +
                ",STUDENT_FLAG_SOURCE\n" +
                ",TEACHER_FLAG_SOURCE\n" +
                ",AGENT_FLAG_SOURCE\n" +
                ",IS_KEY_ACCOUNT_SOURCE\n" +
                ",KEY_ACCOUNT_NUMBER_SOURCE\n" +
                ",IS_FREQUENT_TRAVELER_SOURCE\n" +
                ",FREQUENT_TRAVELER_CARDNO_SOURCE\n" +
                ",FREQUENT_TRAVELER_LEVEL_SOURCE\n" +
                ",YJ_CARD_NUMBER_SOURCE\n" +
                ",YJ_CARD_EXPIREDATE_SOURCE\n" +
                ",DEV_CHANNEL_ONE_SOURCE\n" +
                ",DEV_CHANNEL_TWO_SOURCE\n" +
                ",DEV_CHANNEL_THREE_SOURCE\n" +
                ",DEV_CHANNEL_FOUR_SOURCE\n" +
                ",FT_REGISTER_TIME_SOURCE\n" +
                ",ACCEPT_SMS_MARKETING_SOURCE\n" +
                ",ACCEPT_EMAIL_MARKETING_SOURCE\n" +
                ",PARENT_FT_CARDNO_SOURCE\n" +
                ",IS_LY_USER_SOURCE\n" +
                ",LY_REGISTER_TIME_SOURCE\n" +
                ",LY_CARD_NUMBER_SOURCE\n" +
                ",LY_USER_LEVEL_SOURCE\n" +
                ",LY_USER_STATUS_SOURCE\n" +
                ",LY_REGISTER_STATUS_SOURCE\n" +
                ",LY_VERIFY_STATUS_SOURCE\n" +
                ",LY_LIFETIME_POINTS_SOURCE\n" +
                ",LY_AVAILABLE_POINTS_SOURCE\n" +
                ",IS_HIGH_TRAVELER_SOURCE\n" +
                ",HIGH_TRAVELER_TYPE_SOURCE\n" +
                ",HIGH_TRAVELER_TIER_SOURCE\n" +
                ",TIER_PRESTIGE_EXPIREDATE_SOURCE\n" +
                ",TIER_HONOR_EXPIREDATE_SOURCE\n" +
                ",HIGH_TRAVELER_DS_SOURCE\n" +
                ",IS_YJ_PERSONNEL_SOURCE\n" +
                ",VIP_FLAG_SOURCE\n" +
                ",CIP_FLAG_SOURCE\n" +
                ",VVIP_FLAG_SOURCE\n" +
                ",FOOD_PREFERENCE_SOURCE\n" +
                ",SEAT_PREFERENCE_SOURCE\n" +
                ",TEMP_FOOD_PREFERENCE_SOURCE\n" +
                ",TEMP_SEAT_PREFERENCE_SOURCE\n" +
                ",BEVERAGE_PREFERENCE_SOURCE\n" +
                ",LONG_TERM_SEAT_PREFERENCE_SOURCE\n" +
                ",FIRST_CLASS_LOUNGE_PREFERENCE_SOURCE\n" +
                ",MERGED_TO_USERID_SOURCE\n" +
                ",IS_VALID_USER_SOURCE\n" +
                ",IS_FREQUENT_FLYER_NUMBER_VERIFIED_SOURCE\n" +
                ",CRM_CUSTOMER_ID_UPDATETIME\n" +
                ",LY_VIP_ID_UPDATETIME\n" +
                ",LY_MEMBER_ID_UPDATETIME\n" +
                ",SYS_REGISTER_ID_UPDATETIME\n" +
                ",FFP_REGISTER_ID_UPDATETIME\n" +
                ",HISTORICAL_FFP_ID_UPDATETIME\n" +
                ",ALIPAY_ID_UPDATETIME\n" +
                ",WECHAT_ID_UPDATETIME\n" +
                ",DOUYIN_ID_UPDATETIME\n" +
                ",CN_NAME_UPDATETIME\n" +
                ",EN_NAME_UPDATETIME\n" +
                ",SEX_UPDATETIME\n" +
                ",USER_TYPE_UPDATETIME\n" +
                ",BIRTHDAY_UPDATETIME\n" +
                ",PROVINCE_UPDATETIME\n" +
                ",CITY_UPDATETIME\n" +
                ",NATIONALITY_UPDATETIME\n" +
                ",ETHNICITY_UPDATETIME\n" +
                ",EMPLOYER_UPDATETIME\n" +
                ",MOBILE_PHONE_UPDATETIME\n" +
                ",MOBILE_NUMBER_RELIABILITY_UPDATETIME\n" +
                ",EMAIL_UPDATETIME\n" +
                ",ID_CARD_UPDATETIME\n" +
                ",PASSPORT_UPDATETIME\n" +
                ",SEAMAN_ID_UPDATETIME\n" +
                ",ALIEN_PERMIT_UPDATETIME\n" +
                ",DIPLOMATIC_STAFF_CERTIFICATE_UPDATETIME\n" +
                ",PERMANENT_RESIDENT_ID_UPDATETIME\n" +
                ",CIVILIAN_STAFF_ID_UPDATETIME\n" +
                ",STAFF_ID_UPDATETIME\n" +
                ",OFFICER_ID_CARD_UPDATETIME\n" +
                ",ARMED_POLICE_OFFICER_UPDATETIME\n" +
                ",ARMED_POLICE_SOLDIER_UPDATETIME\n" +
                ",CIVILIAN_OFFICIAL_ID_UPDATETIME\n" +
                ",CONSCRIPT_SOLDIER_ID_UPDATETIME\n" +
                ",NON_COMMISSIONED_OFFICER_ID_UPDATETIME\n" +
                ",HK_MACAO_RESIDENT_PERMIT_UPDATETIME\n" +
                ",TAIWAN_RESIDENT_TRAVEL_PERMIT_UPDATETIME\n" +
                ",BLIND_PASSENGER_UPDATETIME\n" +
                ",DEAF_PASSENGER_UPDATETIME\n" +
                ",IS_DIRECT_USER_UPDATETIME\n" +
                ",IS_OFFICIAL_WEBSITE_NON_REGISTERED_UPDATETIME\n" +
                ",IS_DOUYIN_CARD_PURCHASER_UPDATETIME\n" +
                ",DIRECT_USER_STATUS_UPDATETIME\n" +
                ",DIRECT_REGISTER_DATE_UPDATETIME\n" +
                ",DIRECT_LASTLOGIN_DATE_UPDATETIME\n" +
                ",DIRECT_VERIFIED_FLAG_UPDATETIME\n" +
                ",DIRECT_VERIFY_DATE_UPDATETIME\n" +
                ",IS_BLACKLIST_USER_UPDATETIME\n" +
                ",STUDENT_FLAG_UPDATETIME\n" +
                ",TEACHER_FLAG_UPDATETIME\n" +
                ",AGENT_FLAG_UPDATETIME\n" +
                ",IS_KEY_ACCOUNT_UPDATETIME\n" +
                ",KEY_ACCOUNT_NUMBER_UPDATETIME\n" +
                ",IS_FREQUENT_TRAVELER_UPDATETIME\n" +
                ",FREQUENT_TRAVELER_CARDNO_UPDATETIME\n" +
                ",FREQUENT_TRAVELER_LEVEL_UPDATETIME\n" +
                ",YJ_CARD_NUMBER_UPDATETIME\n" +
                ",YJ_CARD_EXPIREDATE_UPDATETIME\n" +
                ",DEV_CHANNEL_ONE_UPDATETIME\n" +
                ",DEV_CHANNEL_TWO_UPDATETIME\n" +
                ",DEV_CHANNEL_THREE_UPDATETIME\n" +
                ",DEV_CHANNEL_FOUR_UPDATETIME\n" +
                ",FT_REGISTER_TIME_UPDATETIME\n" +
                ",ACCEPT_SMS_MARKETING_UPDATETIME\n" +
                ",ACCEPT_EMAIL_MARKETING_UPDATETIME\n" +
                ",PARENT_FT_CARDNO_UPDATETIME\n" +
                ",IS_LY_USER_UPDATETIME\n" +
                ",LY_REGISTER_TIME_UPDATETIME\n" +
                ",LY_CARD_NUMBER_UPDATETIME\n" +
                ",LY_USER_LEVEL_UPDATETIME\n" +
                ",LY_USER_STATUS_UPDATETIME\n" +
                ",LY_REGISTER_STATUS_UPDATETIME\n" +
                ",LY_VERIFY_STATUS_UPDATETIME\n" +
                ",LY_LIFETIME_POINTS_UPDATETIME\n" +
                ",LY_AVAILABLE_POINTS_UPDATETIME\n" +
                ",IS_HIGH_TRAVELER_UPDATETIME\n" +
                ",HIGH_TRAVELER_TYPE_UPDATETIME\n" +
                ",HIGH_TRAVELER_TIER_UPDATETIME\n" +
                ",TIER_PRESTIGE_EXPIREDATE_UPDATETIME\n" +
                ",TIER_HONOR_EXPIREDATE_UPDATETIME\n" +
                ",HIGH_TRAVELER_DS_UPDATETIME\n" +
                ",IS_YJ_PERSONNEL_UPDATETIME\n" +
                ",VIP_FLAG_UPDATETIME\n" +
                ",CIP_FLAG_UPDATETIME\n" +
                ",VVIP_FLAG_UPDATETIME\n" +
                ",FOOD_PREFERENCE_UPDATETIME\n" +
                ",SEAT_PREFERENCE_UPDATETIME\n" +
                ",TEMP_FOOD_PREFERENCE_UPDATETIME\n" +
                ",TEMP_SEAT_PREFERENCE_UPDATETIME\n" +
                ",BEVERAGE_PREFERENCE_UPDATETIME\n" +
                ",LONG_TERM_SEAT_PREFERENCE_UPDATETIME\n" +
                ",FIRST_CLASS_LOUNGE_PREFERENCE_UPDATETIME\n" +
                ",MERGED_TO_USERID_UPDATETIME\n" +
                ",IS_VALID_USER_UPDATETIME\n" +
                ",IS_FREQUENT_FLYER_NUMBER_VERIFIED_UPDATETIME) VALUES ( " +
                GetTableSql.getFieldStr(summaryModel.getPkId()) +
                GetTableSql.getFieldStr(summaryModel.getCrmCustomerIdSource()) +
                GetTableSql.getFieldStr(summaryModel.getLyVipIdSource()) +
                GetTableSql.getFieldStr(summaryModel.getLyMemberIdSource()) +
                GetTableSql.getFieldStr(summaryModel.getSysRegisterIdSource()) +
                GetTableSql.getFieldStr(summaryModel.getFfpRegisterIdSource()) +
                GetTableSql.getFieldStr(summaryModel.getHistoricalFfpIdSource()) +
                GetTableSql.getFieldStr(summaryModel.getAlipayIdSource()) +
                GetTableSql.getFieldStr(summaryModel.getWechatIdSource()) +
                GetTableSql.getFieldStr(summaryModel.getDouyinIdSource()) +
                GetTableSql.getFieldStr(summaryModel.getCnNameSource()) +
                GetTableSql.getFieldStr(summaryModel.getEnNameSource()) +
                GetTableSql.getFieldStr(summaryModel.getSexSource()) +
                GetTableSql.getFieldStr(summaryModel.getUserTypeSource()) +
                GetTableSql.getFieldStr(summaryModel.getBirthdaySource()) +
                GetTableSql.getFieldStr(summaryModel.getProvinceSource()) +
                GetTableSql.getFieldStr(summaryModel.getCitySource()) +
                GetTableSql.getFieldStr(summaryModel.getNationalitySource()) +
                GetTableSql.getFieldStr(summaryModel.getEthnicitySource()) +
                GetTableSql.getFieldStr(summaryModel.getEmployerSource()) +
                GetTableSql.getFieldStr(summaryModel.getMobilePhoneSource()) +
                GetTableSql.getFieldStr(summaryModel.getMobileNumberReliabilitySource()) +
                GetTableSql.getFieldStr(summaryModel.getEmailSource()) +
                GetTableSql.getFieldStr(summaryModel.getIdCardSource()) +
                GetTableSql.getFieldStr(summaryModel.getPassportSource()) +
                GetTableSql.getFieldStr(summaryModel.getSeamanIdSource()) +
                GetTableSql.getFieldStr(summaryModel.getAlienPermitSource()) +
                GetTableSql.getFieldStr(summaryModel.getDiplomaticStaffCertificateSource()) +
                GetTableSql.getFieldStr(summaryModel.getPermanentResidentIdSource()) +
                GetTableSql.getFieldStr(summaryModel.getCivilianStaffIdSource()) +
                GetTableSql.getFieldStr(summaryModel.getStaffIdSource()) +
                GetTableSql.getFieldStr(summaryModel.getOfficerIdCardSource()) +
                GetTableSql.getFieldStr(summaryModel.getArmedPoliceOfficerSource()) +
                GetTableSql.getFieldStr(summaryModel.getArmedPoliceSoldierSource()) +
                GetTableSql.getFieldStr(summaryModel.getCivilianOfficialIdSource()) +
                GetTableSql.getFieldStr(summaryModel.getConscriptSoldierIdSource()) +
                GetTableSql.getFieldStr(summaryModel.getNonCommissionedOfficerIdSource()) +
                GetTableSql.getFieldStr(summaryModel.getHkMacaoResidentPermitSource()) +
                GetTableSql.getFieldStr(summaryModel.getTaiwanResidentTravelPermitSource()) +
                GetTableSql.getFieldStr(summaryModel.getBlindPassengerSource()) +
                GetTableSql.getFieldStr(summaryModel.getDeafPassengerSource()) +
                GetTableSql.getFieldStr(summaryModel.getIsDirectUserSource()) +
                GetTableSql.getFieldStr(summaryModel.getIsOfficialWebsiteNonRegisteredSource()) +
                GetTableSql.getFieldStr(summaryModel.getIsDouyinCardPurchaserSource()) +
                GetTableSql.getFieldStr(summaryModel.getDirectUserStatusSource()) +
                GetTableSql.getFieldStr(summaryModel.getDirectRegisterDateSource()) +
                GetTableSql.getFieldStr(summaryModel.getDirectLastLoginDateSource()) +
                GetTableSql.getFieldStr(summaryModel.getDirectVerifiedFlagSource()) +
                GetTableSql.getFieldStr(summaryModel.getDirectVerifyDateSource()) +
                GetTableSql.getFieldStr(summaryModel.getIsBlacklistUserSource()) +
                GetTableSql.getFieldStr(summaryModel.getStudentFlagSource()) +
                GetTableSql.getFieldStr(summaryModel.getTeacherFlagSource()) +
                GetTableSql.getFieldStr(summaryModel.getAgentFlagSource()) +
                GetTableSql.getFieldStr(summaryModel.getIsKeyAccountSource()) +
                GetTableSql.getFieldStr(summaryModel.getKeyAccountNumberSource()) +
                GetTableSql.getFieldStr(summaryModel.getIsFrequentTravelerSource()) +
                GetTableSql.getFieldStr(summaryModel.getFrequentTravelerCardnoSource()) +
                GetTableSql.getFieldStr(summaryModel.getFrequentTravelerLevelSource()) +
                GetTableSql.getFieldStr(summaryModel.getYjCardNumberSource()) +
                GetTableSql.getFieldStr(summaryModel.getYjCardExpiredateSource()) +
                GetTableSql.getFieldStr(summaryModel.getDevChannelOneSource()) +
                GetTableSql.getFieldStr(summaryModel.getDevChannelTwoSource()) +
                GetTableSql.getFieldStr(summaryModel.getDevChannelThreeSource()) +
                GetTableSql.getFieldStr(summaryModel.getDevChannelFourSource()) +
                GetTableSql.getFieldStr(summaryModel.getFtRegisterTimeSource()) +
                GetTableSql.getFieldStr(summaryModel.getAcceptSmsMarketingSource()) +
                GetTableSql.getFieldStr(summaryModel.getAcceptEmailMarketingSource()) +
                GetTableSql.getFieldStr(summaryModel.getParentFtCardnoSource()) +
                GetTableSql.getFieldStr(summaryModel.getIsLyUserSource()) +
                GetTableSql.getFieldStr(summaryModel.getLyRegisterTimeSource()) +
                GetTableSql.getFieldStr(summaryModel.getLyCardNumberSource()) +
                GetTableSql.getFieldStr(summaryModel.getLyUserLevelSource()) +
                GetTableSql.getFieldStr(summaryModel.getLyUserStatusSource()) +
                GetTableSql.getFieldStr(summaryModel.getLyRegisterStatusSource()) +
                GetTableSql.getFieldStr(summaryModel.getLyVerifyStatusSource()) +
                GetTableSql.getFieldStr(summaryModel.getLyLifetimePointsSource()) +
                GetTableSql.getFieldStr(summaryModel.getLyAvailablePointsSource()) +
                GetTableSql.getFieldStr(summaryModel.getIsHighTravelerSource()) +
                GetTableSql.getFieldStr(summaryModel.getHighTravelerTypeSource()) +
                GetTableSql.getFieldStr(summaryModel.getHighTravelerTierSource()) +
                GetTableSql.getFieldStr(summaryModel.getTierPrestigeExpiredateSource()) +
                GetTableSql.getFieldStr(summaryModel.getTierHonorExpiredateSource()) +
                GetTableSql.getFieldStr(summaryModel.getHighTravelerDsSource()) +
                GetTableSql.getFieldStr(summaryModel.getIsYjPersonnelSource()) +
                GetTableSql.getFieldStr(summaryModel.getVipFlagSource()) +
                GetTableSql.getFieldStr(summaryModel.getCipFlagSource()) +
                GetTableSql.getFieldStr(summaryModel.getVvipFlagSource()) +
                GetTableSql.getFieldStr(summaryModel.getFoodPreferenceSource()) +
                GetTableSql.getFieldStr(summaryModel.getSeatPreferenceSource()) +
                GetTableSql.getFieldStr(summaryModel.getTempFoodPreferenceSource()) +
                GetTableSql.getFieldStr(summaryModel.getTempSeatPreferenceSource()) +
                GetTableSql.getFieldStr(summaryModel.getBeveragePreferenceSource()) +
                GetTableSql.getFieldStr(summaryModel.getLongTermSeatPreferenceSource()) +
                GetTableSql.getFieldStr(summaryModel.getFirstClassLoungePreferenceSource()) +
                GetTableSql.getFieldStr(summaryModel.getMergedToUseridSource()) +
                GetTableSql.getFieldStr(summaryModel.getIsValidUserSource()) +
                GetTableSql.getFieldStr(summaryModel.getIsFrequentFlyerNumberVerifiedSource()) +
                GetTableSql.getFieldStr(summaryModel.getCrmCustomerIdUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getLyVipIdUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getLyMemberIdUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getSysRegisterIdUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getFfpRegisterIdUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getHistoricalFfpIdUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getAlipayIdUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getWechatIdUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getDouyinIdUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getCnNameUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getEnNameUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getSexUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getUserTypeUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getBirthdayUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getProvinceUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getCityUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getNationalityUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getEthnicityUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getEmployerUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getMobilePhoneUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getMobileNumberReliabilityUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getEmailUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getIdCardUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getPassportUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getSeamanIdUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getAlienPermitUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getDiplomaticStaffCertificateUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getPermanentResidentIdUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getCivilianStaffIdUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getStaffIdUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getOfficerIdCardUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getArmedPoliceOfficerUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getArmedPoliceSoldierUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getCivilianOfficialIdUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getConscriptSoldierIdUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getNonCommissionedOfficerIdUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getHkMacaoResidentPermitUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getTaiwanResidentTravelPermitUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getBlindPassengerUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getDeafPassengerUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getIsDirectUserUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getIsOfficialWebsiteNonRegisteredUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getIsDouyinCardPurchaserUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getDirectUserStatusUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getDirectRegisterDateUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getDirectLastLoginDateUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getDirectVerifiedFlagUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getDirectVerifyDateUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getIsBlacklistUserUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getStudentFlagUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getTeacherFlagUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getAgentFlagUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getIsKeyAccountUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getKeyAccountNumberUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getIsFrequentTravelerUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getFrequentTravelerCardnoUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getFrequentTravelerLevelUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getYjCardNumberUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getYjCardExpiredateUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getDevChannelOneUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getDevChannelTwoUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getDevChannelThreeUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getDevChannelFourUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getFtRegisterTimeUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getAcceptSmsMarketingUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getAcceptEmailMarketingUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getParentFtCardnoUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getIsLyUserUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getLyRegisterTimeUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getLyCardNumberUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getLyUserLevelUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getLyUserStatusUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getLyRegisterStatusUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getLyVerifyStatusUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getLyLifetimePointsUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getLyAvailablePointsUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getIsHighTravelerUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getHighTravelerTypeUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getHighTravelerTierUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getTierPrestigeExpiredateUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getTierHonorExpiredateUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getHighTravelerDsUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getIsYjPersonnelUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getVipFlagUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getCipFlagUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getVvipFlagUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getFoodPreferenceUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getSeatPreferenceUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getTempFoodPreferenceUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getTempSeatPreferenceUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getBeveragePreferenceUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getLongTermSeatPreferenceUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getFirstClassLoungePreferenceUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getMergedToUseridUpdatetime()) +
                GetTableSql.getFieldStr(summaryModel.getIsValidUserUpdatetime()) +
                GetTableSql.getFieldStrEnd(summaryModel.getIsFrequentFlyerNumberVerifiedUpdatetime()) +
                ")";
        DorisUtils.excuteDorisInsert(stmt, updateMergeUserDimSql);
        DorisUtils.excuteDorisInsert(stmt, updateMergedUserDimSql);
        DorisUtils.excuteDorisInsert(stmt, updateUserDimSummarySql);
        System.out.println("updateMergeUserDimSql:" + updateMergeUserDimSql);
        System.out.println("updateMergedUserDimSql:" + updateMergedUserDimSql);
        System.out.println("updateUserDimSummarySql:" + updateUserDimSummarySql);

        System.out.println(summaryModel.toString());
        System.out.println(userDimSummaryModelOld.toString());
        System.out.println(model.toString());
        System.out.println(userDimModelOld.toString());
    }

    /**
     * 合并证件类型和证件号强ID
     *
     * @param certificationOld
     * @param certificationNew
     * @param tidEarly
     * @param etlDate
     * @param values
     * @param finalTid
     * @return
     */
    public static void mergeCertification(Map<String, String> certificationOld, Map<String, String> certificationNew, String tidEarly, String tidOld, String etlDate, List<String> values, Tid finalTid) {
        //遍历老的证件信息
        for (String key :
                certificationOld.keySet()) {
            String certificationStrNew = "";
            if (certificationNew != null) {
                certificationStrNew = certificationNew.get(key);
            }
            String certificationStrOld = certificationOld.get(key);
            if (StringUtils.isNotBlank(certificationStrNew)) {
                if (StringUtils.isNotBlank(certificationStrOld)) {
                    //同类型的证件号，遍历证件号进行对比
                    String[] certificationsOld = certificationStrOld.split(",");
                    for (String certificationOlda :
                            certificationsOld) {
                        if (certificationStrNew.contains(certificationOlda)) {
                            //证件号一致，把原来挂载的tid置为无效
                            values.add("('" + etlDate + "','" + certificationOlda + "','" + key + "','" + tidOld + "',CURRENT_TIMESTAMP(3),0)");
                        } else {
                            //新的强id，进行合并，并进行挂载
                            certificationStrNew += "," + certificationOlda;
                            values.add("('" + etlDate + "','" + certificationOlda + "','" + key + "','" + tidEarly + "',CURRENT_TIMESTAMP(3),1)");
                        }
                    }
                    //更新合并后的证件信息
                    certificationNew.put(key, certificationStrNew);
                }
            } else {
                //新的证件类型信息为空,所有老的证件号挂载到新tid
                String[] certificationsOld = certificationStrOld.split(",");
                for (String certificationOlda :
                        certificationsOld) {
                    values.add("('" + etlDate + "','" + certificationOlda + "','" + key + "','" + tidEarly + "',CURRENT_TIMESTAMP(3),1)");
                }
                certificationNew.put(key, certificationStrOld);
            }
        }
        finalTid.setCertification(certificationNew);
    }

    /**
     * 合并鲁雁行卡号强ID
     *
     * @param lyCardNumberOld
     * @param lyCardNumberNew
     * @param tidEarly
     * @param etlDate
     * @param values
     * @param finalTid
     * @return
     */
    public static void mergeLyCardNumber(String lyCardNumberOld, String lyCardNumberNew, String tidEarly, String tidOld, String etlDate, List<String> values, Tid finalTid) {
        boolean first = true;
        for (String lyCardNumber : lyCardNumberOld.split(",")) {
            if (StringUtils.isNotBlank(lyCardNumberNew)) {
                if (lyCardNumberNew.contains(lyCardNumber)) {
                    //相同强id，把原来挂载的tid置为无效
                    values.add("('" + etlDate + "','" + lyCardNumber + "','LYC','" + tidOld + "',CURRENT_TIMESTAMP(3),0)");
                } else {
                    //新的强id，进行合并，并进行挂载
                    lyCardNumberNew = lyCardNumberNew + "," + lyCardNumber;
                    values.add("('" + etlDate + "','" + lyCardNumber + "','LYC','" + tidEarly + "',CURRENT_TIMESTAMP(3),1)");
                }
            } else {
                if (first) {
                    lyCardNumberNew = lyCardNumber;
                    first = false;
                } else {
                    lyCardNumberNew = lyCardNumberNew + "," + lyCardNumber;
                }
                values.add("('" + etlDate + "','" + lyCardNumber + "','LYC','" + tidEarly + "',CURRENT_TIMESTAMP(3),1)");
            }
        }
        finalTid.setLyCardNumber(lyCardNumberNew);
    }


    /**
     * 合并CRM客户ID强ID
     *
     * @param crmCustomerIdOld
     * @param crmCustomerIdNew
     * @param tidEarly
     * @param etlDate
     * @param values
     * @param finalTid
     * @return
     */
    private static void mergeCrmCustomerId(String crmCustomerIdOld, String crmCustomerIdNew, String tidEarly, String tidOld, String etlDate, List<String> values, Tid finalTid) {
        boolean first = true;
        for (String crmCustomerId : crmCustomerIdOld.split(",")) {
            if (StringUtils.isNotBlank(crmCustomerIdNew)) {
                if (crmCustomerIdNew.contains(crmCustomerId)) {
                    //相同强id，把原来挂载的tid置为无效
                    values.add("('" + etlDate + "','" + crmCustomerId + "','CCID','" + tidOld + "',CURRENT_TIMESTAMP(3),0)");
                } else {
                    //新的强id，进行合并，并进行挂载
                    crmCustomerIdNew = crmCustomerIdNew + "," + crmCustomerId;
                    values.add("('" + etlDate + "','" + crmCustomerId + "','CCID','" + tidEarly + "',CURRENT_TIMESTAMP(3),1)");
                }
            } else {
                //新的强id,进行挂载
                if (first) {
                    crmCustomerIdNew = crmCustomerId;
                    first = false;
                } else {
                    crmCustomerIdNew = crmCustomerIdNew + "," + crmCustomerId;
                }
                values.add("('" + etlDate + "','" + crmCustomerId + "','CCID','" + tidEarly + "',CURRENT_TIMESTAMP(3),0)");
            }
        }
        finalTid.setCrmCustomerId(crmCustomerIdNew);
    }

    /**
     * 合并手机号强ID
     *
     * @param mobilePhoneOld
     * @param mobilePhoneNew
     * @param tidEarly
     * @param etlDate
     * @param values
     * @param finalTid
     * @return
     */
    public static void mergeMobilePhone(String mobilePhoneOld,
                                        String mobilePhoneNew, String tidEarly, String tidOld,
                                        String etlDate, List<String> values, Tid finalTid) {
        boolean first = true;
        for (String mobilePhone : mobilePhoneOld.split(",")) {
            if (StringUtils.isNotBlank(mobilePhoneNew)) {
                if (mobilePhoneNew.contains(mobilePhone)) {
                    //相同强id，把原来挂载的tid置为无效
                    values.add("('" + etlDate + "','" + mobilePhone + "','PHONE','" + tidOld + "',CURRENT_TIMESTAMP(3),0)");
                } else {
                    //新的强id，进行合并，并进行挂载
                    mobilePhoneNew += "," + mobilePhone;
                    values.add("('" + etlDate + "','" + mobilePhone + "','PHONE','" + tidEarly + "',CURRENT_TIMESTAMP(3),1)");
                }
            } else {
                //新的强id，进行挂载
                if (first) {
                    mobilePhoneNew = mobilePhone;
                    first = false;
                } else {
                    mobilePhoneNew += "," + mobilePhone;
                }
                values.add("('" + etlDate + "','" + mobilePhone + "','PHONE','" + tidEarly + "',CURRENT_TIMESTAMP(3),1)");
            }
        }
        finalTid.setMobilePhone(mobilePhoneNew);
    }

    /**
     * 合并常客卡号强ID
     *
     * @param frequentTravelerCardnoOld
     * @param frequentTravelerCardnoNew
     * @param tidEarly
     * @param etlDate
     * @param values
     * @param finalTid
     * @return
     */
    public static void mergeFrequentTravelerCardno(String frequentTravelerCardnoOld,
                                                   String frequentTravelerCardnoNew, String tidEarly, String tidOld,
                                                   String etlDate, List<String> values, Tid finalTid) {
        boolean first = true;
        String[] frequentTravelerCardnosOld = frequentTravelerCardnoOld.split(",");
        for (String frequentTravelerCardno : frequentTravelerCardnosOld) {
            if (StringUtils.isNotBlank(frequentTravelerCardnoNew)) {
                if (frequentTravelerCardnoNew.contains(frequentTravelerCardno)) {
                    //相同强id，把原来挂载的tid置为无效
                    values.add("('" + etlDate + "','" + frequentTravelerCardno + "','FFP','" + tidOld + "',CURRENT_TIMESTAMP(3),0)");
                } else {
                    //新的强id，进行合并，并进行挂载
                    frequentTravelerCardnoNew += "," + frequentTravelerCardno;
                    values.add("('" + etlDate + "','" + frequentTravelerCardno + "','FFP','" + tidEarly + "',CURRENT_TIMESTAMP(3),1)");
                }
            } else {
                //新的强id，进行挂载
                if (first) {
                    frequentTravelerCardnoNew = frequentTravelerCardno;
                    first = false;
                } else {
                    frequentTravelerCardnoNew += "," + frequentTravelerCardno;
                }
                values.add("('" + etlDate + "','" + frequentTravelerCardno + "','FFP','" + tidEarly + "',CURRENT_TIMESTAMP(3),1)");
            }
        }
        finalTid.setFrequentTravelerCardno(frequentTravelerCardnoNew);
    }

    /**
     * 判断两个tid中是否存在任意一个强ID(常客卡号、CUSTOMER_ID、手机号)一致
     *
     * @param tidFirst
     * @param tidSecond
     * @return 是否一致
     */
    public static boolean strongidSame(Tid tidFirst, Tid tidSecond) {
        //对比常客卡号
        String frequentTravelerCardnoFirst = tidFirst.getFrequentTravelerCardno();
        String frequentTravelerCardnoSecond = tidSecond.getFrequentTravelerCardno();
        if (StringUtils.isNotBlank(frequentTravelerCardnoFirst) && StringUtils.isNotBlank(frequentTravelerCardnoSecond)) {
            String[] frequentTravelerCardnosFirst = frequentTravelerCardnoFirst.split(",");
            for (String frequentTravelerCardno :
                    frequentTravelerCardnosFirst) {
                if (frequentTravelerCardnoSecond.contains(frequentTravelerCardno)) {
                    return true;
                }
            }
        }
        //对比CUSTOMER_ID
        String crmCustomerIdFirst = tidFirst.getCrmCustomerId();
        String crmCustomerIdSecond = tidSecond.getCrmCustomerId();
        if (StringUtils.isNotBlank(crmCustomerIdFirst) && StringUtils.isNotBlank(crmCustomerIdSecond)) {
            String[] crmCustomerIddFirst = crmCustomerIdFirst.split(",");
            for (String crmCustomerId :
                    crmCustomerIddFirst) {
                if (crmCustomerIdSecond.contains(crmCustomerId)) {
                    return true;
                }
            }
        }
        //对比手机号
        String mobilePhoneFirst = tidFirst.getMobilePhone();
        String mobilePhoneSecond = tidSecond.getMobilePhone();
        if (StringUtils.isNotBlank(mobilePhoneFirst) && StringUtils.isNotBlank(mobilePhoneSecond)) {
            String[] mobilePhonesFirst = mobilePhoneFirst.split(",");
            for (String mobilePhone : mobilePhonesFirst) {
                if (mobilePhoneSecond.contains(mobilePhone)) {
                    return true;
                }
            }
        }
        return false;
    }
}
