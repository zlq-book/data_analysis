package com.travelsky.trp.usercenter.data.analysis.transform.lygj;


import com.travelsky.dataplatform.utils.*;
import com.travelsky.trp.usercenter.data.analysis.model.dim.Tid;
import com.travelsky.trp.usercenter.data.analysis.model.dwd.fact.CustomerFeedbackFact;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 写入客服-客户意见-业务级事实表
 */
public class LygjCIAToDwdCustomerFeedbackFacts {
    private static final Logger logger = LoggerFactory.getLogger(LygjCIAToDwdCustomerFeedbackFacts.class);

    public static void result(DataStream<Row> rowStream) {
        // 4. 将Map数据流转换为CustomerFeedbackFact流
        SingleOutputStreamOperator<CustomerFeedbackFact> customerFeedbackFactStream = rowStream
                .map(row -> {
                    CustomerFeedbackFact model = new CustomerFeedbackFact();
                    // 主键ID = 投诉编号
                    Object idObj = row.getFieldAs("ID");
                    String pkId = idObj != null ? idObj.toString() : null;

                    if (pkId == null) {
                        logger.error("提取LygjCIAToDwdCustomerFeedbackFacts失败，{}", row.toString());
                        return null;
                    } else {
                        // 设置主键
                        model.setPkId(pkId);

                        // 购票渠道
                        String ticketChannel = row.getFieldAs("TICKET_CHANNEL");
                        model.setTicketChannel(ticketChannel);

                        // 结案意见补充内容
                        String closeAdditionalContent = row.getFieldAs("CLOSE_ADDITIONAL_CONTENT");
                        model.setCloseAdditionalContent(closeAdditionalContent);

                        // 结案意见补充时间
                        String closeAdditionalDate = null != row.getFieldAs("CLOSE_ADDITIONAL_DATE") ?
                                row.getFieldAs("CLOSE_ADDITIONAL_DATE").toString() : null;
                        model.setCloseAdditionalDate(closeAdditionalDate);

                        // 结案意见补充人员
                        String closeAdditionalOperator = row.getFieldAs("CLOSE_ADDITIONAL_OPERATOR");
                        model.setCloseAdditionalOperator(closeAdditionalOperator);

                        // 结案意见补充附件
                        String closeAdditionalFile = row.getFieldAs("CLOSE_ADDITIONAL_FILE");
                        model.setCloseAdditionalFile(closeAdditionalFile);

                        // 意见概要
                        String commentSummary = row.getFieldAs("COMMENT_SUMMARY");
                        model.setCommentSummary(commentSummary);

                        // 是否为转调解的单子
                        String isConvertMediation = row.getFieldAs("IS_CONVERT_MEDIATION");
                        model.setIsConvertMediation(isConvertMediation);

                        // 转投诉调解前的投诉单子ID
                        Long beforeComplaintId = row.getFieldAs("BEFORE_COMPLAINT_ID");
                        model.setBeforeComplaintId(beforeComplaintId);

                        // 是否和解
                        Long mediateFalg = row.getFieldAs("MEDIATE_FALG");
                        model.setMediateFalg(mediateFalg);

                        // 未和解理由
                        String notMediateReason = row.getFieldAs("NOT_MEDIATE_REASON");
                        model.setNotMediateReason(notMediateReason);

                        // 旅客调解诉求
                        String mediationClaim = row.getFieldAs("MEDIATION_CLAIM");
                        model.setMediationClaim(mediationClaim);

                        // 是否为新系统数据
                        String newComplaintInfo = row.getFieldAs("NEW_COMPLAINT_INFO");
                        model.setNewComplaintInfo(newComplaintInfo);

                        // 结案文件
                        String attachRecommunicate = row.getFieldAs("ATTACH_RECOMMUNICATE");
                        model.setAttachRecommunicate(attachRecommunicate);

                        // 顾客意见信息单序列ID
                        Long feedbackId = row.getFieldAs("ID");
                        model.setFeedbackId(feedbackId);

                        // 投诉编号
                        String complaintSerialNum = row.getFieldAs("COMPLAINT_SERIAL_NUM");
                        model.setComplaintSerialNum(complaintSerialNum);

                        // 标题
                        String complaintTitle = row.getFieldAs("COMPLAINT_TITLE");
                        model.setComplaintTitle(complaintTitle);

                        // 乘机人姓名
                        String paxName = row.getFieldAs("PAX_NAME");
                        model.setPaxName(NormalizationUtils.standardize(FieldType.CN_NAME, paxName));

                        // 是否团队成员
                        String isGroup = row.getFieldAs("IS_GROUP");
                        model.setIsGroup(isGroup);

                        // 性别
                        String sex = row.getFieldAs("SEX");
                        model.setSex(NormalizationUtils.standardize(FieldType.GENDER, sex));

                        // 客票/行李号码
                        String tkLuggage = row.getFieldAs("TK_LUGGAGE");
                        model.setTkLuggage(tkLuggage);

                        // 证件号码(加密)
                        String cardNum = row.getFieldAs("CARD_NUM");
                        model.setCardNum(NormalizationUtils.standardize(FieldType.DOCUMENT_NUM, cardNum));

                        // 证件类型
                        String cardType = row.getFieldAs("CARD_TYPE");
                        // 证件类型标准化
                        cardType = NormalizationUtils.standardize(FieldType.DOCUMENT_TYPE, cardType, DataSource.LUYAN_STEWARD);
                        model.setCardType(cardType);

                        // 客户类型
                        String psgType = row.getFieldAs("PSG_TYPE");
                        model.setPsgType(psgType);

                        // 公司/职务/常旅客卡号
                        String compDutyFfp = row.getFieldAs("COMP_DUTY_FFP");
                        model.setCompDutyFfp(compDutyFfp);

                        // 舱位
                        String cabinClass = row.getFieldAs("CABIN_CLASS");
                        model.setCabinClass(cabinClass);

                        // 联系电话（加密）
                        String phonenum = row.getFieldAs("PHONENUM");
                        model.setPhonenum(phonenum);

                        // 联系地址
                        String address = row.getFieldAs("ADDRESS");
                        model.setAddress(address);

                        // 电子邮箱
                        String email = row.getFieldAs("EMAIL");
                        model.setEmail(email);

                        // 其他备注
                        String memo = row.getFieldAs("MEMO");
                        model.setMemo(memo);

                        // 反馈者姓名
                        String feedName = row.getFieldAs("FEED_NAME");
                        model.setFeedName(NormalizationUtils.standardize(FieldType.CN_NAME, feedName));

                        // 来电电话
                        String feedPhone = row.getFieldAs("FEED_PHONE");
                        model.setFeedPhone(feedPhone);

                        // 航班号
                        String flightNum = row.getFieldAs("FLIGHT_NUM");
                        model.setFlightNum(flightNum);

                        // 航班日期
                        String flightDate = null != row.getFieldAs("FLIGHT_DATE") ?
                                row.getFieldAs("FLIGHT_DATE").toString() : null;
                        model.setFlightDate(flightDate);

                        // 出发城市
                        String depCity = row.getFieldAs("DEP_CITY");
                        model.setDepCity(depCity);

                        // 到达城市
                        String arrCity = row.getFieldAs("ARR_CITY");
                        model.setArrCity(arrCity);

                        // 意见方式
                        String complaintWay = row.getFieldAs("COMPLAINT_WAY");
                        model.setComplaintWay(complaintWay);

                        // 意见来源
                        String complaintOri = row.getFieldAs("COMPLAINT_ORI");
                        model.setComplaintOri(complaintOri);

                        // 意见类别
                        Long suggestType = row.getFieldAs("SUGGEST_TYPE");
                        model.setSuggestType(suggestType);

                        // 服务类别
                        Long serviceType = row.getFieldAs("SERVICE_TYPE");
                        model.setServiceType(serviceType);

                        // 服务子类别
                        Long serviceTypeDetail = row.getFieldAs("SERVICE_TYPE_DETAIL");
                        model.setServiceTypeDetail(serviceTypeDetail);

                        // 意见优先级
                        Long complaintPriority = row.getFieldAs("COMPLAINT_PRIORITY");
                        model.setComplaintPriority(complaintPriority);

                        // 意见内容
                        String complaintContent = row.getFieldAs("COMPLAINT_CONTENT");
                        model.setComplaintContent(complaintContent);

                        // 意见内容补充
                        String complaintSupplementContent = row.getFieldAs("COMPLAINT_SUPPLEMENT_CONTENT");
                        model.setComplaintSupplementContent(complaintSupplementContent);

                        // 旅客诉求及处理意见
                        String psgReq = row.getFieldAs("PSG_REQ");
                        model.setPsgReq(psgReq);

                        // 服务提醒
                        String servicesRemind = row.getFieldAs("SERVICES_REMIND");
                        model.setServicesRemind(servicesRemind);

                        // 处理部门
                        String dealDept = row.getFieldAs("DEAL_DEPT");
                        model.setDealDept(dealDept);

                        // 协查部门
                        String assitDept = row.getFieldAs("ASSIT_DEPT");
                        model.setAssitDept(assitDept);

                        // 投诉录入日期时间
                        String complaintDateStr = null != row.getFieldAs("COMPLAINT_DATE") ?
                                row.getFieldAs("COMPLAINT_DATE").toString() : null;
                        if (StringUtils.isNotBlank(complaintDateStr)) { // 判断字符串是否非空
                            String complaintDate = null;
                            String complaintTime = null;

                            if (complaintDateStr.contains("T")) {
                                // 处理 ISO 格式: 2016-07-30T11:20:23 或 2016-07-30T00:00
                                String[] parts = complaintDateStr.split("T");
                                complaintDate = parts[0];
                                complaintTime = parts.length > 1 ? parts[1] : "00:00:00";
                                // 确保时间格式为 HH:mm:ss
                                if (complaintTime.length() == 5) { // 只有 HH:mm 的情况
                                    complaintTime += ":00";
                                }
                            } else if (complaintDateStr.contains(" ")) {
                                // 处理空格分隔格式: 2016-07-30 11:20:23 或 2016-07-30 00:00
                                String[] parts = complaintDateStr.split(" ");
                                complaintDate = parts[0];
                                complaintTime = parts.length > 1 ? parts[1] : "00:00:00";
                                // 确保时间格式为 HH:mm:ss
                                if (complaintTime.length() == 5) { // 只有 HH:mm 的情况
                                    complaintTime += ":00";
                                }
                            } else {
                                // 只有日期部分
                                complaintDate = complaintDateStr;
                                complaintTime = "00:00:00";
                            }

                            if (complaintDate != null) {
                                model.setComplaintDate(complaintDate);
                                model.setComplaintTime(complaintTime);
                            }
                        }


                        // 投诉状态
                        Long complaintState = row.getFieldAs("COMPLAINT_STATE");
                        model.setComplaintState(complaintState);

                        // 录入人员ID
                        String acceptPeople = row.getFieldAs("ACCEPT_PEOPLE");
                        model.setAcceptPeople(acceptPeople);

                        // 录入部门
                        String acceptDept = row.getFieldAs("ACCEPT_DEPT");
                        model.setAcceptDept(acceptDept);

                        // 撤回时间
                        String recallTime = null != row.getFieldAs("RECALL_TIME") ?
                                row.getFieldAs("RECALL_TIME").toString() : null;
                        model.setRecallTime(recallTime);

                        // 流程ID
                        String processId = row.getFieldAs("PROCESS_ID");
                        model.setProcessId(processId);

                        // 添加附件
                        String attachComplaint = row.getFieldAs("ATTACH_COMPLAINT");
                        model.setAttachComplaint(attachComplaint);

                        // 结案意见
                        String closeContent = row.getFieldAs("CLOSE_CONTENT");
                        model.setCloseContent(closeContent);

                        // 投诉结案TIMESTAMP(6)时间
                        String closeDateStr = null != row.getFieldAs("CLOSE_DATE") ?
                                row.getFieldAs("CLOSE_DATE").toString() : null;
                        if (StringUtils.isNotBlank(closeDateStr)) { // 判断字符串是否非空
                            String closeDate = null;
                            String closeTime = null;

                            if (closeDateStr.contains("T")) {
                                // 处理 ISO 格式: 2016-07-30T11:20:23 或 2016-07-30T00:00
                                String[] parts = closeDateStr.split("T");
                                closeDate = parts[0];
                                closeTime = parts.length > 1 ? parts[1] : "00:00:00";
                                // 确保时间格式为 HH:mm:ss
                                if (closeTime.length() == 5) { // 只有 HH:mm 的情况
                                    closeTime += ":00";
                                }
                            } else if (closeDateStr.contains(" ")) {
                                // 处理空格分隔格式: 2016-07-30 11:20:23 或 2016-07-30 00:00
                                String[] parts = closeDateStr.split(" ");
                                closeDate = parts[0];
                                closeTime = parts.length > 1 ? parts[1] : "00:00:00";
                                // 确保时间格式为 HH:mm:ss
                                if (closeTime.length() == 5) { // 只有 HH:mm 的情况
                                    closeTime += ":00";
                                }
                            } else {
                                // 只有日期部分
                                closeDate = closeDateStr;
                                closeTime = "00:00:00";
                            }

                            if (closeDate != null) {
                                model.setCloseDate(closeDate);
                                model.setCloseTime(closeTime);
                            }
                        }


                        // 结案工作日
                        Long closeDays = row.getFieldAs("CLOSE_DAYS");
                        model.setCloseDays(closeDays);

                        // 结案人员ID
                        String closeOp = row.getFieldAs("CLOSE_OP");
                        model.setCloseOp(closeOp);

                        // 是否有效
                        Long valid = row.getFieldAs("VALID");
                        model.setValid(valid);

                        // 处理结果
                        String complaintResult = row.getFieldAs("COMPLAINT_RESULT");
                        model.setComplaintResult(complaintResult);

                        // 是否有效投诉
                        String archiveValid = row.getFieldAs("ARCHIVE_VALID");
                        model.setArchiveValid(archiveValid);

                        // 责任部门
                        String firstDutyDept = row.getFieldAs("FIRST_DUTY_DEPT");
                        model.setFirstDutyDept(firstDutyDept);

                        // 责任部门值
                        String firstDutyDeptScore = row.getFieldAs("FIRST_DUTY_DEPT_SCORE");
                        model.setFirstDutyDeptScore(firstDutyDeptScore);

                        // 涉及人员工号
                        String involvedPersonAcct = row.getFieldAs("INVOLVED_PERSON_ACCT");
                        model.setInvolvedPersonAcct(involvedPersonAcct);

                        // 涉及人员姓名
                        String involvedPersonnelName = row.getFieldAs("INVOLVED_PERSONNEL_NAME");
                        model.setInvolvedPersonnelName(involvedPersonnelName);

                        // 更新人
                        String updatePerson = row.getFieldAs("UPDATE_PERSON");
                        model.setUpdatePerson(updatePerson);

                        // 更新日期
                        String updateDate = null != row.getFieldAs("UPDATE_DATE") ?
                                row.getFieldAs("UPDATE_DATE").toString() : null;
                        model.setUpdateDate(updateDate);

                        // 结案存档附件
                        String resultAttach = row.getFieldAs("RESULT_ATTACH");
                        model.setResultAttach(resultAttach);

                        // 投诉人TID（使用证件号码CARD_NUM字段找TID） todo
                        // 生成TID
                        Tid tid = new Tid();
                        tid.setTid(cardNum);
                        Map<String, String> certification = new HashMap<>();
                        certification.put(cardType, cardNum);
                        tid.setCertification(certification);
                        String tidStr = IdMapping.idMappingFunction(tid, "LYGJ");
                        if (StringUtils.isBlank(tidStr)) {
                            return null;
                        }
                        model.setFkComplaintUserTid(tidStr);

                        // 投诉单计数
                        //model.setComplaintCount(1);

                        // 源系统最后更新时间
                        String sourceLastUpdatetime = row.getFieldAs("UPDATE_DATE") != null ?
                                row.getFieldAs("UPDATE_DATE").toString() : null;
                        model.setSourceLastUpdatetime(sourceLastUpdatetime);

                        // 系统信息
                        String now = LocalDateTime.now().toString();
                        model.setSystemCreatetime(now);
                        model.setSystemLastUpdatetime(now);

                        return model;
                    }
                })
                .filter(Objects::nonNull);

        // 5. 创建Doris Sink并写入数据
        DorisSink<CustomerFeedbackFact> dorisSink = FlinkDorisUtils.creatDorisDWDSink("T_DWD_CUSTOMER_FEEDBACK_FACT");

        customerFeedbackFactStream.sinkTo(dorisSink);
    }
}
