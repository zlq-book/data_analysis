package com.travelsky.trp.usercenter.data.analysis.model.dwd.fact;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;


/**
 * 客服-客户意见-业务级事实表实体类
 * T_DWD_CUSTOMER_FEEDBACK_FACT
 */
public class CustomerFeedbackFact {

    @JsonProperty("PK_ID")
    private String pkId; // PK_ID

    @JsonProperty("TICKET_CHANNEL")
    private String ticketChannel; // 购票渠道

    @JsonProperty("CLOSE_ADDITIONAL_CONTENT")
    private String closeAdditionalContent; // 结案意见补充内容

    @JsonProperty("CLOSE_ADDITIONAL_DATE")
    private String closeAdditionalDate; // 结案意见补充时间

    @JsonProperty("CLOSE_ADDITIONAL_OPERATOR")
    private String closeAdditionalOperator; // 结案意见补充人员

    @JsonProperty("CLOSE_ADDITIONAL_FILE")
    private String closeAdditionalFile; // 结案意见补充附件

    @JsonProperty("COMMENT_SUMMARY")
    private String commentSummary; // 意见概要

    @JsonProperty("IS_CONVERT_MEDIATION")
    private String isConvertMediation; // 是否为转调解的单子

    @JsonProperty("BEFORE_COMPLAINT_ID")
    private Long beforeComplaintId; // 转投诉调解前的投诉单子ID

    @JsonProperty("MEDIATE_FALG")
    private Long mediateFalg; // 是否和解

    @JsonProperty("NOT_MEDIATE_REASON")
    private String notMediateReason; // 未和解理由

    @JsonProperty("MEDIATION_CLAIM")
    private String mediationClaim; // 旅客调解诉求

    @JsonProperty("NEW_COMPLAINT_INFO")
    private String newComplaintInfo; // 是否为新系统数据

    @JsonProperty("ATTACH_RECOMMUNICATE")
    private String attachRecommunicate; // 结案文件

    @JsonProperty("FEEDBACK_ID")
    private Long feedbackId; // 顾客意见信息单序列ID

    @JsonProperty("COMPLAINT_SERIAL_NUM")
    private String complaintSerialNum; // 投诉编号

    @JsonProperty("COMPLAINT_TITLE")
    private String complaintTitle; // 标题

    @JsonProperty("PAX_NAME")
    private String paxName; // 乘机人姓名

    @JsonProperty("IS_GROUP")
    private String isGroup; // 是否团队成员

    @JsonProperty("SEX")
    private String sex; // 性别

    @JsonProperty("TK_LUGGAGE")
    private String tkLuggage; // 客票/行李号码

    @JsonProperty("CARD_NUM")
    private String cardNum; // 证件号码(加密)

    @JsonProperty("CARD_TYPE")
    private String cardType; // 证件类型

    @JsonProperty("PSG_TYPE")
    private String psgType; // 客户类型

    @JsonProperty("COMP_DUTY_FFP")
    private String compDutyFfp; // 公司/职务/常旅客卡号

    @JsonProperty("CABIN_CLASS")
    private String cabinClass; // 舱位

    @JsonProperty("PHONENUM")
    private String phonenum; // 联系电话（加密）

    @JsonProperty("ADDRESS")
    private String address; // 联系地址

    @JsonProperty("EMAIL")
    private String email; // 电子邮箱

    @JsonProperty("MEMO")
    private String memo; // 其他备注

    @JsonProperty("FEED_NAME")
    private String feedName; // 反馈者姓名

    @JsonProperty("FEED_PHONE")
    private String feedPhone; // 来电电话

    @JsonProperty("FLIGHT_NUM")
    private String flightNum; // 航班号

    @JsonProperty("FLIGHT_DATE")
    private String flightDate; // 航班日期

    @JsonProperty("DEP_CITY")
    private String depCity; // 出发城市

    @JsonProperty("ARR_CITY")
    private String arrCity; // 到达城市

    @JsonProperty("COMPLAINT_WAY")
    private String complaintWay; // 意见方式

    @JsonProperty("COMPLAINT_ORI")
    private String complaintOri; // 意见来源

    @JsonProperty("SUGGEST_TYPE")
    private Long suggestType; // 意见类别

    @JsonProperty("SERVICE_TYPE")
    private Long serviceType; // 服务类别

    @JsonProperty("SERVICE_TYPE_DETAIL")
    private Long serviceTypeDetail; // 服务子类别

    @JsonProperty("COMPLAINT_PRIORITY")
    private Long complaintPriority; // 意见优先级

    @JsonProperty("COMPLAINT_CONTENT")
    private String complaintContent; // 意见内容

    @JsonProperty("COMPLAINT_SUPPLEMENT_CONTENT")
    private String complaintSupplementContent; // 意见内容补充

    @JsonProperty("PSG_REQ")
    private String psgReq; // 旅客诉求及处理意见

    @JsonProperty("SERVICES_REMIND")
    private String servicesRemind; // 服务提醒

    @JsonProperty("DEAL_DEPT")
    private String dealDept; // 处理部门

    @JsonProperty("ASSIT_DEPT")
    private String assitDept; // 协查部门

    @JsonProperty("COMPLAINT_DATE")
    private String complaintDate; // 投诉录入日期时间

    @JsonProperty("COMPLAINT_TIME")
    private String complaintTime; // 投诉录入日期时间

    @JsonProperty("COMPLAINT_STATE")
    private Long complaintState; // 投诉状态

    @JsonProperty("ACCEPT_PEOPLE")
    private String acceptPeople; // 录入人员ID

    @JsonProperty("ACCEPT_DEPT")
    private String acceptDept; // 录入部门

    @JsonProperty("RECALL_TIME")
    private String recallTime; // 撤回时间

    @JsonProperty("PROCESS_ID")
    private String processId; // 流程ID

    @JsonProperty("ATTACH_COMPLAINT")
    private String attachComplaint; // 添加附件

    @JsonProperty("CLOSE_CONTENT")
    private String closeContent; // 结案意见

    @JsonProperty("CLOSE_DATE")
    private String closeDate; // 投诉结案日期时间

    @JsonProperty("CLOSE_TIME")
    private String closeTime; // 投诉结案日期时间

    @JsonProperty("CLOSE_DAYS")
    private Long closeDays; // 结案工作日

    @JsonProperty("CLOSE_OP")
    private String closeOp; // 结案人员ID

    @JsonProperty("VALID")
    private Long valid; // 是否有效

    @JsonProperty("COMPLAINT_RESULT")
    private String complaintResult; // 处理结果

    @JsonProperty("ARCHIVE_VALID")
    private String archiveValid; // 是否有效投诉

    @JsonProperty("FIRST_DUTY_DEPT")
    private String firstDutyDept; // 责任部门

    @JsonProperty("FIRST_DUTY_DEPT_SCORE")
    private String firstDutyDeptScore; // 责任部门值

    @JsonProperty("INVOLVED_PERSON_ACCT")
    private String involvedPersonAcct; // 涉及人员工号

    @JsonProperty("INVOLVED_PERSONNEL_NAME")
    private String involvedPersonnelName; // 涉及人员姓名

    @JsonProperty("UPDATE_PERSON")
    private String updatePerson; // 更新人

    @JsonProperty("UPDATE_DATE")
    private String updateDate; // 更新日期

    @JsonProperty("RESULT_ATTACH")
    private String resultAttach; // 结案存档附件

    @JsonProperty("FK_COMPLAINT_USER_TID")
    private String fkComplaintUserTid; // 投诉人TID（使用证件号码CARD_NUM字段找TID）

    @JsonProperty("COMPLAINT_COUNT")
    private Integer complaintCount = 1; // 投诉单计数

    @JsonProperty("SOURCE_LAST_UPDATETIME")
    private String sourceLastUpdatetime; // 源系统最后更新时间

    @JsonProperty("SYSTEM_CREATETIME")
    private String systemCreatetime; // 本系统创建日期时间

    @JsonProperty("SYSTEM_LAST_UPDATETIME")
    private String systemLastUpdatetime = LocalDateTime.now().toString(); // 本系统最后更新日期时间


    public String getPkId() {
        return pkId;
    }

    public void setPkId(String pkId) {
        this.pkId = pkId;
    }

    public String getTicketChannel() {
        return ticketChannel;
    }

    public void setTicketChannel(String ticketChannel) {
        this.ticketChannel = ticketChannel;
    }

    public String getCloseAdditionalContent() {
        return closeAdditionalContent;
    }

    public void setCloseAdditionalContent(String closeAdditionalContent) {
        this.closeAdditionalContent = closeAdditionalContent;
    }

    public String getCloseAdditionalDate() {
        return closeAdditionalDate;
    }

    public void setCloseAdditionalDate(String closeAdditionalDate) {
        this.closeAdditionalDate = closeAdditionalDate;
    }

    public String getCloseAdditionalOperator() {
        return closeAdditionalOperator;
    }

    public void setCloseAdditionalOperator(String closeAdditionalOperator) {
        this.closeAdditionalOperator = closeAdditionalOperator;
    }

    public String getCloseAdditionalFile() {
        return closeAdditionalFile;
    }

    public void setCloseAdditionalFile(String closeAdditionalFile) {
        this.closeAdditionalFile = closeAdditionalFile;
    }

    public String getCommentSummary() {
        return commentSummary;
    }

    public void setCommentSummary(String commentSummary) {
        this.commentSummary = commentSummary;
    }

    public String getIsConvertMediation() {
        return isConvertMediation;
    }

    public void setIsConvertMediation(String isConvertMediation) {
        this.isConvertMediation = isConvertMediation;
    }

    public Long getBeforeComplaintId() {
        return beforeComplaintId;
    }

    public void setBeforeComplaintId(Long beforeComplaintId) {
        this.beforeComplaintId = beforeComplaintId;
    }

    public Long getMediateFalg() {
        return mediateFalg;
    }

    public void setMediateFalg(Long mediateFalg) {
        this.mediateFalg = mediateFalg;
    }

    public String getNotMediateReason() {
        return notMediateReason;
    }

    public void setNotMediateReason(String notMediateReason) {
        this.notMediateReason = notMediateReason;
    }

    public String getMediationClaim() {
        return mediationClaim;
    }

    public void setMediationClaim(String mediationClaim) {
        this.mediationClaim = mediationClaim;
    }

    public String getNewComplaintInfo() {
        return newComplaintInfo;
    }

    public void setNewComplaintInfo(String newComplaintInfo) {
        this.newComplaintInfo = newComplaintInfo;
    }

    public String getAttachRecommunicate() {
        return attachRecommunicate;
    }

    public void setAttachRecommunicate(String attachRecommunicate) {
        this.attachRecommunicate = attachRecommunicate;
    }

    public Long getFeedbackId() {
        return feedbackId;
    }

    public void setFeedbackId(Long feedbackId) {
        this.feedbackId = feedbackId;
    }

    public String getComplaintSerialNum() {
        return complaintSerialNum;
    }

    public void setComplaintSerialNum(String complaintSerialNum) {
        this.complaintSerialNum = complaintSerialNum;
    }

    public String getComplaintTitle() {
        return complaintTitle;
    }

    public void setComplaintTitle(String complaintTitle) {
        this.complaintTitle = complaintTitle;
    }

    public String getPaxName() {
        return paxName;
    }

    public void setPaxName(String paxName) {
        this.paxName = paxName;
    }

    public String getIsGroup() {
        return isGroup;
    }

    public void setIsGroup(String isGroup) {
        this.isGroup = isGroup;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public String getTkLuggage() {
        return tkLuggage;
    }

    public void setTkLuggage(String tkLuggage) {
        this.tkLuggage = tkLuggage;
    }

    public String getCardNum() {
        return cardNum;
    }

    public void setCardNum(String cardNum) {
        this.cardNum = cardNum;
    }

    public String getCardType() {
        return cardType;
    }

    public void setCardType(String cardType) {
        this.cardType = cardType;
    }

    public String getPsgType() {
        return psgType;
    }

    public void setPsgType(String psgType) {
        this.psgType = psgType;
    }

    public String getCompDutyFfp() {
        return compDutyFfp;
    }

    public void setCompDutyFfp(String compDutyFfp) {
        this.compDutyFfp = compDutyFfp;
    }

    public String getCabinClass() {
        return cabinClass;
    }

    public void setCabinClass(String cabinClass) {
        this.cabinClass = cabinClass;
    }

    public String getPhonenum() {
        return phonenum;
    }

    public void setPhonenum(String phonenum) {
        this.phonenum = phonenum;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getMemo() {
        return memo;
    }

    public void setMemo(String memo) {
        this.memo = memo;
    }

    public String getFeedName() {
        return feedName;
    }

    public void setFeedName(String feedName) {
        this.feedName = feedName;
    }

    public String getFeedPhone() {
        return feedPhone;
    }

    public void setFeedPhone(String feedPhone) {
        this.feedPhone = feedPhone;
    }

    public String getFlightNum() {
        return flightNum;
    }

    public void setFlightNum(String flightNum) {
        this.flightNum = flightNum;
    }

    public String getFlightDate() {
        return flightDate;
    }

    public void setFlightDate(String flightDate) {
        this.flightDate = flightDate;
    }

    public String getDepCity() {
        return depCity;
    }

    public void setDepCity(String depCity) {
        this.depCity = depCity;
    }

    public String getArrCity() {
        return arrCity;
    }

    public void setArrCity(String arrCity) {
        this.arrCity = arrCity;
    }

    public String getComplaintWay() {
        return complaintWay;
    }

    public void setComplaintWay(String complaintWay) {
        this.complaintWay = complaintWay;
    }

    public String getComplaintOri() {
        return complaintOri;
    }

    public void setComplaintOri(String complaintOri) {
        this.complaintOri = complaintOri;
    }

    public Long getSuggestType() {
        return suggestType;
    }

    public void setSuggestType(Long suggestType) {
        this.suggestType = suggestType;
    }

    public Long getServiceType() {
        return serviceType;
    }

    public void setServiceType(Long serviceType) {
        this.serviceType = serviceType;
    }

    public Long getServiceTypeDetail() {
        return serviceTypeDetail;
    }

    public void setServiceTypeDetail(Long serviceTypeDetail) {
        this.serviceTypeDetail = serviceTypeDetail;
    }

    public Long getComplaintPriority() {
        return complaintPriority;
    }

    public void setComplaintPriority(Long complaintPriority) {
        this.complaintPriority = complaintPriority;
    }

    public String getComplaintContent() {
        return complaintContent;
    }

    public void setComplaintContent(String complaintContent) {
        this.complaintContent = complaintContent;
    }

    public String getComplaintSupplementContent() {
        return complaintSupplementContent;
    }

    public void setComplaintSupplementContent(String complaintSupplementContent) {
        this.complaintSupplementContent = complaintSupplementContent;
    }

    public String getPsgReq() {
        return psgReq;
    }

    public void setPsgReq(String psgReq) {
        this.psgReq = psgReq;
    }

    public String getServicesRemind() {
        return servicesRemind;
    }

    public void setServicesRemind(String servicesRemind) {
        this.servicesRemind = servicesRemind;
    }

    public String getDealDept() {
        return dealDept;
    }

    public void setDealDept(String dealDept) {
        this.dealDept = dealDept;
    }

    public String getAssitDept() {
        return assitDept;
    }

    public void setAssitDept(String assitDept) {
        this.assitDept = assitDept;
    }

    public String getComplaintDate() {
        return complaintDate;
    }

    public void setComplaintDate(String complaintDate) {
        this.complaintDate = complaintDate;
    }

    public String getComplaintTime() {
        return complaintTime;
    }

    public void setComplaintTime(String complaintTime) {
        this.complaintTime = complaintTime;
    }

    public Long getComplaintState() {
        return complaintState;
    }

    public void setComplaintState(Long complaintState) {
        this.complaintState = complaintState;
    }

    public String getAcceptPeople() {
        return acceptPeople;
    }

    public void setAcceptPeople(String acceptPeople) {
        this.acceptPeople = acceptPeople;
    }

    public String getAcceptDept() {
        return acceptDept;
    }

    public void setAcceptDept(String acceptDept) {
        this.acceptDept = acceptDept;
    }

    public String getRecallTime() {
        return recallTime;
    }

    public void setRecallTime(String recallTime) {
        this.recallTime = recallTime;
    }

    public String getProcessId() {
        return processId;
    }

    public void setProcessId(String processId) {
        this.processId = processId;
    }

    public String getAttachComplaint() {
        return attachComplaint;
    }

    public void setAttachComplaint(String attachComplaint) {
        this.attachComplaint = attachComplaint;
    }

    public String getCloseContent() {
        return closeContent;
    }

    public void setCloseContent(String closeContent) {
        this.closeContent = closeContent;
    }

    public String getCloseDate() {
        return closeDate;
    }

    public void setCloseDate(String closeDate) {
        this.closeDate = closeDate;
    }

    public String getCloseTime() {
        return closeTime;
    }

    public void setCloseTime(String closeTime) {
        this.closeTime = closeTime;
    }

    public Long getCloseDays() {
        return closeDays;
    }

    public void setCloseDays(Long closeDays) {
        this.closeDays = closeDays;
    }

    public String getCloseOp() {
        return closeOp;
    }

    public void setCloseOp(String closeOp) {
        this.closeOp = closeOp;
    }

    public Long getValid() {
        return valid;
    }

    public void setValid(Long valid) {
        this.valid = valid;
    }

    public String getComplaintResult() {
        return complaintResult;
    }

    public void setComplaintResult(String complaintResult) {
        this.complaintResult = complaintResult;
    }

    public String getArchiveValid() {
        return archiveValid;
    }

    public void setArchiveValid(String archiveValid) {
        this.archiveValid = archiveValid;
    }

    public String getFirstDutyDept() {
        return firstDutyDept;
    }

    public void setFirstDutyDept(String firstDutyDept) {
        this.firstDutyDept = firstDutyDept;
    }

    public String getFirstDutyDeptScore() {
        return firstDutyDeptScore;
    }

    public void setFirstDutyDeptScore(String firstDutyDeptScore) {
        this.firstDutyDeptScore = firstDutyDeptScore;
    }

    public String getInvolvedPersonAcct() {
        return involvedPersonAcct;
    }

    public void setInvolvedPersonAcct(String involvedPersonAcct) {
        this.involvedPersonAcct = involvedPersonAcct;
    }

    public String getInvolvedPersonnelName() {
        return involvedPersonnelName;
    }

    public void setInvolvedPersonnelName(String involvedPersonnelName) {
        this.involvedPersonnelName = involvedPersonnelName;
    }

    public String getUpdatePerson() {
        return updatePerson;
    }

    public void setUpdatePerson(String updatePerson) {
        this.updatePerson = updatePerson;
    }

    public String getUpdateDate() {
        return updateDate;
    }

    public void setUpdateDate(String updateDate) {
        this.updateDate = updateDate;
    }

    public String getResultAttach() {
        return resultAttach;
    }

    public void setResultAttach(String resultAttach) {
        this.resultAttach = resultAttach;
    }

    public String getFkComplaintUserTid() {
        return fkComplaintUserTid;
    }

    public void setFkComplaintUserTid(String fkComplaintUserTid) {
        this.fkComplaintUserTid = fkComplaintUserTid;
    }

    public Integer getComplaintCount() {
        return complaintCount;
    }

    public void setComplaintCount(Integer complaintCount) {
        this.complaintCount = complaintCount;
    }

    public String getSourceLastUpdatetime() {
        return sourceLastUpdatetime;
    }

    public void setSourceLastUpdatetime(String sourceLastUpdatetime) {
        this.sourceLastUpdatetime = sourceLastUpdatetime;
    }

    public String getSystemCreatetime() {
        return systemCreatetime;
    }

    public void setSystemCreatetime(String systemCreatetime) {
        this.systemCreatetime = systemCreatetime;
    }

    public String getSystemLastUpdatetime() {
        return systemLastUpdatetime;
    }

    public void setSystemLastUpdatetime(String systemLastUpdatetime) {
        this.systemLastUpdatetime = systemLastUpdatetime;
    }


    @Override
    public String toString() {
        return "CustomerFeedbackFact{" +
                "pkId='" + pkId + '\'' +
                ", ticketChannel='" + ticketChannel + '\'' +
                ", closeAdditionalContent='" + closeAdditionalContent + '\'' +
                ", closeAdditionalDate='" + closeAdditionalDate + '\'' +
                ", closeAdditionalOperator='" + closeAdditionalOperator + '\'' +
                ", closeAdditionalFile='" + closeAdditionalFile + '\'' +
                ", commentSummary='" + commentSummary + '\'' +
                ", isConvertMediation='" + isConvertMediation + '\'' +
                ", beforeComplaintId=" + beforeComplaintId +
                ", mediateFalg=" + mediateFalg +
                ", notMediateReason='" + notMediateReason + '\'' +
                ", mediationClaim='" + mediationClaim + '\'' +
                ", newComplaintInfo='" + newComplaintInfo + '\'' +
                ", attachRecommunicate='" + attachRecommunicate + '\'' +
                ", feedbackId=" + feedbackId +
                ", complaintSerialNum='" + complaintSerialNum + '\'' +
                ", complaintTitle='" + complaintTitle + '\'' +
                ", paxName='" + paxName + '\'' +
                ", isGroup='" + isGroup + '\'' +
                ", sex='" + sex + '\'' +
                ", tkLuggage='" + tkLuggage + '\'' +
                ", cardNum='" + cardNum + '\'' +
                ", cardType='" + cardType + '\'' +
                ", psgType='" + psgType + '\'' +
                ", compDutyFfp='" + compDutyFfp + '\'' +
                ", cabinClass='" + cabinClass + '\'' +
                ", phonenum='" + phonenum + '\'' +
                ", address='" + address + '\'' +
                ", email='" + email + '\'' +
                ", memo='" + memo + '\'' +
                ", feedName='" + feedName + '\'' +
                ", feedPhone='" + feedPhone + '\'' +
                ", flightNum='" + flightNum + '\'' +
                ", flightDate='" + flightDate + '\'' +
                ", depCity='" + depCity + '\'' +
                ", arrCity='" + arrCity + '\'' +
                ", complaintWay='" + complaintWay + '\'' +
                ", complaintOri='" + complaintOri + '\'' +
                ", suggestType=" + suggestType +
                ", serviceType=" + serviceType +
                ", serviceTypeDetail=" + serviceTypeDetail +
                ", complaintPriority=" + complaintPriority +
                ", complaintContent='" + complaintContent + '\'' +
                ", complaintSupplementContent='" + complaintSupplementContent + '\'' +
                ", psgReq='" + psgReq + '\'' +
                ", servicesRemind='" + servicesRemind + '\'' +
                ", dealDept='" + dealDept + '\'' +
                ", assitDept='" + assitDept + '\'' +
                ", complaintDate='" + complaintDate + '\'' +
                ", complaintTime='" + complaintTime + '\'' +
                ", complaintState=" + complaintState +
                ", acceptPeople='" + acceptPeople + '\'' +
                ", acceptDept='" + acceptDept + '\'' +
                ", recallTime='" + recallTime + '\'' +
                ", processId='" + processId + '\'' +
                ", attachComplaint='" + attachComplaint + '\'' +
                ", closeContent='" + closeContent + '\'' +
                ", closeDate='" + closeDate + '\'' +
                ", closeTime='" + closeTime + '\'' +
                ", closeDays=" + closeDays +
                ", closeOp='" + closeOp + '\'' +
                ", valid=" + valid +
                ", complaintResult='" + complaintResult + '\'' +
                ", archiveValid='" + archiveValid + '\'' +
                ", firstDutyDept='" + firstDutyDept + '\'' +
                ", firstDutyDeptScore='" + firstDutyDeptScore + '\'' +
                ", involvedPersonAcct='" + involvedPersonAcct + '\'' +
                ", involvedPersonnelName='" + involvedPersonnelName + '\'' +
                ", updatePerson='" + updatePerson + '\'' +
                ", updateDate='" + updateDate + '\'' +
                ", resultAttach='" + resultAttach + '\'' +
                ", fkComplaintUserTid='" + fkComplaintUserTid + '\'' +
                ", complaintCount=" + complaintCount +
                ", sourceLastUpdatetime='" + sourceLastUpdatetime + '\'' +
                ", systemCreatetime='" + systemCreatetime + '\'' +
                ", systemLastUpdatetime='" + systemLastUpdatetime + '\'' +
                '}';
    }
}
