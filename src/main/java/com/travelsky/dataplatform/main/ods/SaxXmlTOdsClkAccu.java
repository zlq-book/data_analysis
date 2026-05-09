package com.travelsky.dataplatform.main.ods;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.module.ods.TOdsClkAccu;
import com.travelsky.dataplatform.utils.SM4Utils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SaxXmlTOdsClkAccu extends DefaultHandler {
    static final Logger logger = LoggerFactory.getLogger(SaxXmlTOdsClkAccu.class);
    private static Pattern pattern = Pattern.compile("([0-9]{1,20})\\.?[0-9]{0,20}");
    private final List<TOdsClkAccu> dtoList = new ArrayList<>();
    private final Stack<String> elementStack = new Stack<>();
    private TOdsClkAccu currentDto;
    private TOdsClkAccu tempDto;
    private String etlDate;
    // 用于回调，将解析好的对象传递出去
    private RecordCallback<TOdsClkAccu> callback;

    // 使用 StringBuilder 缓冲
    private StringBuilder textBuffer = new StringBuilder();

    public SaxXmlTOdsClkAccu(String etlDate, RecordCallback<TOdsClkAccu> callback) {
        this.callback = callback;
        this.etlDate = etlDate;
    }

    // 回调接口
    @FunctionalInterface
    public interface RecordCallback<T> {
        void onRecord(T record);
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) {
        elementStack.push(qName);
        textBuffer.setLength(0);
        if ("acc:MessageDoc".equalsIgnoreCase(qName)) {
            currentDto = new TOdsClkAccu();
        }

        if ("acc:AccuTxn".equalsIgnoreCase(qName)) {
            currentDto = new TOdsClkAccu(tempDto);
            if (StringUtils.isNotBlank(etlDate)) {
                currentDto.setETLDATE(etlDate);
            }
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) {
        String currentElement = elementStack.peek();
        String text = textBuffer.toString().trim();

        if (currentDto != null) {
            switch (currentElement) {
                case "acc:DocName":
                    currentDto.setDOCNAME(text);
                    break;
                case "acc:TotalRecord":
                    if (!"".equals(text)) {
                        currentDto.setTOTALRECORD(Long.parseLong(text));
                    }
                    break;
                case "acc:TotalAmount":
                    if (!"".equals(text)) {
                        currentDto.setTOTALAMOUNT(Long.parseLong(text));
                    }
                    break;
                case "acc:TotalValue":
                    if (!"".equals(text)) {
                        currentDto.setTOTALVALUE(BigDecimal.valueOf(Double.parseDouble(text)));
                    }
                    break;
                case "acc:BillingMonth":
                    currentDto.setBILLINGMONTH(text);
                    break;
                case "acc:BillingStartDT":
                    currentDto.setBILLINGSTARTDT(text);
                    break;
                case "acc:BillingEndDT":
                    currentDto.setBILLINGENDDT(text);
                    break;
                case "acc:MemberNo":
                    currentDto.setMEMBERNO(SM4Utils.encrypt(text, Constants.SM4_KEY));
                    break;
                case "acc:MemberBrand":
                    currentDto.setMEMBERBRAND(text);
                    break;
                case "acc:MemberTierCode":
                    currentDto.setMEMBERTIERCODE(text);
                    break;
                case "acc:BizTypeCode":
                    currentDto.setBIZTYPECODE(text);
                    break;
                case "acc:BizSubTypeCode":
                    currentDto.setBIZSUBTYPECODE(text);
                    break;
                case "acc:ChannelCode":
                    currentDto.setCHANNELCODE(text);
                    break;
                case "acc:PartnerCode":
                    currentDto.setPARTNERCODE(text);
                    break;
                case "acc:EventNo":
                    currentDto.setEVENTNO(text);
                    break;
                case "acc:ActivityId":
                    currentDto.setACTIVITYID(text);
                    break;
                case "acc:TranId":
                    currentDto.setTRANID(text);
                    break;
                case "acc:OrderNo":
                    currentDto.setORDERNO(text);
                    break;
                case "acc:TktNo":
                    currentDto.setTKTNO(text);
                    break;
                case "acc:CouponNo":
                    currentDto.setCOUPONNO(text);
                    break;
                case "acc:BillCompany":
                    currentDto.setBILLCOMPANY(text);
                    break;
                case "acc:BilledCompany":
                    currentDto.setBILLEDCOMPANY(text);
                    break;
                case "acc:Miles":
                    if (!"".equals(text)) {
                        Matcher matcher = pattern.matcher(text);
                        if (matcher.find()) {
                            currentDto.setMILES(Long.parseLong(matcher.group(1)));
                        }
                    }
                    break;
                case "acc:Currency":
                    currentDto.setCURRENCY(text);
                    break;
                case "acc:SalesPrice":
                    if (!"".equals(text)) {
                        currentDto.setSALESPRICE(BigDecimal.valueOf(Double.parseDouble(text)));                    }
                    break;
                case "acc:CostsPrice":
                    if (!"".equals(text)) {
                        currentDto.setCOSTSPRICE(BigDecimal.valueOf(Double.parseDouble(text)));
                    }
                    break;
                case "acc:Value":
                    if (!"".equals(text)) {
                        currentDto.setVALUE(BigDecimal.valueOf(Double.parseDouble(text)));
                    }
                    break;
                case "acc:ExchPoint":
                    if (!"".equals(text)) {
                        currentDto.setEXCHPOINT(BigDecimal.valueOf(Double.parseDouble(text)));
                    }
                    break;
                case "acc:ActivityDate":
                    currentDto.setACTIVITYDATE(text);
                    break;
                case "acc:FlightDate":
                    currentDto.setFLIGHTDATE(text);
                    break;
                case "acc:Oc":
                    currentDto.setOC(text);
                    break;
                case "acc:OcFlightNo":
                    currentDto.setOCFLIGHTNO(text);
                    break;
                case "acc:OcCabin":
                    currentDto.setOCCABIN(text);
                    break;
                case "acc:OcSubClass":
                    currentDto.setOCSUBCLASS(text);
                    break;
                case "acc:UplStn":
                    currentDto.setUPLSTN(text);
                    break;
                case "acc:DesStn":
                    currentDto.setDESSTN(text);
                    break;
                case "acc:ASS":
                    currentDto.setASS(text);
                    break;
                case "acc:IRN":
                    currentDto.setIRN(text);
                    break;
                case "acc:OAN":
                    currentDto.setOAN(text);
                    break;
            }
        }
        elementStack.pop();

        if ("acc:MessageDoc".equalsIgnoreCase(qName)) {
            tempDto = currentDto;
            currentDto = null;
        }

        if ("acc:AccuTxn".equalsIgnoreCase(qName)) {
            if (callback != null) {
                callback.onRecord(currentDto);
            }
            currentDto = null;
        }

    }

    public List<TOdsClkAccu> getDtoList() {
        return dtoList;
    }

    @Override
    public void characters(char[] ch, int start, int length) {
        if (length > 0) {
            textBuffer.append(ch, start, length); // ✅ 拼接所有片段
        }
        if (textBuffer.length() == 0) return;


    }
}

