package com.travelsky.dataplatform.main.ods;

import com.travelsky.dataplatform.constans.Constants;
import com.travelsky.dataplatform.module.ods.TOdsClkExchint;
import com.travelsky.dataplatform.utils.SM4Utils;
import org.apache.commons.lang3.StringUtils;
import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SaxXmlTOdsClkExchint extends DefaultHandler {
    private static Pattern pattern = Pattern.compile("([0-9]{1,20})\\.?[0-9]{0,20}");
    private final List<TOdsClkExchint> dtoList = new ArrayList<>();
    private final Stack<String> elementStack = new Stack<>();
    private TOdsClkExchint currentDto;
    private TOdsClkExchint tempDto;
    private String etlDate;

    // 使用 StringBuilder 缓冲
    private StringBuilder textBuffer = new StringBuilder();

    public SaxXmlTOdsClkExchint(String etlDate) {
        this.etlDate = etlDate;
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) {
        elementStack.push(qName);
        textBuffer.setLength(0);
        if ("MessageDoc".equalsIgnoreCase(qName)) {
            currentDto = new TOdsClkExchint();
        }

        if ("EXCHTxn".equalsIgnoreCase(qName)) {
            currentDto = new TOdsClkExchint(tempDto);
            if (StringUtils.isNotBlank(etlDate)) {
                currentDto.setEtlDate(etlDate);
            }
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) {
        String currentElement = elementStack.peek();
        String text = textBuffer.toString().trim();
        if (currentDto != null) {
            switch (currentElement) {
                case "DocName":
                    currentDto.setDOCNAME(text);
                    break;
                case "TotalRecord":
                    if (!"".equals(text)) {
                        currentDto.setTOTALRECORD(Long.parseLong(text));
                    }
                    break;
                case "TotalAmount":
                    if (!"".equals(text)) {
                        currentDto.setTOTALAMOUNT(Long.parseLong(text));
                    }
                    break;
                case "TotalValue":
                    if (!"".equals(text)) {
                        currentDto.setTOTALVALUE(BigDecimal.valueOf(Double.parseDouble(text)));
                    }
                    break;
                case "BillingMonth":
                    currentDto.setBILLINGMONTH(text);
                    break;
                case "BillingStartDT":
                    currentDto.setBILLINGSTARTDT(text);
                    break;
                case "BillingEndDT":
                    currentDto.setBILLINGENDDT(text);
                    break;
                case "MemberNo":
                    currentDto.setMEMBERNO(SM4Utils.encrypt(text, Constants.SM4_KEY));
                    break;
                case "MemberBrand":
                    currentDto.setMEMBERBRAND(text);
                    break;
                case "MemberTierCode":
                    currentDto.setMEMBERTIERCODE(text);
                    break;
                case "BizTypeCode":
                    currentDto.setBIZTYPECODE(text);
                    break;
                case "BizSubTypeCode":
                    currentDto.setBIZSUBTYPECODE(text);
                    break;
                case "ExchChannel":
                    currentDto.setEXCHCHANNEL(text);
                    break;
                case "PartnerCode":
                    currentDto.setPARTNERCODE(text);
                    break;
                case "ExchNo":
                    currentDto.setEXCHNO(text);
                    break;
                case "ActivityID":
                    currentDto.setACTIVITYID(text);
                    break;
                case "TranId":
                    currentDto.setTRANID(text);
                    break;
                case "OrderNo":
                    currentDto.setORDERNO(text);
                    break;
                case "TktNo":
                    currentDto.setTKTNO(text);
                    break;
                case "CouponNo":
                    currentDto.setCOUPONNO(text);
                    break;
                case "BillCompany":
                    currentDto.setBILLCOMPANY(text);
                    break;
                case "BilledCompany":
                    currentDto.setBILLEDCOMPANY(text);
                    break;
                case "Miles":
                    if (!"".equals(text)) {
                        Matcher matcher = pattern.matcher(text);
                        if (matcher.find()) {
                            currentDto.setMILES(Long.parseLong(matcher.group(1)));
                        }
                    }
                    break;
                case "TPM":
                    currentDto.setTPM(text);
                    break;
                case "Currency":
                    currentDto.setCURRENCY(text);
                    break;
                case "SalesPrice":
                    if (!"".equals(text)) {
                        currentDto.setSALESPRICE(BigDecimal.valueOf(Double.parseDouble(text)));                    }
                    break;
                case "CostsPrice":
                    if (!"".equals(text)) {
                        currentDto.setCOSTSPRICE(BigDecimal.valueOf(Double.parseDouble(text)));
                    }
                    break;
                case "Value":
                    if (!"".equals(text)) {
                        currentDto.setVALUE(BigDecimal.valueOf(Double.parseDouble(text)));
                    }
                    break;
                case "PNR":
                    currentDto.setPNR(text);
                    break;
                case "ExhDate":
                    currentDto.setEXCHDATE(text);
                    break;
                case "FlightDate":
                    currentDto.setFLIGHTDATE(text);
                    break;
                case "Oc":
                    currentDto.setOC(text);
                    break;
                case "OcFlightNo":
                    currentDto.setOCFLIGHTNO(text);
                    break;
                case "OcCabin":
                    currentDto.setOCCABIN(text);
                    break;
                case "OcSubClass":
                    currentDto.setOCSUBCLASS(text);
                    break;
                case "BuSubClass":
                    currentDto.setBUSUBCLASS(text);
                    break;
                case "UplStn":
                    currentDto.setUPLSTN(text);
                    break;
                case "DesStn":
                    currentDto.setDESSTN(text);
                    break;
                case "ASS":
                    currentDto.setASS(text);
                    break;
                case "CommodityNo":
                    currentDto.setCOMMODITYNO(text);
                    break;
                case "ProdcuInfo":
                    currentDto.setPRODCUINFO(text);
                    break;
            }
        }
        elementStack.pop();

        if ("MessageDoc".equalsIgnoreCase(qName)) {
            tempDto = currentDto;
            currentDto = null;
        }

        if ("EXCHTxn".equalsIgnoreCase(qName)) {
            dtoList.add(currentDto);
            currentDto = null;
        }

    }

    public List<TOdsClkExchint> getDtoList() {
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

