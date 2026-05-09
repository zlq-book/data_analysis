package com.travelsky.trp.usercenter.data.analysis.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 卡号号段匹配工具类
 * 根据卡号后九位匹配对应的渠道信息
 */
public class CardNumberSegmentMatcher {

    private static final Logger logger = LoggerFactory.getLogger(CardNumberSegmentMatcher.class);

    private static final CardNumberSegmentMatcher INSTANCE = new CardNumberSegmentMatcher();

    private final TreeMap<Long, SegmentRange> segmentMap = new TreeMap<>();

    private CardNumberSegmentMatcher() {
        initSegmentData();
    }

    public static CardNumberSegmentMatcher getInstance() {
        return INSTANCE;
    }

    /**
     * 根据卡号匹配渠道信息
     * 只匹配后九位数字，忽略前三位
     *
     * @param cardNumber 完整卡号（12位）
     * @return 渠道信息，如果未匹配到则返回null
     */
    public ChannelInfo match(String cardNumber) {
        if (cardNumber == null || cardNumber.length() < 9) {
            return null;
        }

        String lastNineDigits = cardNumber.substring(Math.max(0, cardNumber.length() - 9));
        try {
            long number = Long.parseLong(lastNineDigits);
            return matchByNumber(number);
        } catch (NumberFormatException e) {
            logger.warn("卡号后九位不是数字: {}", lastNineDigits);
            return null;
        }
    }

    /**
     * 根据后九位数字匹配渠道信息
     *
     * @param lastNineDigits 后九位数字
     * @return 渠道信息，如果未匹配到则返回null
     */
    public ChannelInfo matchByNumber(long lastNineDigits) {
        java.util.Map.Entry<Long, SegmentRange> entry = segmentMap.floorEntry(lastNineDigits);
        if (entry != null) {
            SegmentRange range = entry.getValue();
            if (range != null && range.contains(lastNineDigits)) {
                return range.getChannelInfo();
            }
        }
        return null;
    }

    /**
     * 初始化号段匹配数据
     */
    private void initSegmentData() {
        List<SegmentRange> ranges = new ArrayList<>();

        ranges.add(new SegmentRange(510000000L, 510499999L, new ChannelInfo("XSFZ", "线上发展", "FFP_GW", "官网", "WZTZ", "网站跳转", null, null)));
        ranges.add(new SegmentRange(520000000L, 529999999L, new ChannelInfo("XSFZ", "线上发展", "FFP_GW", "官网", "WZTZ", "网站跳转", null, null)));
        ranges.add(new SegmentRange(563600001L, 563900000L, new ChannelInfo("XSFZ", "线上发展", "FFP_GW", "官网", "WZTZ", "网站跳转", null, null)));
        ranges.add(new SegmentRange(567200001L, 570200000L, new ChannelInfo("XSFZ", "线上发展", "FFP_GW", "官网", "SHGW", "山航官网", null, null)));
        ranges.add(new SegmentRange(562500001L, 563500000L, new ChannelInfo("XSFZ", "线上发展", "FFP_GW", "官网", "GWFZ", "官网发展", null, null)));
        ranges.add(new SegmentRange(571000001L, 571100000L, new ChannelInfo("XSFZ", "线上发展", "FFP_GW", "官网", "GWFCYX", "山航官网（分层营销）", null, null)));
        ranges.add(new SegmentRange(550000000L, 550499999L, new ChannelInfo("XSFZ", "线上发展", "FFP_GW", "官网", "EWM", "国航二维码", null, null)));
        ranges.add(new SegmentRange(566800001L, 566900000L, new ChannelInfo("XSFZ", "线上发展", "FFP_OTA", "OTA会员发展（国）", "QUNAER", "去哪儿（山航）", null, null)));
        ranges.add(new SegmentRange(566700001L, 566800000L, new ChannelInfo("XSFZ", "线上发展", "FFP_OTA", "OTA会员发展（国）", "QUNAER", "去哪儿（山航）", null, null)));
        ranges.add(new SegmentRange(570900001L, 571000000L, new ChannelInfo("XSFZ", "线上发展", "FFP_OTA", "OTA会员发展（国）", "QUNAER", "去哪儿（山航）", null, null)));
        ranges.add(new SegmentRange(566600001L, 566700000L, new ChannelInfo("XSFZ", "线上发展", "FFP_OTA", "OTA会员发展（国）", "FEIZHU", "飞猪（山航）", null, null)));
        ranges.add(new SegmentRange(566500001L, 566600000L, new ChannelInfo("XSFZ", "线上发展", "FFP_OTA", "OTA会员发展（国）", "XIECHENG", "携程（山航）", null, null)));
        ranges.add(new SegmentRange(530000000L, 530499999L, new ChannelInfo("XSFZ", "线上发展", "FFP_ZSF", "掌尚飞", "SJKHD", "手机客户端", null, null)));
        ranges.add(new SegmentRange(564500001L, 566500000L, new ChannelInfo("XSFZ", "线上发展", "FFP_ZSF", "掌尚飞", "SJKHD", "手机客户端", null, null)));
        ranges.add(new SegmentRange(564010001L, 564200000L, new ChannelInfo("XSFZ", "线上发展", "FFP_ZSF", "掌尚飞", "SJKHD", "手机客户端", null, null)));
        ranges.add(new SegmentRange(563910001L, 564010000L, new ChannelInfo("XSFZ", "线上发展", "FFP_ZSF", "掌尚飞", "SJKHD", "手机客户端", null, null)));
        ranges.add(new SegmentRange(563500001L, 563600000L, new ChannelInfo("XSFZ", "线上发展", "FFP_WECHAT", "微信", "WX", "微信", null, null)));
        ranges.add(new SegmentRange(566900001L, 567200000L, new ChannelInfo("XSFZ", "线上发展", "FFP_WECHAT", "微信", "SHWXXCX", "山航微信小程序", null, null)));
        ranges.add(new SegmentRange(564200001L, 564500000L, new ChannelInfo("XSFZ", "线上发展", "FFP_WECHAT", "微信", "SHXCX", "山航微信小程序", null, null)));
        ranges.add(new SegmentRange(561000000L, 562000000L, new ChannelInfo("XSFZ", "线上发展", "DZSW", "电商", "HYZH", "会员转换", null, null)));
        ranges.add(new SegmentRange(562000001L, 562500000L, new ChannelInfo("XSFZ", "线上发展", "DZSW", "电商", "ZYH", "知音汇", null, null)));
        ranges.add(new SegmentRange(200185001L, 200235000L, new ChannelInfo("FFHZ", "非航合作", "FFP_YH", "银行（国）", "GFYH", "广发银行", null, null)));
        ranges.add(new SegmentRange(200030000L, 200185000L, new ChannelInfo("FFHZ", "非航合作", "FFP_YH", "银行（国）", "GFYH", "广发银行", null, null)));
        ranges.add(new SegmentRange(200000000L, 200029999L, new ChannelInfo("FFHZ", "非航合作", "FFP_YH", "银行（国）", "JSYH", "建设银行", null, null)));
        ranges.add(new SegmentRange(200385001L, 200415000L, new ChannelInfo("FFHZ", "非航合作", "FFP_YH", "银行（国）", "CAGF", "国航广发联名卡", null, null)));
        ranges.add(new SegmentRange(300000000L, 300100000L, new ChannelInfo("FFHZ", "非航合作", "FFP_YH", "银行（国）", "SHZX", "深航中信联名卡", null, null)));
        ranges.add(new SegmentRange(200235001L, 200385000L, new ChannelInfo("FFHZ", "非航合作", "FFP_YH", "银行（国）", "ZXYH", "中信银行", null, null)));
        ranges.add(new SegmentRange(300100000L, 301499999L, new ChannelInfo("FFHZ", "非航合作", "DSF", "第三方", "SFWB", "第三方外包", null, null)));
        ranges.add(new SegmentRange(100280001L, 100300000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/PEK", "北京营业部")));
        ranges.add(new SegmentRange(102920001L, 102930000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/DLC", "大连营业部")));
        ranges.add(new SegmentRange(102930001L, 102940000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/DLC", "大连营业部")));
        ranges.add(new SegmentRange(102910001L, 102920000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/DLC", "大连营业部")));
        ranges.add(new SegmentRange(100400001L, 100405000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/DLC", "大连营业部")));
        ranges.add(new SegmentRange(100300001L, 100310000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/CAN", "广州营业部")));
        ranges.add(new SegmentRange(100461001L, 100465000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/KWL", "桂林营业部")));
        ranges.add(new SegmentRange(100460001L, 100461000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/KWL", "桂林营业部")));
        ranges.add(new SegmentRange(103200001L, 103210000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/HRB", "哈尔滨营业部")));
        ranges.add(new SegmentRange(100420001L, 100425000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/HRB", "哈尔滨营业部")));
        ranges.add(new SegmentRange(100441001L, 100445000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/HAK", "海口营业部")));
        ranges.add(new SegmentRange(100440001L, 100441000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/HAK", "海口营业部")));
        ranges.add(new SegmentRange(100320001L, 100330000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/HGH", "杭州营业部")));
        ranges.add(new SegmentRange(100455001L, 100460000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/HFE", "合肥营业部")));
        ranges.add(new SegmentRange(100475001L, 100480000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/HET", "呼和浩特营业部")));
        ranges.add(new SegmentRange(100000000L, 100070000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/TNA", "济南营业部")));
        ranges.add(new SegmentRange(100500001L, 100700000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/TNA", "济南营业部")));
        ranges.add(new SegmentRange(100360001L, 100370000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/KMG", "昆明营业部")));
        ranges.add(new SegmentRange(104900001L, 104920000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/NKG", "南京营业部")));
        ranges.add(new SegmentRange(103100001L, 103200000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/NKG", "南京营业部")));
        ranges.add(new SegmentRange(100405001L, 100408000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/NKG", "南京营业部")));
        ranges.add(new SegmentRange(100408001L, 100410000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/NKG", "南京营业部")));
        ranges.add(new SegmentRange(100480001L, 100485000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/NNG", "南宁营业部")));
        ranges.add(new SegmentRange(105000001L, 105100000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/TAO", "青岛营业部")));
        ranges.add(new SegmentRange(100750001L, 100820000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/TAO", "青岛营业部")));
        ranges.add(new SegmentRange(100070001L, 100140000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/TAO", "青岛营业部")));
        ranges.add(new SegmentRange(100840001L, 100900000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/TAO", "青岛营业部")));
        ranges.add(new SegmentRange(100700001L, 100750000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/TAO", "青岛营业部")));
        ranges.add(new SegmentRange(105100001L, 105150000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/TAO", "青岛营业部")));
        ranges.add(new SegmentRange(100820001L, 100840000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/TAO", "青岛营业部")));
        ranges.add(new SegmentRange(100200001L, 100260000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/XMN", "厦门营业部")));
        ranges.add(new SegmentRange(100310001L, 100320000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/SHA", "上海营业部")));
        ranges.add(new SegmentRange(100330001L, 100340000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/SZX", "深圳营业部")));
        ranges.add(new SegmentRange(100425001L, 100430000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/SHE", "沈阳营业部")));
        ranges.add(new SegmentRange(100416001L, 100420000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/TYN", "太原营业部")));
        ranges.add(new SegmentRange(100415001L, 100416000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/TYN", "太原营业部")));
        ranges.add(new SegmentRange(100370001L, 100380000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/TSN", "天津营业部")));
        ranges.add(new SegmentRange(100445001L, 100450000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/WUH", "武汉营业部")));
        ranges.add(new SegmentRange(100340001L, 100350000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/XIY", "西安营业部")));
        ranges.add(new SegmentRange(100435001L, 100440000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/INC", "银川营业部")));
        ranges.add(new SegmentRange(100495001L, 100499999L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/CGQ", "长春营业部")));
        ranges.add(new SegmentRange(100380001L, 100390000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/CGO", "郑州营业部")));
        ranges.add(new SegmentRange(100260001L, 100280000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/CKG", "重庆营业部")));
        ranges.add(new SegmentRange(101300001L, 101320000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/CKG", "重庆营业部")));
        ranges.add(new SegmentRange(100430001L, 100435000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/KWE", "贵阳营业部")));
        ranges.add(new SegmentRange(100410001L, 100413000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/LHW", "兰州营业部")));
        ranges.add(new SegmentRange(100413001L, 100415000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/LHW", "兰州营业部")));
        ranges.add(new SegmentRange(100470001L, 100475000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/KHN", "南昌营业部")));
        ranges.add(new SegmentRange(100490001L, 100495000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/NGB", "宁波营业部")));
        ranges.add(new SegmentRange(100465001L, 100470000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/SYX", "三亚营业部")));
        ranges.add(new SegmentRange(100485001L, 100490000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/SJW", "石家庄营业部")));
        ranges.add(new SegmentRange(100350001L, 100360000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/WNZ", "温州营业部")));
        ranges.add(new SegmentRange(100390001L, 100400000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/URC", "乌鲁木齐营业部")));
        ranges.add(new SegmentRange(100140001L, 100200000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/YNT", "烟台营业部")));
        ranges.add(new SegmentRange(100450001L, 100455000L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "YYB", "营业部", "MSC-OF/CSX", "长沙营业部")));
        ranges.add(new SegmentRange(41000000L, 47999999L, new ChannelInfo("XXFZ", "线下发展", "FFP_YYB", "营业部", "PAD", "PAD", "PAD", "PAD")));
        ranges.add(new SegmentRange(48000000L, 52999999L, new ChannelInfo("XXFZ", "线下发展", "FFP_JS", "机上会员发展", "PAD", "PAD", null, null)));
        ranges.add(new SegmentRange(53000000L, 54999999L, new ChannelInfo("XXFZ", "线下发展", "FFP_JS", "机上会员发展", "PAD", "PAD", null, null)));
        ranges.add(new SegmentRange(0L, 999999L, new ChannelInfo("XXFZ", "线下发展", "FFP_JS", "机上会员发展", "OKCB", "老客舱", "OKCBJNKC", "山航济南客舱")));
        ranges.add(new SegmentRange(10000000L, 10999999L, new ChannelInfo("XXFZ", "线下发展", "FFP_JS", "机上会员发展", "OKCB", "老客舱", "OKCBQDKC", "山航青岛客舱")));
        ranges.add(new SegmentRange(40000000L, 40999999L, new ChannelInfo("XXFZ", "线下发展", "FFP_JS", "机上会员发展", "OKCB", "老客舱", "OKCBSC", "山航客舱")));
        ranges.add(new SegmentRange(20000000L, 20100000L, new ChannelInfo("XXFZ", "线下发展", "FFP_JS", "机上会员发展", "OKCB", "老客舱", "OKCBYTKC", "山航烟台客舱")));
        ranges.add(new SegmentRange(30000000L, 30100000L, new ChannelInfo("XXFZ", "线下发展", "FFP_JS", "机上会员发展", "OKCB", "老客舱", "OKCBXMKC", "山航厦门客舱")));

        ranges.sort(Comparator.comparingLong(a -> a.start));

        for (SegmentRange range : ranges) {
            segmentMap.put(range.start, range);
        }
    }

    /**
     * 号段区间内部类
     */
    private static class SegmentRange {
        private final long start;
        private final long end;
        private final ChannelInfo channelInfo;

        public SegmentRange(long start, long end, ChannelInfo channelInfo) {
            this.start = start;
            this.end = end;
            this.channelInfo = channelInfo;
        }

        public boolean contains(long number) {
            return number >= start && number <= end;
        }

        public ChannelInfo getChannelInfo() {
            return channelInfo;
        }
    }

    public static void main(String[] args) {
        CardNumberSegmentMatcher instance = CardNumberSegmentMatcher.getInstance();
        System.out.println(instance.match("000054999999"));
    }
}

