package com.travelsky.trp.usercenter.data.analysis.utils.channel;

import com.travelsky.trp.usercenter.data.analysis.utils.ChannelInfo;

import java.util.HashMap;
import java.util.Map;

public class AChannelProcessor implements ChannelProcessor {

    private static final String LEVEL1_CODE = "XSFZ";
    private static final String LEVEL1_NAME = "线上发展";

    private static final Map<String, String[]> CHANNEL_MAP = new HashMap<>();
    static {
        CHANNEL_MAP.put("SC_LYX_LYZ", new String[]{"FFP_LYX", "鲁雁行", "SC_LYX_LYZ", "鲁雁行"});
        CHANNEL_MAP.put("wechatmini", new String[]{"FFP_WECHAT", "微信", "wechatmini", "微信小程序"});
        CHANNEL_MAP.put("APP", new String[]{"FFP_ZSF", "掌尚飞", "APP", "APP"});
        CHANNEL_MAP.put("SCAPP", new String[]{"FFP_ZSF", "掌尚飞", "SCAPP", "掌尚飞"});
        CHANNEL_MAP.put("SCGW", new String[]{"FFP_GW", "官网", "SCGW", "官网"});
        CHANNEL_MAP.put("hytd@pc", new String[]{"FFP_HYTD", "会员天地", "hytd@pc", "会员天地PC"});
        CHANNEL_MAP.put("hytd@h5", new String[]{"FFP_HYTD", "会员天地", "hytd@h5", "会员天地H5"});
        CHANNEL_MAP.put("XCXHYTD", new String[]{"FFP_HYTD", "会员天地", "XCXHYTD", "微信小程序会员天地"});
        CHANNEL_MAP.put("SCZSF", new String[]{"FFP_HYTD", "会员天地", "SCZSF", "掌尚飞会员天地"});
        CHANNEL_MAP.put("GWHYTD", new String[]{"FFP_HYTD", "会员天地", "GWHYTD", "官网会员天地"});
    }

    @Override
    public boolean supports(String channelId) {
        if (channelId == null) return false;
        return CHANNEL_MAP.containsKey(channelId);
    }

    @Override
    public ChannelInfo map(String channelId) {
        String[] m = CHANNEL_MAP.get(channelId);
        if (m == null) return null;
        return new ChannelInfo(
                LEVEL1_CODE, LEVEL1_NAME,
                m[0], m[1],
                m[2], m[3],
                null, null
        );
    }
}


