package com.travelsky.trp.usercenter.data.analysis.utils.channel;

import com.travelsky.trp.usercenter.data.analysis.utils.ChannelInfo;

public class BChannelProcessor implements ChannelProcessor {

    @Override
    public boolean supports(String channelId) {
        if (channelId == null) return false;
        return "SCLYGJ".equals(channelId) || "clkFFP".equals(channelId);
    }

    @Override
    public ChannelInfo map(String channelId) {
        if ("SCLYGJ".equals(channelId)) {
            return new ChannelInfo(
                    "XXFZ", "线下发展",
                    "FFP_JS", "机上会员发展",
                    null, null,
                    null, null
            );
        }
        if ("cIkFFP".equals(channelId)) {
            return new ChannelInfo(
                    "XXFZ", "线下发展",
                    "FFP_CLK", "常旅客系统",
                    null, null,
                    null, null
            );
        }
        return null;
    }
}


