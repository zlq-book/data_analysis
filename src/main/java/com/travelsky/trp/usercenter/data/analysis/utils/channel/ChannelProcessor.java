package com.travelsky.trp.usercenter.data.analysis.utils.channel;

import com.travelsky.trp.usercenter.data.analysis.utils.ChannelInfo;

public interface ChannelProcessor {
    boolean supports(String channelId);
    ChannelInfo map(String channelId);
}


