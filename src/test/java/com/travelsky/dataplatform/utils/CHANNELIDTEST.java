package com.travelsky.dataplatform.utils;

import com.travelsky.trp.usercenter.data.analysis.utils.ChannelInfo;
import com.travelsky.trp.usercenter.data.analysis.utils.channel.CompositeChannelProcessor;

public class CHANNELIDTEST {
    public static void main(String[] args) {
        CompositeChannelProcessor channelProcessor = new CompositeChannelProcessor();
        String channelId="wechatmini";
        if (channelProcessor.supports(channelId)) {
            ChannelInfo info = channelProcessor.map(channelId);
            if (info != null) {
                System.out.println(info.toString());
            }
        }
    }
}
