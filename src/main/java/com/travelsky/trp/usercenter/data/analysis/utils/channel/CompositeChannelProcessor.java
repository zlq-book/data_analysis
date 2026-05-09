package com.travelsky.trp.usercenter.data.analysis.utils.channel;

import com.travelsky.trp.usercenter.data.analysis.utils.ChannelInfo;

import java.util.ArrayList;
import java.util.List;

public class CompositeChannelProcessor implements ChannelProcessor {

    private final List<ChannelProcessor> processors = new ArrayList<>();

    public CompositeChannelProcessor() {
        processors.add(new AChannelProcessor());
        processors.add(new BChannelProcessor());
    }

    @Override
    public boolean supports(String channelId) {
        for (ChannelProcessor p : processors) {
            if (p.supports(channelId)) return true;
        }
        return false;
    }

    @Override
    public ChannelInfo map(String channelId) {
        for (ChannelProcessor p : processors) {
            if (p.supports(channelId)) {
                return p.map(channelId);
            }
        }
        return null;
    }
}


