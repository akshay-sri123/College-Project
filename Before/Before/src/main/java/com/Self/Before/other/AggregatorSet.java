package com.Self.Before.other;

import java.util.ArrayList;
import java.util.List;

public class AggregatorSet
{

    public AggregatorSet()
    {
    }

    List<Aggregator> aggregatorList = new ArrayList<>();

    public void addAggregator(Aggregator aggregator) {
        aggregatorList.add(aggregator);
    }

    public void processItem(AdInfo adInfo) {
        for (Aggregator aggr : aggregatorList) {
            aggr.add(adInfo);
        }
    }

    public List<Aggregator> getAggregatorList() {
        return aggregatorList;
    }
}
