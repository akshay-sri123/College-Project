package com.Self.Before.other;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AggregatorById extends Aggregator {

    public Map<List, List> aggMap = new HashMap<>();

    public AggregatorById(){}

    public AggregatorById(String[] keys, String[] metrics, int id)
    {
        super(keys, metrics, id);
    }

    public AggregatorById(int id) {
        super(new IdFactory().getKey(id), null, id);
    }
}
