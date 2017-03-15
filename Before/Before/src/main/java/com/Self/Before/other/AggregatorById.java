package com.Self.Before.other;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AggregatorById extends Aggregator {
    private Map<List, List> aggMap = new HashMap<>();

    public AggregatorById(int id) {
        super(new IdFactory().getKey(id), null, id);
    }
}
