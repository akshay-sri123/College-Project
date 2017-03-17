package com.Self.Before.other;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Aggregator {
    public Aggregator()
    {
    }

    public String[] keys;
    public String[] metrics;
    public int id;
    public Map<List, List> aggMap = new HashMap<>();


    public Aggregator(String[] keys, String[] metrics, int id) {
        this.keys = keys;
        this.metrics = metrics;
        this.id = id;
    }

    public void add(AdInfo adInfo) {
        List key = getKey(adInfo);
        List vals = aggMap.get(key);
        if (vals == null) {
            vals = getInitialValues(adInfo);
            aggMap.put(key, vals);
        } else {
            updateValues(vals, adInfo);
        }
    }

    private void updateValues(List vals, AdInfo adInfo) {
      /*
      0->cost
  	1->clicks
  	2->impressions
  	3->frequency
  	 */
        vals.set(0, (Long) vals.get(0) + adInfo.getCost());
        vals.set(1, (Long) vals.get(1) + adInfo.getImpressions());
        vals.set(2, (Integer) vals.get(2) + (adInfo.isClicks() ? 1 : 0));
        vals.set(3, (Integer) vals.get(3) + 1);
    }

    private List getInitialValues(AdInfo adInfo) {
        List vals;
        vals = new ArrayList();
        //0->cost, 1->impressions, 2->click, 3->frequency
        vals.add(adInfo.getCost());
        vals.add(adInfo.getImpressions());
        vals.add(adInfo.isClicks() ? 1 : 0);
        vals.add(1);
        return vals;
    }

    private List getKey(AdInfo adInfo) {
        List key = new ArrayList();
        key.add(id);
        for (String keyStr : keys) {
            if (keyStr.equals("Publisher")) {
                key.add(adInfo.getPublisher());
            } else if (keyStr.equals("Location")) {
                key.add(adInfo.getLocation());
            } else if (keyStr.equals("Advertiser")) {
                key.add(adInfo.getAdvertiser());
            }
        }
        return key;
    }

    public void dump() {
        for (Map.Entry entry : aggMap.entrySet()) {
            System.out.println(entry.getKey() + " " + entry.getValue());
        }
    }
}