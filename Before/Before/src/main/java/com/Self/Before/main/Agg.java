package com.Self.Before.main;


import com.Self.Before.other.AdInfo;
import com.Self.Before.other.Aggregator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;


import java.util.ArrayList;
import java.util.List;

public class Agg extends BaseOperator{
	
	List<Aggregator> aggregatorList = new ArrayList<>();
	
	public void addAggregator(Aggregator aggregator) {
		aggregatorList.add(aggregator);
	}
	
	
	public final DefaultInputPort<AdInfo> inputPort = new DefaultInputPort<AdInfo>() {
		@Override
		public void process(AdInfo adInfo) {
			
			for (Aggregator aggregator : aggregatorList) {
				aggregator.add(adInfo);
			}
		}
	};
	
	public List<Aggregator> getAggregatorList() {
		return aggregatorList;
	}
}
