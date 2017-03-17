package com.Self.Before.main;

import com.Self.Before.other.AdInfo;
import com.Self.Before.other.AggregatorSet;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

public class Aggregation extends BaseOperator{

	public AggregatorSet aggregatorSet;

	public Aggregation()
	{
	}

	public Aggregation(AggregatorSet aggregatorSet)
	{
		this.aggregatorSet = aggregatorSet;
	}

	public final transient DefaultOutputPort<AggregatorSet> outputPort = new DefaultOutputPort<>();
	public final transient DefaultInputPort<AdInfo> inputPort = new DefaultInputPort<AdInfo>()
	{
		@Override
		public void process(AdInfo adInfo)
		{
				aggregatorSet.processItem(adInfo);

				outputPort.emit(aggregatorSet);
		}
	};

}
