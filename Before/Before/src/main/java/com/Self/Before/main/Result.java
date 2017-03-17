package com.Self.Before.main;

import java.util.Map;

import com.Self.Before.other.Aggregator;
import com.Self.Before.other.AggregatorSet;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

public class Result extends BaseOperator
{

  public Result()
  {
  }

  public final transient DefaultOutputPort<String> outputPort = new DefaultOutputPort<>();

  public final transient DefaultInputPort<AggregatorSet> inputPort = new DefaultInputPort<AggregatorSet>()
  {
    @Override
    public void process(AggregatorSet aggregatorSet)
    {
      for(Aggregator aggregator : aggregatorSet.getAggregatorList())
      {
        for(Map.Entry entry : aggregator.aggMap.entrySet())
        {
          outputPort.emit(entry.getKey() + " " + entry.getValue());
        }
      }
    }
  };

}
