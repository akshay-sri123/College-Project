package com.Self.Before.main;

import com.Self.Before.aggregations.AggregationHelper;
import com.Self.Before.aggregations.AggregationMetrics;
import com.Self.Before.aggregations.AggregationSchema;
import com.Self.Before.aggregations.AggregationTypes;
import com.Self.Before.other.*;
import com.Self.Before.row.DataType;
import com.Self.Before.row.RowMeta;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

import java.util.ArrayList;
import java.util.List;

public class Generator extends BaseOperator implements InputOperator
{
  private static RandomEnumGenerator<Publisher> randomPublisher = new RandomEnumGenerator<Publisher>(Publisher.class);
  private static RandomEnumGenerator<Advertiser> randomAdvertiser = new RandomEnumGenerator<Advertiser>(Advertiser.class);
  private static RandomEnumGenerator<Location> randomLocation = new RandomEnumGenerator<Location>(Location.class);
  private static RandomValueGenerator randomValueGenerator = new RandomValueGenerator();

  public final DefaultOutputPort<AdInfo> outputPort = new DefaultOutputPort<>();

  @Override
  public void emitTuples()
  {
    long seconds = 60 ;

    long startTime = System.currentTimeMillis();
    long currentTime = 0, timeDifference = 0;

    do{
      AdInfo adInfo = new AdInfo(randomPublisher.random().toString(), randomAdvertiser.random().toString(), randomLocation.random().toString(),
        randomValueGenerator.randomCost(), randomValueGenerator.randomImpressions(), randomValueGenerator.randomClicks());

      outputPort.emit(adInfo);

      currentTime = System.currentTimeMillis();
      timeDifference = currentTime - startTime;
    } while(timeDifference < (seconds * 1000));

  }
}
