package com.Self.Before.main;

import com.Self.Before.other.AdInfo;
import com.Self.Before.other.Advertiser;
import com.Self.Before.other.Location;
import com.Self.Before.other.Publisher;
import com.Self.Before.other.RandomEnumGenerator;
import com.Self.Before.other.RandomValueGenerator;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

public class Generator extends BaseOperator implements InputOperator
{
  public Generator()
  {
  }

  private static RandomEnumGenerator<Publisher> randomPublisher = new RandomEnumGenerator<Publisher>(Publisher.class);
  private static RandomEnumGenerator<Advertiser> randomAdvertiser = new RandomEnumGenerator<Advertiser>(Advertiser.class);
  private static RandomEnumGenerator<Location> randomLocation = new RandomEnumGenerator<Location>(Location.class);
  private static RandomValueGenerator randomValueGenerator = new RandomValueGenerator();

  public final DefaultOutputPort<AdInfo> outputPort = new DefaultOutputPort<>();

  private int recordLimit = 10;
  private int available;

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    available = recordLimit;
  }

  @Override
  public void emitTuples()
  {
    if (available > 0) {
      AdInfo adInfo = new AdInfo(randomPublisher.random().toString(), randomAdvertiser.random().toString(), randomLocation.random().toString(),
        randomValueGenerator.randomCost(), randomValueGenerator.randomImpressions(), randomValueGenerator.randomClicks());

      outputPort.emit(adInfo);
      available--;
    }
  }
  public int getRecordLimit()
  {
    return recordLimit;
  }

  public void setRecordLimit(int recordLimit)
  {
    this.recordLimit = recordLimit;
  }

}

