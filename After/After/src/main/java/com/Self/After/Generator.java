package com.Self.After;

import com.Self.After.other.*;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

public class Generator extends BaseOperator implements InputOperator
{
	private static RandomEnumGenerator<Publisher> randomPublisher = new RandomEnumGenerator<Publisher>(Publisher.class);
	private static RandomEnumGenerator<Advertiser> randomAdvertiser = new RandomEnumGenerator<Advertiser>(Advertiser.class);
	private static RandomEnumGenerator<Location> randomLocation = new RandomEnumGenerator<Location>(Location.class);
	private static RandomValueGenerator randomValueGenerator = new RandomValueGenerator();
	private int recordLimit = 10;
	public final DefaultOutputPort<AdInfo> outputPort = new DefaultOutputPort<>();
	private int available;

	public Generator(){}

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
