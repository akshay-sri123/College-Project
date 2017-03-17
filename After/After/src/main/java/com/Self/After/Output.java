package com.Self.After;

import java.util.Map;

import com.Self.After.aggregator.meta.AggregationSchema;
import com.Self.After.other.AdInfo;
import com.Self.After.row.PojoBasedCoder;
import com.Self.After.row.Row;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

public class Output extends BaseOperator implements InputOperator{

	public AggregationSchema schema;
	public PojoBasedCoder coder = new PojoBasedCoder();

	public Output()
	{
	}

	public Output(AggregationSchema schema)
	{
		this.schema = schema;
	}



	public final transient DefaultOutputPort<String> outputPort = new DefaultOutputPort<>();

	public final transient DefaultInputPort<Map<Row, Row>> inputPort = new DefaultInputPort<Map<Row, Row>>()
	{
		@Override
		public void process(Map<Row, Row> resultMap)
		{
			for(Map.Entry<Row, Row> entry : resultMap.entrySet())
			{
				AdInfo adInfo = new AdInfo();
				try {
					adInfo = (AdInfo) coder.decoder(schema.keySchema,entry.getKey(), adInfo);
					adInfo = (AdInfo) coder.decoder(schema.valueSchema,entry.getValue(), adInfo);
				} catch (NoSuchFieldException e) {
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				}

				outputPort.emit(adInfo.toString());

			}
		}
	};

	public void emitTuples()
	{

	}
}
