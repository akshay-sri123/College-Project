package com.Self.After;

import java.util.HashMap;
import java.util.Map;

import com.Self.After.aggregator.meta.AggregationMetrics;
import com.Self.After.aggregator.meta.AggregationSchema;
import com.Self.After.aggregator.operations.Operations;
import com.Self.After.other.AdInfo;
import com.Self.After.row.EntryField;
import com.Self.After.row.PojoBasedCoder;
import com.Self.After.row.Row;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

public class Aggregation extends BaseOperator
{
	public AggregationMetrics metrics;
	public AggregationSchema schema;
	public Map<Row, Row> resultMap = new HashMap<>();

	public Operations operations = new Operations();



	public Aggregation()
	{
	}

	public Aggregation(AggregationMetrics metrics, AggregationSchema schema)
	{
		this.metrics = metrics;
		this.schema = schema;
	}

	public PojoBasedCoder coder = new PojoBasedCoder();

	public final transient DefaultOutputPort<String> outputPort = new DefaultOutputPort<>();
	public final transient DefaultInputPort<EntryField> inputPort = new DefaultInputPort<EntryField>()
	{
		@Override
		public void process(EntryField entryField)
		{
			resultMap = operations.aggOperations(entryField, metrics, schema, resultMap);


		}
	};

	@Override
	public void endWindow()
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
		super.endWindow();
	}
}
