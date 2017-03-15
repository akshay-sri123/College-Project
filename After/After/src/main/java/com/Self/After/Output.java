package com.Self.After;

import com.Self.After.aggregator.meta.AggregationSchema;
import com.Self.After.other.AdInfo;
import com.Self.After.row.PojoBasedCoder;
import com.Self.After.row.Row;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

import java.util.Map;

public class Output extends BaseOperator implements InputOperator{
	
	AdInfo adInfo ;
	AggregationSchema schema = new AggregationSchema();
	PojoBasedCoder coder = new PojoBasedCoder();
	
	public Output(AggregationSchema schema)
	{
		this.schema = schema;
	}
	
	public final DefaultInputPort<Map<Row, Row>> inputPort = new DefaultInputPort<Map<Row, Row>>() {
		@Override
		public void process(Map<Row, Row> resultMap) {
			for(Map.Entry<Row, Row> entry : resultMap.entrySet())
			{
				adInfo = new AdInfo();
				try {
					adInfo = (AdInfo) coder.decoder(schema.keySchema ,entry.getKey(), adInfo);
					adInfo = (AdInfo) coder.decoder(schema.valueSchema,entry.getValue(), adInfo);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}}};
	
	public final DefaultOutputPort<String> outputPort = new DefaultOutputPort<>();
	public void emitTuples()
	{
		outputPort.emit(adInfo.toString());
	}
}
