package com.Self.After;

import com.Self.After.aggregator.meta.AggregationMetrics;
import com.Self.After.aggregator.meta.AggregationSchema;
import com.Self.After.other.AdInfo;
import com.Self.After.row.*;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

public class Intermediate extends BaseOperator implements InputOperator
{
	RowMeta rowMeta;
	AggregationMetrics metrics;
	AggregationSchema schema;

	EntryField entryField;
	
	
	public Intermediate(RowMeta rowMeta, AggregationMetrics metrics, AggregationSchema schema)
	{
		this.rowMeta = rowMeta;
		this.metrics = metrics;
		this.schema = schema;
	}
	
	public final DefaultOutputPort<EntryField> outputPort = new DefaultOutputPort<>();
	public final DefaultInputPort<AdInfo> inputPort = new DefaultInputPort<AdInfo>() {
		@Override
		public void process(AdInfo adInfo) {
			ByteLength length = new ByteLength();
			PojoBasedCoder coder = new PojoBasedCoder();
			
			
			int keyLen = 0;
			try {
				keyLen = length.getByteLength(schema.keySchema, adInfo);
				int valLen = length.getByteLength(schema.valueSchema, adInfo);
				
				Row keyRow = new Row();
				Row valRow = new Row();
				
				keyRow.dataBytes = new byte[keyLen];
				keyRow = coder.encoder(schema.keySchema, adInfo);
				
				valRow.dataBytes = new byte[valLen];
				valRow = coder.encoder(schema.valueSchema, adInfo);
				entryField = new EntryField(keyRow,valRow);
				
			} catch (Exception e) {
				e.printStackTrace();}
		}
	};
	
	public void init() throws NoSuchFieldException, IllegalAccessException {
		
	}
	
	public void emitTuples()
	{
		outputPort.emit(entryField);
	}
}
