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
	public RowMeta rowMeta;
	public AggregationMetrics metrics;
	public AggregationSchema schema;

	public EntryField entryField;

	public ByteLength length = new ByteLength();
	public PojoBasedCoder coder = new PojoBasedCoder();

	public Intermediate(){}

	public Intermediate(RowMeta rowMeta, AggregationMetrics metrics, AggregationSchema schema)
	{
		this.rowMeta = rowMeta;
		this.metrics = metrics;
		this.schema = schema;
	}

	public final transient DefaultOutputPort<EntryField> outputPort = new DefaultOutputPort<>();
	public final transient DefaultInputPort<AdInfo> inputPort = new DefaultInputPort<AdInfo>() {
		@Override
		public void process(AdInfo adInfo) {

			Row keyRow = new Row();
			Row valRow = new Row();
			try {
				int keyLen = length.getByteLength(schema.keySchema, adInfo);
				int valLen = length.getByteLength(schema.valueSchema, adInfo);
				keyRow.dataBytes = new byte[keyLen];
				keyRow = coder.encoder(schema.keySchema, adInfo);

				valRow.dataBytes = new byte[valLen];
				valRow = coder.encoder(schema.valueSchema, adInfo);
			} catch (Exception e) {
				e.printStackTrace();}

			entryField = new EntryField(keyRow,valRow);
			outputPort.emit(entryField);
		}
	};
	
	public void emitTuples()
	{
	}
}
