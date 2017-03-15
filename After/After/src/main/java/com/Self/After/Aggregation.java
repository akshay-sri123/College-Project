package com.Self.After;


import com.Self.After.aggregator.meta.AggregationMetrics;
import com.Self.After.aggregator.meta.AggregationSchema;
import com.Self.After.aggregator.operations.Operations;
import com.Self.After.row.EntryField;
import com.Self.After.row.Row;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

import java.util.HashMap;
import java.util.Map;

public class Aggregation extends BaseOperator implements InputOperator
{
	AggregationMetrics metrics;
	AggregationSchema schema;
	Map<Row, Row> resultMap = new HashMap<>();

	Operations operations = new Operations();
	public Aggregation(AggregationMetrics metrics, AggregationSchema schema)
	{
		this.metrics = metrics;
		this.schema = schema;
	}
	
	public final DefaultInputPort<EntryField> inputPort = new DefaultInputPort<EntryField>() {
		@Override
		public void process(EntryField entryField) {
			resultMap = operations.aggOperations(entryField,metrics,schema,resultMap);
		}
	};
	
	public final DefaultOutputPort<Map<Row, Row>> outputPort = new DefaultOutputPort<>();
	
	public void emitTuples()
	{
		outputPort.emit(resultMap);
	}
}
