package com.Self.After;

import java.util.HashMap;
import java.util.Map;

import com.Self.After.aggregator.meta.AggregationMetrics;
import com.Self.After.aggregator.meta.AggregationSchema;
import com.Self.After.aggregator.operations.Operations;
import com.Self.After.row.EntryField;
import com.Self.After.row.Row;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

public class Aggregation extends BaseOperator implements InputOperator
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

	public final transient DefaultOutputPort<Map<Row, Row>> outputPort = new DefaultOutputPort<>();
	public final transient DefaultInputPort<EntryField> inputPort = new DefaultInputPort<EntryField>()
	{
		@Override
		public void process(EntryField entryField)
		{
			resultMap = operations.aggOperations(entryField, metrics, schema, resultMap);

			outputPort.emit(resultMap);
		}
	};


	public void emitTuples(){}

}
