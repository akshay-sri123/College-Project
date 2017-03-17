package com.Self.After;

import java.util.Map;

import com.Self.After.row.Row;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

public class Output extends BaseOperator implements InputOperator{

	public Output()
	{
	}

	public final transient DefaultOutputPort<String> outputPort = new DefaultOutputPort<>();

	public final transient DefaultInputPort<Map.Entry<Row, Row>> inputPort = new DefaultInputPort<Map.Entry<Row, Row>>()
	{
		@Override
		public void process(Map.Entry<Row, Row> rowEntry)
		{
				outputPort.emit(rowEntry.toString());
		}
	};

	public void emitTuples()
	{

	}
}
