package com.Self.After.aggregator.operations;

import com.Self.After.aggregator.meta.AggregationMetrics;
import com.Self.After.aggregator.meta.AggregationSchema;
import com.Self.After.row.EntryField;
import com.Self.After.row.Row;
import com.Self.After.row.RowValueFunctions;

import java.util.Map;

public class Operations
{
	long max;

	public Operations()
	{
	}

	public Map<Row, Row> aggOperations(EntryField entryField, AggregationMetrics metrics, AggregationSchema schema, Map<Row, Row> resultMap)
	{
		Row valRow;
		RowValueFunctions rowValueFunctions = new RowValueFunctions();
		switch (metrics.getAggTypes())
		{
			case MAX:
				if(resultMap.containsKey(entryField.getKeyRow()))
				{
					long currentVal = rowValueFunctions.readLong(entryField.getValRow(), schema.valueSchema);
					valRow = resultMap.get(entryField.getKeyRow());
					long currentMapVal = rowValueFunctions.readLong(valRow, schema.valueSchema);
					
					if(currentVal > currentMapVal)
					{
						valRow = rowValueFunctions.updateLong(resultMap.get(entryField.getKeyRow()), schema.valueSchema, currentVal);
						resultMap.put(entryField.getKeyRow(), valRow);
					}
				}
				else
				{
					resultMap.put(entryField.getKeyRow(), entryField.getValRow());
				}
				break;
			case MIN:
				if(resultMap.containsKey(entryField.getKeyRow()))
				{
					long currentVal = rowValueFunctions.readLong(entryField.getValRow(), schema.valueSchema);
					valRow = resultMap.get(entryField.getKeyRow());
					long currentMapVal = rowValueFunctions.readLong(valRow, schema.valueSchema);
					
					if(currentVal < currentMapVal)
					{
						valRow = rowValueFunctions.updateLong(resultMap.get(entryField.getKeyRow()), schema.valueSchema, currentVal);
						resultMap.put(entryField.getKeyRow(), valRow);
					}
				}
				else
				{
					resultMap.put(entryField.getKeyRow(), entryField.getValRow());
				}
				break;
			case COUNT:
				break;
			case SUM:
				break;
			case AVG:
				break;
		}
		return resultMap;
	}
}
