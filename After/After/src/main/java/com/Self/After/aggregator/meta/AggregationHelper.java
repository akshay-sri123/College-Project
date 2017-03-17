package com.Self.After.aggregator.meta;

import com.Self.After.row.RowMeta;

public class AggregationHelper {

	public AggregationHelper()
	{
	}

	public AggregationSchema createAggregationSchema(RowMeta originalSchema, AggregationMetrics aggregationMetrics) throws NoSuchFieldException, IllegalAccessException {
		
			
			AggregationSchema schema = new AggregationSchema();

			schema.keySchema = originalSchema.subset(aggregationMetrics.getKeys());
			switch (aggregationMetrics.getAggTypes()) {
				case COUNT:
					schema.valueSchema = originalSchema.subset(aggregationMetrics.getVals());
					break;
				
				case SUM:
					schema.valueSchema = originalSchema.subset(aggregationMetrics.getVals());
					break;
				
				case MIN:
					schema.valueSchema = originalSchema.subset(aggregationMetrics.getVals());
					break;
				
				case MAX:
					schema.valueSchema = originalSchema.subset(aggregationMetrics.getVals());
					break;
			}
		
		return schema;
	}
}
