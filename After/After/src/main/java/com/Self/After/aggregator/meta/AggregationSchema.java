package com.Self.After.aggregator.meta;

import com.Self.After.row.RowMeta;

public class AggregationSchema {
	
	public int id;
	public RowMeta keySchema;
	public RowMeta valueSchema;


	public AggregationSchema() {
		keySchema = new RowMeta();
		valueSchema = new RowMeta();
	}
	
	@Override
	public String toString() {
		return "AggregationSchema{" +
				"id=" + id +
				", keySchema=" + keySchema +
				", valueSchema=" + valueSchema +
				'}';
	}
}
