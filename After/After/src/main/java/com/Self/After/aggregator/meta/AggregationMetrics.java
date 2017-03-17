package com.Self.After.aggregator.meta;

import java.util.Arrays;

public class AggregationMetrics
{
	String[] keys;
	String[] vals;
	AggregationTypes aggTypes;

	public AggregationMetrics(){}

	public AggregationMetrics(String[] keys, String[] vals, AggregationTypes aggTypes) {
		this.keys = keys;
		this.vals = vals;
		this.aggTypes = aggTypes;
	}
	
	public String[] getKeys()
	{
		return keys;
	}

	public void setKeys(String[] keys)
	{
		this.keys = keys;
	}
	
	public String[] getVals() {	return vals; }
	
	public void setVals(String[] vals) { this.vals = vals;	}
	
	public AggregationTypes getAggTypes()
	{
		return aggTypes;
	}

	public void setAggTypes(AggregationTypes aggTypes)
	{
		this.aggTypes = aggTypes;
	}
	
	@Override
	public String toString() {
		return "AggregationMetrics{" +
				"keys=" + Arrays.toString(keys) +
				", vals=" + Arrays.toString(vals) +
				", aggTypes=" + aggTypes +
				'}';
	}
}
