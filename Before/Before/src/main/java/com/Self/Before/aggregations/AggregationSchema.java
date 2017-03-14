package com.Self.Before.aggregations;

import com.Self.Before.row.RowMeta;

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

