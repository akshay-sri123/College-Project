package com.Self.After.row;

import com.Self.After.platform.Platform;

public class RowValueFunctions {
	
	int readOffset = 0;
	int updateOffset = 0;
	
	public int readInteger(Row row, RowMeta rowMeta)
	{
		int intVal = Platform.getInt(row.dataBytes, Platform.INT_ARRAY_OFFSET + readOffset);
		readOffset += 8;
		return intVal;
	}
	
	public long readLong(Row row, RowMeta rowMeta)
	{
		long longVal = Platform.getLong(row.dataBytes, Platform.LONG_ARRAY_OFFSET + readOffset);
		return  longVal;
	}
	
	public Row updateLong(Row row, RowMeta rowMeta, long longVal){
		Platform.putLong(row.dataBytes, Platform.LONG_ARRAY_OFFSET + updateOffset, longVal);
		return row;
	}
}
