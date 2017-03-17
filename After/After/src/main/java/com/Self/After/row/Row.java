package com.Self.After.row;

import java.util.Arrays;

public class Row {
	public byte[] dataBytes;

	public Row(){}

	public byte[] getDataBytes() {
		return dataBytes;
	}
	
	@Override
	public boolean equals(Object object) {
		if (this == object) return true;
		if (object == null || getClass() != object.getClass()) return false;
		
		Row row = (Row) object;
		
		return Arrays.equals(dataBytes, row.dataBytes);
	}
	
	@Override
	public int hashCode() {
		return Arrays.hashCode(dataBytes);
	}
}
