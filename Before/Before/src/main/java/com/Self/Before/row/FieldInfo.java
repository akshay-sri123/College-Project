package com.Self.Before.row;

public class FieldInfo {
	public String name;
	public DataType dataType;
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public DataType getDataType() {
		return dataType;
	}
	
	public void setDataType(DataType dataType) {
		this.dataType = dataType;
	}
	
	@Override
	public String toString() {
		return "FieldInfo{" +
				"name='" + name + '\'' +
				", dataType=" + dataType +
				'}';
	}
}
