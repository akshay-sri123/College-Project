package com.Self.After.row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RowMeta {
	public List<FieldInfo> fieldInfoList = new ArrayList<>();
	private Map<String, DataType> nameToTypeMap = new HashMap<>();

	public RowMeta(){}

	public void addField(String name, DataType dataType) {

		FieldInfo fieldInfo = new FieldInfo();
		fieldInfo.setName(name);
		fieldInfo.setDataType(dataType);
		
		fieldInfoList.add(fieldInfo);
    nameToTypeMap.put(name, dataType);
	}
	
	public boolean isKeyPresent(String key) {
		return nameToTypeMap.containsKey(key);
	}

	public RowMeta subset(String[] keys)
	{
	    RowMeta meta = new RowMeta();
	    for (String key : keys) {
	      if (isKeyPresent(key)) {	        meta.addField(key, nameToTypeMap.get(key));
	      }
	    }
	    return meta;
    }

	public DataType getDataType(String key) {
		return nameToTypeMap.get(key);
	}
	
	public List<FieldInfo> getFieldInfoList() {
		return fieldInfoList;
	}
	
	@Override
	public String toString() {
		return "RowMeta{" +
				"fieldInfoList=" + fieldInfoList +
				'}';
	}
}
