package com.Self.After.row;


import com.Self.After.platform.Platform;

public class PojoBasedCoder implements Coder{

	public PojoBasedCoder(){}

	VariableFunctions variableFunctions = new VariableFunctions();
	ByteLength length = new ByteLength();

	@Override
	public Row encoder(RowMeta rowMeta, Object o) throws NoSuchFieldException, IllegalAccessException {
		Row row = new Row();
		long offset = 0;
		int varoffset = length.getVarOffset(rowMeta);
		int size = length.getByteLength(rowMeta, o);
		row.dataBytes = new byte[size];
		
		for (FieldInfo fieldInfo : rowMeta.getFieldInfoList()) {
			switch (fieldInfo.getDataType()) {
				case STRING:
					String str = (String) o.getClass().getField(fieldInfo.getName()).get(o);
 					variableFunctions.putString(row.dataBytes, Platform.INT_ARRAY_OFFSET + offset, varoffset, str);
					offset += 8;
					varoffset +=  str.getBytes().length;
					break;
				
				case INTEGER:
					int intVal = (int) o.getClass().getField(fieldInfo.getName()).get(o);
					Platform.putInt(row.dataBytes, Platform.INT_ARRAY_OFFSET + offset, intVal);
					offset += 4;
					break;
				
				case LONG:
					long longVal = (long) o.getClass().getField(fieldInfo.getName()).get(o);
					Platform.putLong(row.dataBytes, Platform.LONG_ARRAY_OFFSET + offset, longVal);
					offset += 8;
					break;
				
				case CHARACTER:
					char charVal = (char) o.getClass().getField(fieldInfo.getName()).get(o);
					variableFunctions.putChar(row.dataBytes, Platform.BYTE_ARRAY_OFFSET + offset, charVal);
					offset += 2;
					break;
				
				case DOUBLE:
					double doubleVal = (double) o.getClass().getField(fieldInfo.getName()).get(o);
					Platform.putDouble(row.dataBytes, Platform.DOUBLE_ARRAY_OFFSET + offset, doubleVal);
					offset += 8;
					break;
				
				case BOOLEAN:
					boolean boolval = (boolean) o.getClass().getField(fieldInfo.getName()).get(o);
					Platform.putBoolean(row.dataBytes, Platform.BOOLEAN_ARRAY_OFFSET + offset, boolval);
					offset += 1;
					break;
				
				case FLOAT:
					float floatVal = (float) o.getClass().getField(fieldInfo.getName()).get(o);
					Platform.putFloat(row.dataBytes, Platform.FLOAT_ARRAY_OFFSET + offset, floatVal);
					offset += 4;
					break;
			}
		}
		return row;
	}
	
	
	@Override
	public Object decoder(RowMeta rowMeta, Row row, Object object) throws NoSuchFieldException, IllegalAccessException
	{
		int offset = 0;
		for(FieldInfo fieldInfo : rowMeta.getFieldInfoList())
		{
			switch (fieldInfo.getDataType())
			{
				case STRING:
					String strVal = variableFunctions.getString(row.dataBytes, Platform.INT_ARRAY_OFFSET + offset);
					object.getClass().getField(fieldInfo.getName()).set(object, strVal);
					offset += 8;
					break;

				case INTEGER:
					int intVal = Platform.getInt(row.dataBytes, Platform.INT_ARRAY_OFFSET + offset);
					object.getClass().getField(fieldInfo.getName()).set(object, intVal);
					offset += 4;
					break;

				case LONG:
					long longVal = Platform.getLong(row.dataBytes, Platform.LONG_ARRAY_OFFSET + offset);
					object.getClass().getField(fieldInfo.getName()).set(object, longVal);
					offset += 8;
					break;

				case CHARACTER:
					char charVal = variableFunctions.getChar(row.dataBytes, Platform.BYTE_ARRAY_OFFSET + offset);
					object.getClass().getField(fieldInfo.getName()).set(object, charVal);
					offset += 2;
					break;
				
				case DOUBLE:
					double doubleVal = Platform.getDouble(row.dataBytes, Platform.DOUBLE_ARRAY_OFFSET + offset);
					object.getClass().getField(fieldInfo.getName()).set(object, doubleVal);
					offset += 8;
					break;
				
				case BOOLEAN:
					boolean boolVal = Platform.getBoolean(row.dataBytes, Platform.BOOLEAN_ARRAY_OFFSET + offset);
					object.getClass().getField(fieldInfo.getName()).set(object, boolVal);
					offset += 8;
					break;
				
				case FLOAT:
					float floatVal = Platform.getFloat(row.dataBytes, Platform.FLOAT_ARRAY_OFFSET + offset);
					object.getClass().getField(fieldInfo.getName()).set(object, floatVal);
					break;
			}
		}

		return object;
	}
}
