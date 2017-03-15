package com.Self.After.row;

public class ByteLength
{
	public int getByteLength(RowMeta rowMeta, Object o) throws NoSuchFieldException, IllegalAccessException {
		int len = 0;
		GetFieldValues getFieldValues = new GetFieldValues();
		for(FieldInfo fieldInfo : rowMeta.getFieldInfoList())
		{
			switch (fieldInfo.getDataType())
			{
				case BOOLEAN :
					len += 1;
					break;
				case INTEGER:
					len += 4;
					break;
				case DOUBLE:
					len +=8;
					break;
				case FLOAT:
					len +=4;
					break;
				case LONG:
					len +=8;
					break;
				case CHARACTER:
					len += 2;
					break;
				case STRING:
					len += 4;
					String strValue = (String) o.getClass().getField(fieldInfo.getName()).get(o);
					len += strValue.getBytes().length;
					len += 4;
			}
		}
		return len;
	}

	public int getVarOffset(RowMeta rowMeta)
	{
		int len = 0;
		for(FieldInfo fieldInfo : rowMeta.getFieldInfoList())
		{
			switch (fieldInfo.getDataType())
			{
				case BOOLEAN :
					len += 1;
					break;
				case INTEGER:
					len += 4;
					break;
				case DOUBLE:
					len +=8;
					break;
				case FLOAT:
					len +=4;
					break;
				case LONG:
					len +=8;
					break;
				case CHARACTER:
					len += 2;
					break;
				case STRING:
					len += 4;
					len += 4;
			}
		}
		return len;
	}
}
