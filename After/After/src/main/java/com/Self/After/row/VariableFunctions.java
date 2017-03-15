package com.Self.After.row;


import com.Self.After.platform.Platform;

public class VariableFunctions
{
	public static void putString(Object object, long offset, int varoffset, String value)
	{
		Platform.putInt(object, offset, varoffset);
		
		Platform.putInt(object, offset+4, value.length());
		
		byte[] bytestr = value.getBytes();
		
		for(int i=0;i<bytestr.length;i++)
		{
			Platform.putByte(object, Platform.BYTE_ARRAY_OFFSET + varoffset + i, bytestr[i]);
		}
	}
	
	public static String getString(Object object, long offset)
	{
		StringBuilder stringBuilder = new StringBuilder("");
		int varoffset = Platform.getInt(object, offset);
		
		int len = Platform.getInt(object, offset + 4);
		
		for(int i=0;i<len;i++)
		{
			stringBuilder.append((char) Platform.getByte(object, Platform.BYTE_ARRAY_OFFSET + varoffset + i));
		}
		
		return stringBuilder.toString();
	}
	
	public static void putChar(Object object, long offset, char value)
	{
		byte charByte = (byte) value;
		Platform.putByte(object, offset, charByte);
	}
	
	public static char getChar(Object object, long offset)
	{
		char value = (char) Platform.getByte(object, offset);
		return value;
	}

}
