package com.Self.After.row;

import java.lang.reflect.Field;

public class GetFieldValues {
	public String getString(Object o, String fieldName) throws NoSuchFieldException, IllegalAccessException {
		Field f = o.getClass().getField(fieldName);
		return (String) f.get(o);
	}
	
	public int getInteger(Object o, String fieldName) throws NoSuchFieldException, IllegalAccessException {
		Field f = o.getClass().getField(fieldName);
		return (Integer) f.get(o);
	}
	
	public boolean getBoolean(Object o, String fieldName) throws IllegalAccessException, NoSuchFieldException {
		Field f = o.getClass().getField(fieldName);
		return (boolean) f.get(o);
	}
	
	public long getLong(Object o, String fieldName) throws NoSuchFieldException, IllegalAccessException {
		Field f = o.getClass().getField(fieldName);
		return (long) f.get(o);
	}
	
	public char getCharacter(Object o, String fieldName) throws NoSuchFieldException, IllegalAccessException {
		Field f = o.getClass().getField(fieldName);
		return (char) f.get(o);
	}
}
