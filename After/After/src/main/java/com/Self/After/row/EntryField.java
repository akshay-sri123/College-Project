package com.Self.After.row;

public class EntryField {
	Row keyRow;
	Row valRow;

	public EntryField(){}

	public EntryField(Row keyRow, Row valRow) {
		this.keyRow = keyRow;
		this.valRow = valRow;
	}
	
	public Row getKeyRow() {
		return keyRow;
	}
	
	public void setKeyRow(Row keyRow) {
		this.keyRow = keyRow;
	}
	
	public Row getValRow() {
		return valRow;
	}
	
	public void setValRow(Row valRow) {
		this.valRow = valRow;
	}

	public void set(Row keyRow, Row valRow)
	{
		this.keyRow = keyRow;
		this.valRow = valRow;
	}
	
	@Override
	public String toString() {
		return "EntryField{" +
				"keyRow=" + keyRow +
				", valRow=" + valRow +
				'}';
	}
}
