package com.Self.After.row;

public interface Coder {
	Row encoder(RowMeta rowMeta, Object o) throws NoSuchFieldException, IllegalAccessException;
	Object decoder(RowMeta rowMeta, Row row, Object object) throws NoSuchFieldException, IllegalAccessException;
}
                                    