package com.Self.After.other;

import java.util.Random;

public class RandomEnumGenerator<E extends Enum>
{
	protected static final Random random = new Random();
	protected final E[] values;
	
	public RandomEnumGenerator(Class<E> token) {
		values = token.getEnumConstants();
	}
	
	public E random() {
		return values[random.nextInt(values.length)];
	}

}
