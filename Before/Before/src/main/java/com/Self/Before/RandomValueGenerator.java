package com.Self.Before;

import java.util.Random;

/**
 * Created by akshay on 22/12/16.
 */
public class RandomValueGenerator
{
	Random random = new Random();
	public long randomCost()
	{
		return (2 * (random.nextInt(10)+1));
	}
	
	public long randomImpressions()
	{
		return random.nextInt(5)+1;
	}
	
	public boolean randomClicks()
	{
		return random.nextBoolean();
	}
}
