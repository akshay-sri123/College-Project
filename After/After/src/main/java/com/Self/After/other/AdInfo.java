package com.Self.After.other;

public class AdInfo
{
	public String publisher;
	public String advertiser;
	public String location;
	
	public long cost;
	public long impressions;
	public boolean clicks;
	
	
	public AdInfo(){}
	
	public AdInfo(String publisher, String advertiser, String location, long cost, long impressions, boolean clicks) {
		this.publisher = publisher;
		this.advertiser = advertiser;
		this.location = location;
		this.cost = cost;
		this.impressions = impressions;
		this.clicks = clicks;
	}

	public String getPublisher() {
		return publisher;
	}

	public void setPublisher(String publisher) {
		this.publisher = publisher;
	}

	public String getAdvertiser() {
		return advertiser;
	}

	public void setAdvertiser(String advertiser) {
		this.advertiser = advertiser;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public long getCost() {
		return cost;
	}

	public void setCost(long cost) {
		this.cost = cost;
	}

	public long getImpressions() {
		return impressions;
	}

	public void setImpressions(long impressions) {
		this.impressions = impressions;
	}

	public boolean isClicks() {
		return clicks;
	}

	public void setClicks(boolean clicks) {
		this.clicks = clicks;
	}
	
	@Override
	public String toString() {
		return publisher +
				"," + advertiser +
				"," + location +
				"," + cost +
				"," + impressions +
				"," + clicks;
	}
}

