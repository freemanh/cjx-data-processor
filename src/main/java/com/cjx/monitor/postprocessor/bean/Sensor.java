package com.cjx.monitor.postprocessor.bean;

import java.util.Date;

public class Sensor {
	private Date collectTime;
	private Double humidity;
	private Double temperature;
	private boolean isOverHeat;
	private boolean isOverHum;

	@Override
	public String toString() {
		return "MonitorData [collectTime=" + collectTime + ", humidity="
				+ humidity + ", temperature=" + temperature + ", isOverHeat="
				+ isOverHeat + ", isOverHum=" + isOverHum + "]";
	}

	public Date getCollectTime() {
		return collectTime;
	}

	public void setCollectTime(Date collectTime) {
		this.collectTime = collectTime;
	}

	public Double getHumidity() {
		return humidity;
	}

	public void setHumidity(Double humidity) {
		this.humidity = humidity;
	}

	public Double getTemperature() {
		return temperature;
	}

	public void setTemperature(Double temperature) {
		this.temperature = temperature;
	}

	public boolean isOverHeat() {
		return isOverHeat;
	}

	public void setOverHeat(boolean isOverHeat) {
		this.isOverHeat = isOverHeat;
	}

	public boolean isOverHum() {
		return isOverHum;
	}

	public void setOverHum(boolean isOverHum) {
		this.isOverHum = isOverHum;
	}

}
