package com.cjx.monitor.postprocessor.core;

import java.util.Date;

public class SensorDevice {
	long sensorId;
	Date collectTime;
	double tempRev;
	double humRev;
	boolean supportPowerAlarm;
	int status;
	long deviceId;
	double minHumidity;
	double maxHumidity;
	double minTemp;
	double maxTemp;
	boolean overHeat;
	boolean overHum;
	boolean synced;
	int uploadFrequency;
	private String sensorName;

	public SensorDevice() {
		super();
	}

	public long getSensorId() {
		return sensorId;
	}

	public void setSensorId(long sensorId) {
		this.sensorId = sensorId;
	}

	public Date getCollectTime() {
		return collectTime;
	}

	public void setCollectTime(Date collectTime) {
		this.collectTime = collectTime;
	}

	public double getTempRev() {
		return tempRev;
	}

	public void setTempRev(double tempRev) {
		this.tempRev = tempRev;
	}

	public double getHumRev() {
		return humRev;
	}

	public void setHumRev(double humRev) {
		this.humRev = humRev;
	}

	public boolean isSupportPowerAlarm() {
		return supportPowerAlarm;
	}

	public void setSupportPowerAlarm(boolean supportPowerAlarm) {
		this.supportPowerAlarm = supportPowerAlarm;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public long getDeviceId() {
		return deviceId;
	}

	public void setDeviceId(long deviceId) {
		this.deviceId = deviceId;
	}

	public double getMinHumidity() {
		return minHumidity;
	}

	public void setMinHumidity(double minHumidity) {
		this.minHumidity = minHumidity;
	}

	public double getMaxHumidity() {
		return maxHumidity;
	}

	public void setMaxHumidity(double maxHumidity) {
		this.maxHumidity = maxHumidity;
	}

	public double getMinTemp() {
		return minTemp;
	}

	public void setMinTemp(double minTemp) {
		this.minTemp = minTemp;
	}

	public double getMaxTemp() {
		return maxTemp;
	}

	public void setMaxTemp(double maxTemp) {
		this.maxTemp = maxTemp;
	}

	public boolean isOverHeat() {
		return overHeat;
	}

	public void setOverHeat(boolean overHeat) {
		this.overHeat = overHeat;
	}

	public boolean isOverHum() {
		return overHum;
	}

	public void setOverHum(boolean overHum) {
		this.overHum = overHum;
	}

	public boolean isSynced() {
		return synced;
	}

	public void setSynced(boolean synced) {
		this.synced = synced;
	}

	public int getUploadFrequency() {
		return uploadFrequency;
	}

	public void setUploadFrequency(int uploadFrequency) {
		this.uploadFrequency = uploadFrequency;
	}

	public String getSensorName() {
		return sensorName;
	}

	public void setSensorName(String sensorName) {
		this.sensorName = sensorName;
	}

}
