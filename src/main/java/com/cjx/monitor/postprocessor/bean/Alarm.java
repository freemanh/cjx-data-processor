package com.cjx.monitor.postprocessor.bean;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

import org.springframework.jdbc.core.RowMapper;

import com.cjx.monitor.postprocessor.core.AlarmType;

public class Alarm implements RowMapper<Alarm> {
	private AlarmType type;
	private String deviceId;
	private String sensorId;
	private Double max;
	private Double min;
	private Double reading;
	private Date clearTime;
	private Date createTime;
	private boolean messageSent;

	@Override
	public String toString() {
		return "Alarm [type=" + type + ", deviceId=" + deviceId + ", sensorId="
				+ sensorId + ", max=" + max + ", min=" + min + ", reading="
				+ reading + ", clearTime=" + clearTime + ", createTime="
				+ createTime + ", messageSent=" + messageSent + "]";
	}

	public AlarmType getType() {
		return type;
	}

	public void setType(AlarmType type) {
		this.type = type;
	}

	public String getDeviceId() {
		return deviceId;
	}

	public void setDeviceId(String deviceId) {
		this.deviceId = deviceId;
	}

	public String getSensorId() {
		return sensorId;
	}

	public void setSensorId(String sensorId) {
		this.sensorId = sensorId;
	}

	public Double getMax() {
		return max;
	}

	public void setMax(Double max) {
		this.max = max;
	}

	public Double getMin() {
		return min;
	}

	public void setMin(Double min) {
		this.min = min;
	}

	public Double getReading() {
		return reading;
	}

	public void setReading(Double reading) {
		this.reading = reading;
	}

	public Date getClearTime() {
		return clearTime;
	}

	public void setClearTime(Date clearTime) {
		this.clearTime = clearTime;
	}

	public Date getCreateTime() {
		return createTime;
	}

	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}

	public boolean isMessageSent() {
		return messageSent;
	}

	public void setMessageSent(boolean messageSent) {
		this.messageSent = messageSent;
	}

	@Override
	public Alarm mapRow(ResultSet rs, int rowNum) throws SQLException {
		Alarm alarm = new Alarm();
		alarm.setClearTime(rs.getTimestamp("clearTime"));
		alarm.setCreateTime(rs.getTimestamp("createTime"));
		alarm.setDeviceId(rs.getString("device_id"));
		alarm.setSensorId(rs.getString("sensor_id"));
		alarm.setMax(rs.getDouble("max"));
		alarm.setMin(rs.getDouble("min"));
		alarm.setMessageSent(rs.getBoolean("messageSent"));
		alarm.setReading(rs.getDouble("reading"));
		alarm.setType(AlarmType.values()[rs.getInt("alarm_type")]);
		return alarm;
	}

}
