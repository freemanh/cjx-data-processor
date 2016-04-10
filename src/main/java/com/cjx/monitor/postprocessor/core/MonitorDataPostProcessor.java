package com.cjx.monitor.postprocessor.core;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class MonitorDataPostProcessor {
	@Autowired
	private JdbcTemplate jdbc;

	@Transactional
	public void exec(String content) throws JsonParseException, JsonMappingException, IOException, ParseException {
		ObjectMapper om = new ObjectMapper();
		Map<String, Object> msg = om.readValue(content, Map.class);

		String deviceId = msg.get("deviceId").toString();
		Double reading1 = (Double) msg.get("reading1");
		Double reading2 = (Double) msg.get("reading2");
		boolean poweroff = (Boolean) msg.get("poweroff");
		// int failCount = (Integer) msg.get("failCount");
		Date date = new Date(Long.valueOf(msg.get("date").toString()));

		SensorDevice sd = jdbc
				.queryForObject(
						"select s.id as sensorId, s.collectTime, d.support_power_alarm as supportPowerAlarm, d.status status, d.id as deviceId, s.minHumidity, s.maxHumidity, s.minTemp, s.maxTemp, s.is_over_heat overHeat, s.is_over_hum overHum, temp_revision tempRev, hum_revision as humRev from xdevice d join xsensor s on d.id=s.device_id where d.code=?",
						new BeanPropertyRowMapper<SensorDevice>(SensorDevice.class), deviceId);
		DeviceStatus oldDeviceStatus = DeviceStatus.values()[sd.getStatus()];

		reading1 = reading1 + sd.getTempRev();
		reading2 = reading2 + sd.getHumRev();

		String sql = String.format("INSERT INTO %s (`collect_time`, `device_code`, `humidity`, `sensor_index`, `temperature`, `create_time`) VALUES (?, ?, ?, ?, ?, NOW())",
				"MONDATA_" + deviceId);
		jdbc.update(sql, date, deviceId, reading2, 0, reading1);
		if (null == sd.getCollectTime() || date.compareTo(sd.getCollectTime()) > 0) {
			boolean isOverHeat = reading1 < sd.getMinTemp() || reading1 > sd.getMaxTemp();
			boolean isOverHum = reading2 < sd.getMinHumidity() || reading2 > sd.getMaxHumidity();

			DeviceStatus newDeviceStatus = DeviceStatus.NORMAL;
			if (poweroff && sd.supportPowerAlarm && !oldDeviceStatus.equals(DeviceStatus.POWER_OFF)) {
				jdbc.update("insert into alarm(alarm_type, device_id, createTime, messageSent) values(?,?,now(), false)", AlarmType.POWEROFF.ordinal(), sd.getDeviceId());
				newDeviceStatus = DeviceStatus.POWER_OFF;
			}
			String clearAlarmSql = "update alarm set clearTime=now() where alarm_type=? and sensor_id=?";
			if (isOverHeat) {
				jdbc.update("insert into alarm(alarm_type, device_id, sensor_id, max, min, reading, createTime, messageSent) values(?,?,?,?,?,?,NOW(),?)",
						AlarmType.OVER_HEAT.ordinal(), sd.getDeviceId(), sd.getSensorId(), sd.getMaxTemp(), sd.getMinTemp(), reading1, false);
			} else {
				jdbc.update(clearAlarmSql, AlarmType.OVER_HEAT.ordinal(), sd.getSensorId());
			}
			if (isOverHum) {
				jdbc.update("insert into alarm(alarm_type, device_id, sensor_id, max, min, reading, createTime, messageSent) values(?,?,?,?,?,?,NOW(),?)",
						AlarmType.OVER_HUM.ordinal(), sd.getDeviceId(), sd.getSensorId(), sd.getMaxHumidity(), sd.getMinHumidity(), reading2, false);
			} else {
				jdbc.update(clearAlarmSql, AlarmType.OVER_HUM.ordinal(), sd.getSensorId());
			}

			jdbc.update("update xsensor set temperature=?, humidity=?, collectTime=?, is_over_heat=?, is_over_hum=? where id=?", reading1, reading2, date, isOverHeat, isOverHum,
					sd.getSensorId());
			jdbc.update("update xdevice set status=? where id=?", newDeviceStatus.ordinal(), sd.getDeviceId());
		}

	}
}
