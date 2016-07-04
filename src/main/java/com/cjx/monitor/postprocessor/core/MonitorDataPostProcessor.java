package com.cjx.monitor.postprocessor.core;

import java.io.IOException;
import java.text.ParseException;
import java.text.ParsePosition;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.ISO8601Utils;
import com.surftools.BeanstalkClient.Client;

@Component
public class MonitorDataPostProcessor {
	private final static Logger LOGGER = LoggerFactory
			.getLogger(MonitorDataPostProcessor.class);

	@Autowired
	private JdbcTemplate jdbc;

	@Autowired
	private Client queue;

	@Transactional
	public void exec(String content) throws JsonParseException,
			JsonMappingException, IOException, ParseException {
		LOGGER.debug("Received content:" + content);

		ObjectMapper om = new ObjectMapper();
		Map<String, Object> msg = om.readValue(content, Map.class);

		String deviceId = msg.get("deviceId").toString();
		Double reading1 = (Double) msg.get("reading1");
		Double reading2 = (Double) msg.get("reading2");
		boolean poweroff = (Boolean) msg.get("poweroff");
		// int failCount = (Integer) msg.get("failCount");
		Date date = null;
		try {
			date = ISO8601Utils.parse(msg.get("date").toString(),
					new ParsePosition(0));
		} catch (Exception e) {
			LOGGER.info(
					"Fail to parse date string:{}, it maybe stale data, just pass it.",
					msg.get("date").toString());
			return;
		}

		SensorDevice sd = jdbc
				.queryForObject(
						"select s.id as sensorId, s.name as sensorName, s.collectTime, d.support_power_alarm as supportPowerAlarm, d.status status, d.id as deviceId, s.minHumidity, s.maxHumidity, s.minTemp, s.maxTemp, s.is_over_heat overHeat, s.is_over_hum overHum, temp_revision tempRev, hum_revision as humRev from xdevice d join xsensor s on d.id=s.device_id where d.code=?",
						new BeanPropertyRowMapper<SensorDevice>(
								SensorDevice.class), deviceId);
		DeviceStatus oldDeviceStatus = DeviceStatus.values()[sd.getStatus()];

		reading1 = reading1 + sd.getTempRev();
		reading2 = reading2 + sd.getHumRev();

		String sql = String
				.format("INSERT INTO %s (`collect_time`, `device_code`, `humidity`, `sensor_index`, `temperature`, `create_time`) VALUES (?, ?, ?, ?, ?, NOW())",
						"MONDATA_" + deviceId);
		jdbc.update(sql, date, deviceId, reading2, 0, reading1);

		boolean isOverHeat = false;
		boolean isOverHum = false;

		if (null == sd.getCollectTime()
				|| date.compareTo(sd.getCollectTime()) > 0) {
			DeviceStatus newDeviceStatus = DeviceStatus.NORMAL;

			isOverHeat = reading1 < sd.getMinTemp()
					|| reading1 > sd.getMaxTemp();
			isOverHum = reading2 < sd.getMinHumidity()
					|| reading2 > sd.getMaxHumidity();

			String clearAlarmSql = "update alarm set clearTime=now() where alarm_type=? and sensor_id=?";
			if (poweroff && sd.supportPowerAlarm) {
				newDeviceStatus = DeviceStatus.POWER_OFF;

				if (!oldDeviceStatus.equals(DeviceStatus.POWER_OFF)) {
					jdbc.update(
							"insert into alarm(alarm_type, device_id, createTime, messageSent) values(?,?,now(), false)",
							AlarmType.POWEROFF.ordinal(), sd.getDeviceId());

					List<String> mobiles = queryMobileByDeviceId(sd
							.getDeviceId());
					String deviceName = jdbc.queryForObject(
							"select name from xdevice where id=?",
							String.class, sd.getDeviceId());
					queue.useTube("alarm.poweroff");
					for (String m : mobiles) {
						queue.put(
								1024,
								0,
								60,
								String.format(
										"{\"mobile\":\"%1$s\",\"device\": \"%2$s\", \"addedTime\": \"%3$s\"}",
										m, deviceName,
										ISO8601Utils.format(new Date()))
										.getBytes("utf-8"));
					}
				}

			} else {
				newDeviceStatus = DeviceStatus.NORMAL;
				jdbc.update(
						"update alarm set clearTime=now() where alarm_type=? and device_id=?",
						AlarmType.POWEROFF, sd.getDeviceId());
			}
			if (isOverHeat) {
				jdbc.update(
						"insert into alarm(alarm_type, device_id, sensor_id, max, min, reading, createTime, messageSent) values(?,?,?,?,?,?,NOW(),?)",
						AlarmType.OVER_HEAT.ordinal(), sd.getDeviceId(),
						sd.getSensorId(), sd.getMaxTemp(), sd.getMinTemp(),
						reading1, false);

				List<String> mobiles = queryMobileByDeviceId(sd.getDeviceId());
				queue.useTube("alarm.reading1");
				for (String m : mobiles) {
					queue.put(
							1024,
							0,
							60,
							String.format(
									"{\"mobile\":\"%1$s\", \"reading\": %2$f, \"sensor\":\"%3$s\", \"min\": %4$f, \"max\": %5$f, \"addedTime\": \"%6$s\"}",
									m, reading1, sd.getSensorName(),
									sd.getMinTemp(), sd.getMaxTemp(),
									ISO8601Utils.format(new Date())).getBytes(
									"utf-8"));
				}
			} else {
				jdbc.update(clearAlarmSql, AlarmType.OVER_HEAT.ordinal(),
						sd.getSensorId());
			}
			if (isOverHum) {
				jdbc.update(
						"insert into alarm(alarm_type, device_id, sensor_id, max, min, reading, createTime, messageSent) values(?,?,?,?,?,?,NOW(),?)",
						AlarmType.OVER_HUM.ordinal(), sd.getDeviceId(),
						sd.getSensorId(), sd.getMaxHumidity(),
						sd.getMinHumidity(), reading2, false);

				List<String> mobiles = queryMobileByDeviceId(sd.getDeviceId());
				queue.useTube("alarm.reading2");
				for (String m : mobiles) {
					queue.put(
							1024,
							0,
							60,
							String.format(
									"{\"mobile\":\"%1$s\", \"reading\": %2$f, \"sensor\":\"%3$s\", \"min\": %4$f, \"max\": %5$f, \"addedTime\": \"%6$s\"}",
									m, reading2, sd.getSensorName(),
									sd.getMinHumidity(), sd.getMaxHumidity(),
									ISO8601Utils.format(new Date())).getBytes(
									"utf-8"));
				}
			} else {
				jdbc.update(clearAlarmSql, AlarmType.OVER_HUM.ordinal(),
						sd.getSensorId());
			}

			jdbc.update(
					"update xsensor set temperature=?, humidity=?, collectTime=?, is_over_heat=?, is_over_hum=? where id=?",
					reading1, reading2, date, isOverHeat, isOverHum,
					sd.getSensorId());
			jdbc.update("update xdevice set status=? where id=?",
					newDeviceStatus, sd.getDeviceId());
		}

	}

	private List<String> queryMobileByDeviceId(long id) {
		return jdbc
				.queryForList(
						"SELECT distinct(alarm_phone_number) as mobile "
								+ "FROM `xcompany_alarm_phone_number` phone join `xcompany` comp  on phone.`company_id`= comp.`id` join xdevice d on d.company_id= comp.id "
								+ "where d.`id`= ? and phone.`alarm_phone_number` is not null",
						String.class, id);
	}

}
