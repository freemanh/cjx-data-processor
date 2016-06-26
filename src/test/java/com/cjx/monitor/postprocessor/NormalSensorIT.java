package com.cjx.monitor.postprocessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.text.DateFormat;
import java.util.Date;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import com.cjx.monitor.postprocessor.bean.Alarm;
import com.cjx.monitor.postprocessor.bean.Sensor;
import com.cjx.monitor.postprocessor.core.AlarmType;
import com.cjx.monitor.postprocessor.core.DeviceStatus;
import com.cjx.monitor.postprocessor.core.MonitorDataPostProcessor;
import com.fasterxml.jackson.databind.util.ISO8601DateFormat;

public class NormalSensorIT extends BaseIT {

	@Autowired
	MonitorDataPostProcessor handler;

	DateFormat dateFormat = new ISO8601DateFormat();

	@Before
	public void setup() {
		jdbcTemplate
				.execute("CREATE TABLE IF NOT EXISTS MONDATA_Jingsu66011 LIKE mondata_temp");

		jdbcTemplate
				.update("insert into xcompany(id, name) values(1, 'test_company')");
		jdbcTemplate
				.update("insert into xdevice(id, name, company_id, code, status, support_power_alarm) values(1, 'test_device', 1, 'Jingsu66011', '1', 1)");

		double maxHumidity = 100;
		double minHumidity = 10;
		double maxTemp = 50;
		double minTemp = 5;
		double humRevision = -10;
		double tempRevision = -1;
		jdbcTemplate
				.update("insert into xsensor(id, maxHumidity, minHumidity, maxTemp, minTemp, name, device_id, status, sensor_index, hum_revision, temp_revision, upload_frequency, is_synced, collectTime)"
						+ " values(100, ?, ?, ?, ?, 'test_sensor_name', 1, '0', 0, ?, ?, 1, 1, '2015-11-29 22:00:00')",
						maxHumidity, minHumidity, maxTemp, minTemp,
						humRevision, tempRevision);
	}

	@Test
	public void testNormalData() throws Exception {
		// given
		String content = String
				.format("{\"deviceId\":\"Jingsu66011\", \"reading1\":10.0, \"reading2\":20.0, \"poweroff\" : false, \"failCount\" :0, \"date\": \"%s\"}",
						dateFormat.format(new Date()));

		// when
		handler.exec(content);

		// then
		assertEquals(1, countRowsInTable("MONDATA_Jingsu66011"));
		assertEquals(0, countRowsInTable("alarm"));

		Map<String, Object> data = jdbcTemplate
				.queryForMap("select temperature temp, humidity, collect_time collectTime, create_time createTime from MONDATA_Jingsu66011 where device_code='Jingsu66011'");
		assertEquals(9.0, new Double(data.get("temp").toString()), 0.1);
		assertEquals(10.0, new Double(data.get("humidity").toString()), 0.1);
		assertNotNull(data.get("collectTime"));
		assertNotNull(data.get("createTime"));

		Sensor sensor = jdbcTemplate
				.queryForObject(
						"select collectTime, humidity, temperature, is_over_heat isOverHeat, is_over_hum isOverHum from xsensor where device_id=1",
						new BeanPropertyRowMapper<Sensor>(Sensor.class));
		assertNotNull(sensor.getCollectTime());
		assertEquals(9.0, sensor.getTemperature(), 0.1);
		assertEquals(10.0, sensor.getHumidity(), 0.1);
		assertFalse(sensor.isOverHeat());
		assertFalse(sensor.isOverHum());
	}

	@Test
	public void oldData_addHistoricalData_notUpdateSensorData()
			throws Exception {
		// given
		String content = String
				.format("{\"deviceId\":\"Jingsu66011\", \"reading1\":10.0, \"reading2\":20.0, \"poweroff\" : false, \"failCount\" :1, \"date\": \"2015-11-29T21:59:00+08:00\"}");

		// when
		handler.exec(content);

		// then
		assertEquals(1, countRowsInTable("MONDATA_Jingsu66011"));

		Sensor sensor = jdbcTemplate.queryForObject(
				"select temperature, humidity from xsensor where device_id=1",
				new BeanPropertyRowMapper<Sensor>(Sensor.class));
		assertNull(sensor.getTemperature());
		assertNull(sensor.getHumidity());
	}

	@Test
	public void firstPowerOff_addHistoricalData_addPowerOffAlarm()
			throws Exception {
		// given
		String content = String
				.format("{\"deviceId\":\"Jingsu66011\", \"reading1\":10.0, \"reading2\":20.0, \"poweroff\" : true, \"failCount\" :0, \"date\": \"%s\"}",
						dateFormat.format(new Date()));

		// when
		handler.exec(content);

		// then
		assertEquals(1, countRowsInTable("MONDATA_Jingsu66011"));

		Alarm alarm = jdbcTemplate.queryForObject(
				"select * from alarm where device_id=1", new Alarm());
		assertEquals(AlarmType.POWEROFF, alarm.getType());
		assertFalse(alarm.isMessageSent());

		Map<String, Object> device = jdbcTemplate
				.queryForMap("select status from xdevice where id=1");
		assertEquals(DeviceStatus.POWER_OFF.ordinal(),
				Integer.valueOf(device.get("status").toString()).intValue());
	}

	@Test
	public void secondPoweroff_noNewAlarm() throws Exception {
		// given
		jdbcTemplate
				.update("INSERT INTO alarm(alarm_type, device_id, createTime, messageSent) VALUES(?,?,?,?)",
						AlarmType.POWEROFF.ordinal(), 1, new Date(), false);

		String content = String
				.format("{\"deviceId\":\"Jingsu66011\", \"reading1\":10.0, \"reading2\":20.0, \"poweroff\":true, \"failCount\" :0, \"date\": \"%s\"}",
						dateFormat.format(new Date()));

		jdbcTemplate.update("update xdevice set status=? where id=1",
				DeviceStatus.POWER_OFF.ordinal());

		// when
		handler.exec(content);

		// then
		assertEquals(1, countRowsInTable("MONDATA_Jingsu66011"));
		assertEquals(1, countRowsInTable("alarm"));
	}

	@Test
	public void testPowerback_clearAlarm() throws Exception {
		// given
		jdbcTemplate
				.update("INSERT INTO alarm(alarm_type, device_id, createTime, messageSent) VALUES(?,?,?,?)",
						AlarmType.POWEROFF.ordinal(), 1, new Date(), false);
		jdbcTemplate.update("update xdevice set status=? where id=1",
				DeviceStatus.POWER_OFF.ordinal());

		String content = String
				.format("{\"deviceId\":\"Jingsu66011\", \"reading1\":10.0, \"reading2\":20.0, \"poweroff\" : false, \"failCount\" :0, \"date\": \"%s\"}",
						dateFormat.format(new Date()));

		// when
		handler.exec(content);

		// then
		assertEquals(1, countRowsInTable("MONDATA_Jingsu66011"));

		assertEquals(1, countRowsInTable("alarm"));
		assertEquals(0, countRowsInTableWhere("alarm", "clearTime is not null"));

		int deviceStatus = jdbcTemplate.queryForObject(
				"select status from xdevice where id=1", Integer.class);
		assertEquals(DeviceStatus.NORMAL.ordinal(), deviceStatus);
	}

	@Test
	public void testLowTemp_addAlarm() throws Exception {
		// given
		String content = String
				.format("{\"deviceId\":\"Jingsu66011\", \"reading1\":5.9, \"reading2\":20.0, \"poweroff\" : false, \"failCount\" :0, \"date\": \"%s\"}",
						dateFormat.format(new Date()));

		// when
		handler.exec(content);

		// then
		assertEquals(1, countRowsInTable("MONDATA_Jingsu66011"));

		Alarm alarm = jdbcTemplate.queryForObject(
				"select * from alarm where sensor_id=100", new Alarm());
		assertEquals(AlarmType.OVER_HEAT, alarm.getType());
		assertEquals(50.0, alarm.getMax(), 0.1);
		assertEquals(5.0, alarm.getMin(), 0.1);
		assertEquals(4.9, alarm.getReading(), 0.1);
		assertNull(alarm.getClearTime());
		assertNotNull(alarm.getCreateTime());
		assertFalse(alarm.isMessageSent());
	}

	@Test
	public void testLowHum_addAlarm() throws Exception {
		// given
		String content = String
				.format("{\"deviceId\":\"Jingsu66011\", \"reading1\":6.0, \"reading2\":19.0, \"poweroff\" : false, \"failCount\" :0, \"date\": \"%s\"}",
						dateFormat.format(new Date()));

		// when
		handler.exec(content);

		// then
		assertEquals(1, countRowsInTable("MONDATA_Jingsu66011"));

		Alarm alarm = jdbcTemplate.queryForObject(
				"select * from alarm where sensor_id=100", new Alarm());
		assertEquals(AlarmType.OVER_HUM, alarm.getType());
		assertEquals(100.0, alarm.getMax(), 0.1);
		assertEquals(10.0, alarm.getMin(), 0.1);
		assertEquals(9.0, alarm.getReading(), 0.1);
		assertNull(alarm.getClearTime());
		assertNotNull(alarm.getCreateTime());
		assertFalse(alarm.isMessageSent());
	}

	@Test
	public void testLowTempAndHum() throws Exception {
		// given
		String content = String
				.format("{\"deviceId\":\"Jingsu66011\", \"reading1\":5.9, \"reading2\":19.0, \"poweroff\" : false, \"failCount\" :0, \"date\": \"%s\"}",
						dateFormat.format(new Date()));

		// when
		handler.exec(content);

		// then
		assertEquals(1, countRowsInTable("MONDATA_Jingsu66011"));
		assertEquals(2, countRowsInTableWhere("alarm", "clearTime is null"));
	}
}
