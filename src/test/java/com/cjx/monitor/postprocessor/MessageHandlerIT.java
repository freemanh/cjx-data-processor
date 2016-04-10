package com.cjx.monitor.postprocessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.text.DateFormat;
import java.util.Date;
import java.util.Map;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.jdbc.Sql;

import com.cjx.monitor.postprocessor.core.AlarmType;
import com.cjx.monitor.postprocessor.core.DeviceStatus;
import com.cjx.monitor.postprocessor.core.MonitorDataPostProcessor;
import com.fasterxml.jackson.databind.util.ISO8601DateFormat;

@Sql("/sql/data.sql")
public class MessageHandlerIT extends BaseIT {

	@Autowired
	MonitorDataPostProcessor handler;

	DateFormat dateFormat = new ISO8601DateFormat();

	@Test
	public void testNormal() throws Exception {
		// given
		String content = String.format("{\"deviceId\":\"Jingsu66011\", \"reading1\":10.0, \"reading2\":20.0, \"poweroff\" : false, \"failCount\" :0, \"date\": \"%s\"}",
				dateFormat.format(new Date()));
		System.out.println(content);

		// when
		handler.exec(content);

		// then
		assertEquals(1, countRowsInTable("mondata_jingsu66011"));
		assertEquals(0, countRowsInTable("alarm"));

		Map<String, Object> data = jdbcTemplate
				.queryForMap("select temperature temp, humidity, collect_time collectTime, create_time createTime from mondata_jingsu66011 where device_code='Jingsu66011'");
		assertEquals(9.0, new Double(data.get("temp").toString()), 0.1);
		assertEquals(10.0, new Double(data.get("humidity").toString()), 0.1);
		assertNotNull(data.get("collectTime"));
		assertNotNull(data.get("createTime"));

		Map<String, Object> sensor = jdbcTemplate.queryForMap("select collectTime, humidity, temperature temp, is_over_heat, is_over_hum from xsensor where device_id=1");
		assertNotNull(sensor.get("collectTime"));
		assertEquals(9.0, new Double(sensor.get("temp").toString()), 0.1);
		assertEquals(10.0, new Double(sensor.get("humidity").toString()), 0.1);
	}

	@Test
	public void testOldData() throws Exception {
		// given
		String content = String
				.format("{\"deviceId\":\"Jingsu66011\", \"reading1\":10.0, \"reading2\":20.0, \"poweroff\" : false, \"failCount\" :0, \"date\": \"2015-11-29T21:59:00+08:00\"}",
						new Date());

		// when
		handler.exec(content);

		// then
		assertEquals(1, countRowsInTable("mondata_jingsu66011"));

		Map<String, Object> sensor = jdbcTemplate.queryForMap("select temperature temp, humidity hum from xsensor where device_id=1");
		assertNull(sensor.get("temp"));
		assertNull(sensor.get("hum"));
	}

	@Test
	public void testFirstPoweroff() throws Exception {
		// given
		String content = String.format("{\"deviceId\":\"Jingsu66011\", \"reading1\":10.0, \"reading2\":20.0, \"poweroff\" : true, \"failCount\" :0, \"date\": \"%s\"}",
				dateFormat.format(new Date()));

		// when
		handler.exec(content);

		// then
		assertEquals(1, countRowsInTable("mondata_jingsu66011"));

		Map<String, Object> alarm = jdbcTemplate.queryForMap("select alarm_type type, messageSent sent from alarm where device_id=1");
		assertEquals(AlarmType.POWEROFF.ordinal(), Integer.valueOf(alarm.get("type").toString()).intValue());
		assertFalse((Boolean) alarm.get("sent"));

		Map<String, Object> device = jdbcTemplate.queryForMap("select status from xdevice where id=1");
		assertEquals(DeviceStatus.POWER_OFF.ordinal(), Integer.valueOf(device.get("status").toString()).intValue());
	}

	@Test
	public void testSecondPoweroff() throws Exception {
		// given
		String content = String.format("{\"deviceId\":\"Jingsu66011\", \"reading1\":10.0, \"reading2\":20.0, \"poweroff\" : true, \"failCount\" :0, \"date\": \"%s\"}",
				dateFormat.format(new Date()));

		jdbcTemplate.update("update xdevice set status=? where id=1", DeviceStatus.POWER_OFF.ordinal());

		// when
		handler.exec(content);

		// then
		assertEquals(1, countRowsInTable("mondata_jingsu66011"));
		assertEquals(0, countRowsInTable("alarm"));
	}

	@Test
	public void testPowerback() throws Exception {
		// given
		String content = String.format("{\"deviceId\":\"Jingsu66011\", \"reading1\":10.0, \"reading2\":20.0, \"poweroff\" : false, \"failCount\" :0, \"date\": \"%s\"}",
				dateFormat.format(new Date()));

		jdbcTemplate.update("update xdevice set status=? where id=1", DeviceStatus.POWER_OFF.ordinal());

		// when
		handler.exec(content);
		// then
		assertEquals(1, countRowsInTable("mondata_jingsu66011"));
		assertEquals(0, countRowsInTable("alarm"));
		Map<String, Object> device = jdbcTemplate.queryForMap("select status from xdevice where id=1");
		assertEquals(DeviceStatus.NORMAL.ordinal(), Integer.valueOf(device.get("status").toString()).intValue());
	}

	@Test
	public void testLowTemp() throws Exception {
		// given
		String content = String.format("{\"deviceId\":\"Jingsu66011\", \"reading1\":5.9, \"reading2\":20.0, \"poweroff\" : false, \"failCount\" :0, \"date\": \"%s\"}",
				dateFormat.format(new Date()));

		// when
		handler.exec(content);

		// then
		assertEquals(1, countRowsInTable("mondata_jingsu66011"));

		Map<String, Object> alarm = jdbcTemplate.queryForMap("select alarm_type type, max, min, reading, clearTime, createTime, messageSent from alarm where sensor_id=100");
		assertEquals(AlarmType.OVER_HEAT.ordinal(), Integer.parseInt(alarm.get("type").toString()));
		assertEquals(50.0, new Double(alarm.get("max").toString()), 0.1);
		assertEquals(5.0, new Double(alarm.get("min").toString()), 0.1);
		assertEquals(4.9, new Double(alarm.get("reading").toString()), 0.1);
		assertNull(alarm.get("clearTime"));
		assertNotNull(alarm.get("createTime"));
		assertFalse((Boolean) alarm.get("messageSent"));
	}

	@Test
	public void testLowHum() throws Exception {
		// given
		String content = String.format("{\"deviceId\":\"Jingsu66011\", \"reading1\":6.0, \"reading2\":19.0, \"poweroff\" : false, \"failCount\" :0, \"date\": \"%s\"}",
				dateFormat.format(new Date()));

		// when
		handler.exec(content);

		// then
		assertEquals(1, countRowsInTable("mondata_jingsu66011"));

		Map<String, Object> alarm = jdbcTemplate.queryForMap("select alarm_type type, max, min, reading, clearTime, createTime, messageSent from alarm where sensor_id=100");
		assertEquals(AlarmType.OVER_HUM.ordinal(), Integer.parseInt(alarm.get("type").toString()));
		assertEquals(100.0, new Double(alarm.get("max").toString()), 0.1);
		assertEquals(10.0, new Double(alarm.get("min").toString()), 0.1);
		assertEquals(9.0, new Double(alarm.get("reading").toString()), 0.1);
		assertNull(alarm.get("clearTime"));
		assertNotNull(alarm.get("createTime"));
		assertFalse((Boolean) alarm.get("messageSent"));
	}

	@Test
	public void testLowTempAndHum() throws Exception {
		// given
		String content = String.format("{\"deviceId\":\"Jingsu66011\", \"reading1\":5.9, \"reading2\":19.0, \"poweroff\" : false, \"failCount\" :0, \"date\": \"%s\"}",
				dateFormat.format(new Date()));

		// when
		handler.exec(content);

		// then
		assertEquals(1, countRowsInTable("mondata_jingsu66011"));
		assertEquals(2, countRowsInTable("alarm"));
	}
}
