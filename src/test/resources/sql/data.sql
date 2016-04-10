insert into xcompany(id, name) values(1, 'test_company');

insert into xdevice(id, name, company_id, code, status, support_power_alarm) values(
1, 'test_device', 1, 'Jingsu66011', '0', 1);

insert into xsensor(id, maxHumidity, minHumidity, maxTemp, minTemp, name, device_id, status, sensor_index, hum_revision, temp_revision, upload_frequency, is_synced, collectTime) values
(100, 100, 10, 50, 5, 'test_sensor_name', 1, '0', 0, -10, -1, 1, 1, '2015-11-29 22:00:00')