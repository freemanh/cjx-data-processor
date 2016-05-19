package com.cjx.monitor.postprocessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.cjx.monitor.postprocessor.core.MonitorDataPostProcessor;
import com.surftools.BeanstalkClient.Client;
import com.surftools.BeanstalkClient.Job;
import com.surftools.BeanstalkClientImpl.Serializer;

@Configuration
@EnableAutoConfiguration
@ComponentScan
public class Application {
	private final static Logger logger = LoggerFactory.getLogger(Application.class);

	public static void main(String[] args) {
		ConfigurableApplicationContext ctx = SpringApplication.run(Application.class, args);

		MonitorDataPostProcessor proc = ctx.getBean(MonitorDataPostProcessor.class);

		Client client = ctx.getBean(Client.class);
		client.useTube("data.reading");
		while (true) {
			Job job = client.reserve(10);
			if (null == job) {
				continue;
			} else {
				String content = Serializer.byteArrayToSerializable(job.getData()).toString();
				try {
					proc.exec(content);
					client.delete(job.getJobId());
				} catch (Exception e) {
					logger.error(String.format("Failed to process job with content:%s!", content), e);
				}
			}
		}
	}
}
