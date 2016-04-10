package com.cjx.monitor.postprocessor.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.surftools.BeanstalkClient.Client;
import com.surftools.BeanstalkClientImpl.ClientImpl;

@Configuration
public class PostProcessorConfig {
	@Bean
	public Client beanstalkdClient(@Value("${beanstalkd.host}") String host, @Value("${beanstalkd.port}") int port) {
		return new ClientImpl(host, port);
	}
}
