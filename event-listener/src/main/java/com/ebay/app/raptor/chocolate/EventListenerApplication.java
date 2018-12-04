package com.ebay.app.raptor.chocolate;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;

/**
 * Event listener entrance class
 * @author - xiangli4
 */
@SpringBootApplication
@EnableGlobalMethodSecurity(prePostEnabled = true)
public class EventListenerApplication {

	public static void main(String[] args) {
		SpringApplication.run(EventListenerApplication.class, args);
	}
}
