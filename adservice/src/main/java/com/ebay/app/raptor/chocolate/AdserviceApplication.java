package com.ebay.app.raptor.chocolate;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;

/**
 * Event listener entrance class
 * @author - xiangli4
 */
@SpringBootApplication
@EnableGlobalMethodSecurity(prePostEnabled = true, proxyTargetClass = true)
public class AdserviceApplication {

	public static void main(String[] args) {
		SpringApplication.run(AdserviceApplication.class, args);
	}
}
