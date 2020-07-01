package com.ebay.app.raptor.chocolate;

import com.ebay.app.raptor.chocolate.adservice.configuration.DatabaseProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;

/**
 * Adservice entrance class
 * @author - xiangli4
 */
@SpringBootApplication
@EnableGlobalMethodSecurity(prePostEnabled = true, proxyTargetClass = true)
@EnableConfigurationProperties({
		DatabaseProperties.class
})
public class AdserviceApplication {

	public static void main(String[] args) {
		SpringApplication.run(AdserviceApplication.class, args);
	}
}
