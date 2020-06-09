package com.ebay.app.raptor.chocolate;

import org.springframework.boot.web.embedded.tomcat.ConfigurableTomcatWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.core.Ordered;

import com.ebay.raptor.tomcat.autoconfigure.RaptorTomcatCustomizer;

public class WrappedRaptorTomcatCustomeizer implements WebServerFactoryCustomizer<ConfigurableTomcatWebServerFactory>, Ordered {
	
	private RaptorTomcatCustomizer customizer;
	
	WrappedRaptorTomcatCustomeizer(RaptorTomcatCustomizer customizer){
		 this.customizer = customizer;
	}

	@Override
	public int getOrder() {
		return 5000;
	}

	@Override
	public void customize(ConfigurableTomcatWebServerFactory factory) {
		customizer.customize(factory);
	}
}
