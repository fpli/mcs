package com.ebay.app.raptor.chocolate;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.stereotype.Component;

import com.ebay.raptor.tomcat.autoconfigure.RaptorTomcatCustomizer;

@Component
public class CustomizerPostProcess implements BeanPostProcessor {

	public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
		if(beanName.equalsIgnoreCase("RaptorTomcatCustomizer")) {
			if (bean instanceof RaptorTomcatCustomizer) {
				RaptorTomcatCustomizer customizer = (RaptorTomcatCustomizer) bean;
				return new WrappedRaptorTomcatCustomeizer(customizer);
			}
		}
		
		return bean;
	}
	
	
}
