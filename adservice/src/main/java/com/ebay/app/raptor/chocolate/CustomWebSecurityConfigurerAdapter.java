package com.ebay.app.raptor.chocolate;

import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;

/**
 * Remove X-Content-Type-Options and X-Frame-Options, same with rover
 * Use Order(1) annotation to override raptor WebSecurityConfigurerAdapter
 * @author Zhiyuan Wang
 * @since 2020/2/13
 */
@Configuration
@Order(1)
public class CustomWebSecurityConfigurerAdapter extends WebSecurityConfigurerAdapter {

  @Override
  protected void configure(HttpSecurity http) throws Exception {
    http.headers().frameOptions().disable();
    http.headers().contentTypeOptions().disable();
    http.headers().xssProtection().disable();
  }
}
