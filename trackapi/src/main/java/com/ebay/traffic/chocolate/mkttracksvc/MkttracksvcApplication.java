package com.ebay.traffic.chocolate.mkttracksvc;

import com.ebay.kernel.bean.configuration.BaseConfigBean;
import com.ebay.kernel.bean.configuration.IConfigBeanBag;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.inject.Inject;
import java.util.Collection;

@SpringBootApplication
public class MkttracksvcApplication {
  public static void main(String[] args) {
    SpringApplication.run(MkttracksvcApplication.class, args);
  }
}
