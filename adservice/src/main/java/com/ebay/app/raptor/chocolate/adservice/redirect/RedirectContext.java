package com.ebay.app.raptor.chocolate.adservice.redirect;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class RedirectContext {
  private RedirectStrategy strategy;

  public RedirectContext(RedirectStrategy strategy){
    this.strategy = strategy;
  }

  public void execute(HttpServletRequest request, HttpServletResponse response) throws IOException {
      strategy.process(request, response);
  }
}
