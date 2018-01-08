package com.ebay.traffic.chocolate.cappingrules.common;

import java.io.Serializable;

public interface IStorage<T> extends Serializable {
  
  public void writeToStorage(T records);
}
