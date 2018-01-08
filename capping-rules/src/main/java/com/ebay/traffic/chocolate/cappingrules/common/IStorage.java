package com.ebay.traffic.chocolate.cappingrules.common;

public interface IStorage<T> {
  
  public void writeToStorage(T records);
}
