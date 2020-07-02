package com.ebay.app.raptor.chocolate.filter.service;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class EmptyQueue<E> extends ArrayBlockingQueue<E> {

  EmptyQueue() {
    super(1);
  }

  public boolean add(E e) {
    return true;
  }

  public void put(E e) throws InterruptedException {
  }

  public E take() throws InterruptedException {
    return null;
  }

  public E poll() {
    return null;
  }

  public E poll(long timeout, TimeUnit unit)
          throws InterruptedException {
    return null;
  }
}
