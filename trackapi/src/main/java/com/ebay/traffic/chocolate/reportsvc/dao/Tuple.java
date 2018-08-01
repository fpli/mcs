package com.ebay.traffic.chocolate.reportsvc.dao;

public class Tuple<A, B> {
  public final A first;

  public final B second;

  public Tuple(A a, B b) {
    first = a;
    second = b;
  }

  @Override
  public String toString() {
    return "(" + first + ", " + second + ")";
  }
}
