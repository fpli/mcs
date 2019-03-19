package com.ebay.traffic.chocolate.util;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class SetUtil {

  public static HashSet<String> transferToHashSet(Set set){
    HashSet<String> hashSet = new HashSet<String>();
    Iterator<String> iterator = set.iterator();

    while(iterator.hasNext()){
      hashSet.add(iterator.next());
    }
    return hashSet;
  }

}
