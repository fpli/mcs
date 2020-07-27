package com.ebay.app.raptor.chocolate.eventlistener.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SearchEngineFreeListingsRotationEnumTest {

  @Test
  public void getRotation() {
    SearchEngineFreeListingsRotationEnum parse = SearchEngineFreeListingsRotationEnum.parse(186);
    assertEquals("1185-159470-979592-4", parse.getRotation());
  }

  @Test
  public void parse() {
    assertEquals(SearchEngineFreeListingsRotationEnum.ES, SearchEngineFreeListingsRotationEnum.parse(186));
  }

}