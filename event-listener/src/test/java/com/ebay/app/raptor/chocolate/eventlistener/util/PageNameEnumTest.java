package com.ebay.app.raptor.chocolate.eventlistener.util;

import org.junit.Test;

import static org.junit.Assert.*;

public class PageNameEnumTest {

  @Test
  public void getName() {
    assertEquals("Click", PageNameEnum.CLICK.getName());
    assertEquals("Impression", PageNameEnum.IMPRESSION.getName());
    assertEquals("Open", PageNameEnum.OPEN.getName());
    assertEquals("AdRequest", PageNameEnum.ADREQUEST.getName());
    assertEquals("ThirdParty_Click", PageNameEnum.THIRDPARTY_CLICK.getName());
    assertEquals("Roi", PageNameEnum.ROI.getName());
    assertEquals("Chocolate_Click_Bot", PageNameEnum.CHOCOLATE_CLICK_BOT.getName());
    assertEquals("Rover_Click", PageNameEnum.ROVER_CLICK.getName());
    assertEquals("Rover_Impression", PageNameEnum.ROVER_IMPRESSION.getName());
    assertEquals("Rover_Open", PageNameEnum.ROVER_OPEN.getName());
    assertEquals("Rover_AdRequest", PageNameEnum.ROVER_ADREQUEST.getName());
    assertEquals("Rover_ThirdParty_Click", PageNameEnum.ROVER_THIRDPARTY_CLICK.getName());
    assertEquals("Rover_Click_Bot", PageNameEnum.CLICK_BOT.getName());
    assertEquals("Rover_Open_Bot", PageNameEnum.ROVER_OPEN_BOT.getName());
  }
}