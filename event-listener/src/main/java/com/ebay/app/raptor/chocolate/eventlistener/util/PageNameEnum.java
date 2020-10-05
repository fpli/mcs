package com.ebay.app.raptor.chocolate.eventlistener.util;

/**
 * Created by jialili1 on 8/11/20
 *
 * Page names in unified pipeline
 */
public enum PageNameEnum {
  // click
  CLICK("Click"),
  // impression
  IMPRESSION("Impression"),
  // open
  OPEN("Open"),
  // adrequest
  ADREQUEST("AdRequest"),
  // third party click
  THIRDPARTY_CLICK("ThirdParty_Click"),
  // roi
  ROI("Roi"),
  // chocolate click bot
  CHOCOLATE_CLICK_BOT("Chocolate_Click_Bot"),
  // rover click
  ROVER_CLICK("Rover_Click"),
  // rover impression
  ROVER_IMPRESSION("Rover_Impression"),
  // rover open
  ROVER_OPEN("Rover_Open"),
  // rover adrequest
  ROVER_ADREQUEST("Rover_AdRequest"),
  // rover third party click
  ROVER_THIRDPARTY_CLICK("Rover_ThirdParty_Click"),
  // rover click bot
  CLICK_BOT("Rover_Click_Bot"),
  // rover open bot
  ROVER_OPEN_BOT("Rover_Open_Bot"),
  // rover roi
  ROVER_ROI("Rover_Roi");

  private final String name;

  PageNameEnum(String pageName) {
    this.name = pageName;
  }

  public String getName() {
    return name;
  }


}
