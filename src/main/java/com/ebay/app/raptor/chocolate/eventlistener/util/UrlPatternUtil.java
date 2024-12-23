/*
 * Copyright (c) 2021. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.eventlistener.util;

import com.ebay.app.raptor.chocolate.avro.ChannelType;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class UrlPatternUtil {

  // do not filter /ulk XC-1541
  public static final Pattern ebaysites = Pattern.compile(
      "^(http[s]?:\\/\\/)?(?!rover)([\\w-.]+\\.)?(ebay(objects|motors|promotion|development|static|express|liveauctions|rtm)?)\\.[\\w-.]+($|\\/(?!ulk\\/).*)",
      Pattern.CASE_INSENSITIVE);

  public static final Pattern ebaySitesIncludeULK = Pattern.compile(
          "^(http[s]?:\\/\\/)?(?!rover)([\\w-.]+\\.)?(ebay(objects|motors|promotion|development|static|express|liveauctions|rtm)?)\\.[\\w-.]+($|\\/.*)",
          Pattern.CASE_INSENSITIVE);

  public static final Pattern roversites = Pattern.compile(
      "^(http[s]?:\\/\\/)?rover\\.(qa\\.)?ebay\\.[\\w-.]+(\\/.*)",
      Pattern.CASE_INSENSITIVE);

  // app deeplink sites XC-1797
  public static final Pattern deeplinksites =
      Pattern.compile("^(pad)?ebay:\\/\\/link([\\w-$%?&/.])?", Pattern.CASE_INSENSITIVE);
  // determine whether the url belongs to ebay sites for app deep link, and don't do any filter
  public static final Pattern deeplinkEbaySites = Pattern.compile(
      "^(http[s]?:\\/\\/)?(?!rover)([\\w-.]+\\.)?(ebay(objects|motors|promotion|development|static|express|liveauctions|rtm)?)\\.[\\w-.]+($|.*)",
      Pattern.CASE_INSENSITIVE);

  // e page target url sites
  public static final Pattern ePageSites = Pattern.compile(
      "^(http[s]?:\\/\\/)?c\\.([\\w.]+\\.)?(qa\\.)?ebay\\.[\\w-.]+\\/marketingtracking\\/v1\\/pixel\\?(.*)",
      Pattern.CASE_INSENSITIVE);

  // signin pattern
  public static final Pattern signinsites = Pattern.compile("^(http[s]?:\\/\\/)?signin\\.([\\w-.]+\\.)?ebay\\.[\\w-.]+(\\/.*)",
      Pattern.CASE_INSENSITIVE);

  // ulk link
  public static final Pattern ulksites = Pattern.compile(
          "^(http[s]?:\\/\\/)?(?!rover)([\\w-.]+\\.)?(ebay(objects|motors|promotion|development|static|express|liveauctions|rtm)?)\\.[\\w-.]+(\\/ulk\\/.*)",
          Pattern.CASE_INSENSITIVE);
}
