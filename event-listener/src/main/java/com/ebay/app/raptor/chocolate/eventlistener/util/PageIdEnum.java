package com.ebay.app.raptor.chocolate.eventlistener.util;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Define the PageId used in UBI
 *
 * @author Zhiyuan Wang
 * @since 2019/11/25
 */
public enum PageIdEnum {
  // for click
  CLICK(2547208, ChannelAction.CLICK),
  // for email open
  EMAIL_OPEN(3962, ChannelAction.EMAIL_OPEN),
  // for ad request
  AR(2561745, ChannelAction.SERVE),
  // for roi event
  ROI(2483445, ChannelAction.ROI);

  private final int id;
  private ChannelAction channelAction;

  PageIdEnum(int pageId, ChannelAction channelAction) {
    this.id = pageId;
    this.channelAction = channelAction;
  }

  /**
   * Mapping from action to enum
   */
  private static final Map<ChannelAction, Integer> MAP;


  static {
    Map<ChannelAction, Integer> map = new HashMap<>();
    for (PageIdEnum p : PageIdEnum.values()) {
      map.put(p.getAction(), p.getId());
    }

    MAP = Collections.unmodifiableMap(map);
  }

  public int getId() {
    return id;
  }

  public ChannelAction getAction() {
    return channelAction;
  }

  public static int getPageIdByAction(ChannelAction channelAction) {
    return MAP.get(channelAction);
  }
}
