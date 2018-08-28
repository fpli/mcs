package com.ebay.app.raptor.chocolate.filter.rules;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.filter.service.BaseFilterRule;
import com.ebay.app.raptor.chocolate.filter.service.FilterRequest;
import org.apache.commons.lang3.math.Fraction;

import java.util.HashMap;

/**
 * tests the click-through rate of campaigns, fails if too high
 * Created by spugach on 1/13/17.
 */
public class CampaignClickThroughRateRule extends BaseFilterRule {
  private static final Object logSync = new Object();
  private static HashMap<Long, Fraction> log = new HashMap<>();
  private float rateLimit;
  
  /**
   * Constructor
   */
  public CampaignClickThroughRateRule(ChannelType channelType) {
    super(channelType);
    if (filterRuleContent == null || filterRuleContent.getRateLimit() == null) {
      this.rateLimit = 0;
    } else {
      this.rateLimit = filterRuleContent.getRateLimit();
    }
    //this.rateLimit = ApplicationOptions.getInstance().getByNameFloat(CLICK_THROUGH_RATE_CAP);
  }
  
  /**
   * Test the event's campaign for click-through rate if it has enough events
   * <p>
   * Shared side effect.
   *
   * @param event event (impression/click) to test
   * @return a bit, 0 for pass, 1 for fail
   */
  @Override
  public int test(FilterRequest event) {
    long campaignId = event.getCampaignId();
    int numerator, denominator;
    
    synchronized (logSync) {
      if (log.containsKey(campaignId)) {
        Fraction f = log.get(campaignId);
        numerator = f.getNumerator();
        denominator = f.getDenominator() + 1;
      } else {
        numerator = 0;
        denominator = 1;
      }
      
      if (event.getChannelAction() == ChannelAction.CLICK) {
        ++numerator;
      }
      
      Fraction newFraction = Fraction.getFraction(numerator, denominator);
      
      log.put(campaignId, newFraction);
      
      if (denominator < 100) {    // Not enough samples to make a judgement, auto-pass
        return 0;
      }
      
      if (newFraction.floatValue() < rateLimit || filterRuleContent == null) {
        return 0;
      } else {
        return 1;
      }
    }
  }
}
