package com.ebay.traffic.chocolate.flink.nrt.filter;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.traffic.chocolate.flink.nrt.filter.configs.FilterRuleConfig;
import com.ebay.traffic.chocolate.flink.nrt.filter.configs.FilterRuleContent;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import com.google.gson.Gson;

import java.util.HashMap;
import java.util.Map;

/**
 * Controls the parsing of Chocolate application options.
 *
 * @author jepounds
 */
public class FilterRuleMgr {
    /** Configurable Filter Rules **/
    private final Map<ChannelType, Map<String, FilterRuleContent>> filterRuleConfigMap = new HashMap<>();

    public static FilterRuleMgr getInstance() {
        return FilterRuleMgr.SingletonHolder.instance;
    }

    private FilterRuleMgr() {
    }

    private static class SingletonHolder {
        private static final FilterRuleMgr instance = new FilterRuleMgr();
    }

    public Map<ChannelType, Map<String, FilterRuleContent>> getFilterRuleConfigMap() {
        return filterRuleConfigMap;
    }

    /**
     * Application options to load from internal jar
     *
     * @param  fileName load file from
     */
    public void initFilterRuleConfig(String fileName) {
        String jsonTxt = PropertyMgr.getInstance().loadFile(fileName);
        FilterRuleConfig[] filterRuleConfigArray = new Gson().fromJson(jsonTxt, FilterRuleConfig[].class);

        //Get Default Rule
        Map<String, FilterRuleContent> defaultRulesMap = new HashMap<>();
        for(FilterRuleConfig ruleConfig : filterRuleConfigArray){
            if(ruleConfig.getChannelType().equals(ChannelType.DEFAULT)){
                for(FilterRuleContent frc : ruleConfig.getFilterRules()){
                    defaultRulesMap.put(frc.getRuleName(), frc);
                }
                break;
            }
        }
        //Set All Channel Rules into HashMap
        for(FilterRuleConfig ruleConfig : filterRuleConfigArray){
            Map<String, FilterRuleContent> filterRuleConentMap = new HashMap<>(defaultRulesMap);
            for(FilterRuleContent ruleConent : ruleConfig.getFilterRules()){
                filterRuleConentMap.put(ruleConent.getRuleName(), ruleConent);
            }
            filterRuleConfigMap.put(ruleConfig.getChannelType(), filterRuleConentMap);
        }
    }
}
