package com.ebay.app.raptor.chocolate.constant;

/**
 * This enum is used for ROI Transaction Type
 */
public enum RoiTransactionEnum {
    UNKNOWN("Unknown", 0),
    REGISTRATION("Reg", 1),
    REG_CONFIRM("Conf", 2),
    BID("Bid", 3),
    LIST("Sell", 4),
    BIN_ABIN("BIN-ABIN", 5),
    BIN_FP("BIN-FP", 6),
    BIN_STORE("BIN-Store", 7),
    LIST_BIN("Sell-BIN", 8),
    STORE_BUILDER_CONFIRM("Store-Build-Conf", 9),
    BIN("BIN", 10),
    REG_SELLER_CONF("Reg-Sconf", 11),
    BID_FIRST_BID("BID-FB", 12),
    BID_NON_FIRST_BID("Bid-NFB", 13),
    BID_OUTBID("Bid-OB", 14),
    LIST_ABIN("List-ABIN", 15),
    LIST_FP("List-FP", 16),
    LIST_STORE("List-Store", 17),
    STORE_BASIC("Store-Basic", 18),
    STORE_FEATURED("Store-Featured", 19),
    REG_CONFIRM_2("RegistrationConfirm-2", 20),
    BID_FROM_REGISTRATION("BID-From-Registration", 21),
    BIN_FROM_REGISTRATION("BIN-From-Registration", 22),
    SM_SUB("SMsub", 23),
    SM_PRO_SUB("SMProSub", 24),
    SA_SUB("SASub", 25),
    SA_PRO_SUB("SAProSub", 26),
    PP_PREFERRED("PPPreferred", 27),
    PP_ALL("PP_All", 28),
    SEM_EVENT("Sem-Event", 29),
    STORE_ANCHOR("Store-Anchor", 30),
    REG_SELLER("Reg-Sell", 31),
    REG_SELLER_INT("Reg-Sell-Int", 32),
    REG_SELLER_CONF_INT("Reg-Sconf-Int", 33),
    HALF_BIN("Half-Bin", 34),
    HALF_REG("Half-Reg", 714),
    HALF_CART_CONF("Half-Cart-Conf", 713),
    REG_CHECKOUT("Reg-Checkout", 35),
    CHECKOUT("Checkout", 36),
    REG_CONFIRM_EBX_EULA("Buyer Registration - DE User Agreement approval", 37),
    REG_CONFIRM_EBX_TIP("Buyer Registration - Non-Checout - Task In Progress", 38),
    REG_CONFIRM_EBX_EULA_TIP("Buyer Registration - DE User Agreement approval", 40),
    REG_EBX_EULA_CHECKOUT("Buyer Registration-DE User Agreement approval", 41),
    STREAMLINED_BIN("Streamlined-BIN", 42),
    CART_ADD("Cart-Add", 717),
    REGISTRATION_INTERNAL("Reg-Int", 101),
    REG_CONFIRM_INTERNAL("Conf-Int", 102),
    BID_INTERNAL("Bid-Int", 103),
    LIST_INTERNAL("Sell-Int", 104),
    BIN_ABIN_INTERNAL("BIN-ABIN-Int", 105),
    BIN_FP_INTERNAL("BIN-FP-Int", 106),
    BIN_STORE_INTERNAL("BIN-Store-Int", 107),
    LIST_BIN_INTERNAL("Sell-BIN-Int", 108),
    STORE_BUILDER_CONFIRM_INTERNAL("Store-Build-Conf-Int", 109),
    SELLER_FEATURE("Sell_Feat", 110),
    LIST_FORMAT("Format", 111),
    LIST_BINFORMAT("SellBIN", 112),
    LIST_PHOTOS("Photos", 113),
    STREAMLINED_BIN_INTERNAL("Streamlined-BIN-Int", 114),
    AUTO_REGISTRATION("Auto-Reg", 201),
    AUTO_REG_CONFIRM("Auto-Conf", 202),
    AUTO_BID("Auto-Bid", 203),
    AUTO_LIST("Auto-Sell", 204),
    AUTO_BIN_ABIN("Auto-BIN-ABIN", 205),
    AUTO_BIN_FP("Auto-BIN-FP", 206),
    AUTO_BIN_STORE("Auto-BIN-Store", 207),
    AUTO_LIST_BIN("Auto-Sell-BIN", 208),
    AUTO_REGISTRATION_INTERNAL("Auto-Reg-Int", 301),
    AUTO_REG_CONFIRM_INTERNAL("Auto-Conf-Int", 302),
    AUTO_BID_INTERNAL("Auto-Bid-Int", 303),
    AUTO_LIST_INTERNAL("Auto-Sell-Int", 304),
    AUTO_BIN_ABIN_INTERNAL("Auto-BIN-ABIN-Int", 305),
    AUTO_BIN_FP_INTERNAL("Auto-BIN-FP-Int", 306),
    AUTO_BIN_STORE_INTERNAL("Auto-BIN-Store-Int", 307),
    AUTO_LIST_BIN_INTERNAL("Auto-Sell-BIN-Int", 308),
    AUTOVEH_REGISTRATION("AutoVeh-Reg", 401),
    AUTOVEH_REG_CONFIRM("AutoVeh-Conf", 402),
    AUTOVEH_BID("AutoVeh-Bid", 403),
    AUTOVEH_LIST("AutoVeh-Sell", 404),
    AUTOVEH_BIN_ABIN("AutoVeh-BIN-ABIN", 405),
    AUTOVEH_BIN_FP("AutoVeh-BIN-FP", 406),
    AUTOVEH_BIN_STORE("AutoVeh-BIN-Store", 407),
    AUTOVEH_LIST_BIN("AutoVeh-Sell-BIN", 408),
    AUTOVEH_REGISTRATION_INTERNAL("AutoVeh-Reg-Int", 501),
    AUTOVEH_REG_CONFIRM_INTERNAL("AutoVeh-Conf-Int", 502),
    AUTOVEH_BID_INTERNAL("AutoVeh-Bid-Int", 503),
    AUTOVEH_LIST_INTERNAL("AutoVeh-Sell-Int", 504),
    AUTOVEH_BIN_ABIN_INTERNAL("AutoVeh-BIN-ABIN-Int", 505),
    AUTOVEH_BIN_FP_INTERNAL("AutoVeh-BIN-FP-Int", 506),
    AUTOVEH_BIN_STORE_INTERNAL("AutoVeh-BIN-Store-Int", 507),
    AUTOVEH_LIST_BIN_INTERNAL("AutoVeh-Sell-BIN-Int", 508),
    UNIT_TESTING_ONLY("UsedForUnitTesting", 9999),
    LIST_SUBTITLE("Subtitle", 601),
    LIST_2NDCAT("2ndCat", 602),
    LIST_SCHED_START("SchedStart", 603),
    LIST_10DAY("10day", 604),
    LIST_ADDPICS("PicQTY", 605),
    LIST_SUPERSIZE("LargePic", 606),
    LIST_PICTUREPACK("PicPack", 607),
    LIST_DESIGNER("Designer", 608),
    LIST_GALLERY("GallImage", 609),
    LIST_GALLERY_FEATURED("GallFeature", 610),
    LIST_BOLD("Bold", 611),
    LIST_BORDER("Border", 612),
    LIST_HIGHLIGHT("Highlight", 613),
    LIST_FEATPLUS("FeatPlus", 614),
    LIST_HPFEAT("HPFeat", 615),
    LIST_GIFT("Gift", 616),
    LIST_RESERVE("Reserve", 617),
    LIST_IMMEDPAY("ImmedPay", 618),
    CLASSIFIFED_VIP("Classifieds - VIP", 710),
    CLASSIFIFED_ASQ("Classifieds - Ask Seller", 711),
    TRACK_INVALID_EMAIL_ALERT("signin_u", 701),
    TRACK_RECONFIRM_EMAIL_SHOW("recon_em", 702),
    TRACK_CHANGE_EMAIL("chng_e", 703),
    TRACK_CHANGE_EMAIL_SUCCESS("chng_suc", 704),
    LIST_CLS("lst_cls", 705),
    AAQ_CLS("aaq_cls", 706),
    SITS_PAYMENT("sits_payment", 707),
    PP_PAYMENT("paypal_payment", 708),
    PRE_BID("Pre-Bid", 709),
    LIST_EBX_ONLY("lst_ebx", 39),
    BEST_OFFER_CONFIRM("BO", 712),
    BIN_MOBILEAPP("BIN-MobileApp", 715),
    BID_MOBILEAPP("BID-MobileApp", 716),
    SELL_MOBILE_APP("Sell-MobileApp", 718),
    REG_MOBILE_APP("Reg-MobileApp", 719),
    CONF_MOBILE_APP("Conf-MobileApp", 720),
    BO_MOBILE_APP("BO-MobileApp", 721),
    REG_SELL_MOBILE_APP("RegSell-MobileApp", 722);

    private String transTypeName;
    private Integer transTypeValue;

    public String getTransTypeName() {
        return transTypeName;
    }

    public Integer getTransTypeValue() {
        return transTypeValue;
    }

    RoiTransactionEnum(String transTypeName, Integer transTypeValue) {
        this.transTypeName = transTypeName;
        this.transTypeValue = transTypeValue;
    }

    public static RoiTransactionEnum getByTransTypeName(String transTypeName) {
        for (RoiTransactionEnum roiTransactionEnum : RoiTransactionEnum.values()) {
            if (roiTransactionEnum.transTypeName.equals(transTypeName)) {
                return roiTransactionEnum;
            }
        }
        return UNKNOWN;
    }
}
