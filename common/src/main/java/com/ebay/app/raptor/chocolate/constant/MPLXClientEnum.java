package com.ebay.app.raptor.chocolate.constant;

/**
 * Identify the various marketing channels for which we track.
 */
public enum MPLXClientEnum {
  NL(1346, "Netherlands", 146),
  CN(4080, "China", 223),
  CH(5222, "Switzerland", 193),
  SG(3423, "Singapore", 216),
  QUEBEC(5968, "Quebec", 210),
  BE_NL(1553, "Belgium", 123),
  AT(5221, "Austria", 16),
  DE(707, "Germany", 77),
  //  MAIN(5662, "Prostores", 0),
  MY(4825, "Malaysia", 207),
  AU(705, "Australia", 15),
  CA(706, "Canada", 2),
  IT(724, "Italy", 101),
  PH(4824, "Philippines", 211),
  //  US(9993, "ProStoresSEM", 0),
  IE(5282, "Ireland", 205),
  TW(1631, "Taiwan", 196),
  SE(3424, "Sweden", 218),
  ES(1185, "Spain", 186),
  IN(4686, "India", 203),
  MAIN(711, "US", 1),
  US(711, "US Main", 0),
  PL(4908, "Poland", 212),
  HK(3422, "Hong Kong", 201),
  UK(710, "UK", 3),
  FR(709, "France", 71);

  private Integer mplxClientId;
  private String mplxClientName;
  private Integer ebaySiteId;

  public Integer getMplxClientId() {
    return mplxClientId;
  }

  public String getMplxClientName() {
    return mplxClientName;
  }

  public Integer getEbaySiteId() {
    return ebaySiteId;
  }


  MPLXClientEnum(Integer mplxClientId, String mplxClientName, Integer ebaySiteId) {
    this.mplxClientId = mplxClientId;
    this.mplxClientName = mplxClientName;
    this.ebaySiteId = ebaySiteId;
  }

  public static MPLXClientEnum getBySiteId(int siteId) {
    for (MPLXClientEnum mplxClientEnum : MPLXClientEnum.values()) {
      if (mplxClientEnum.ebaySiteId == siteId) {
        return mplxClientEnum;
      }
    }
    return null;
  }
}
