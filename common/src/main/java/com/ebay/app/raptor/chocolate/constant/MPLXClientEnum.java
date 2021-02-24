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
  US(711, "US", 1),
  MAIN(711, "US", 0),
  PL(4908, "Poland", 212),
  HK(3422, "Hong Kong", 201),
  UK(710, "UK", 3),
  FR(709, "France", 71),

  //new client_id for LatAm Countries
  AR(21551, "Argentina", 12),
  BO(21528, "Bolivia", 28),
  BR(21527, "Brazil", 31),
  CL(21552, "Chile", 44),
  CO(21553, "Colombia", 46),
  CR(21554, "Costa Rica", 51),
  DO(21555, "Dominican Republic", 60),
  EC(21556, "Ecuador", 61),
  GT(21559, "Guatemala", 85),
  MX(21562, "Mexico", 136),
  NI(21566, "Nicaragua", 150),
  PA(21567, "Panama", 158),
  PY(21568, "Paraguay", 160),
  PE(21565, "Peru", 225),
  PR(21569, "Puerto Rico", 214),
  UY(21570, "Uruguay", 248),
  VE(21571, "Venezuela", 252),

  // new client_id for GBH countries
  IL(21572, "Israel", 206),
  UA(21573, "UKRAINE", 232),
  GR(21574, "GREECE", 200),
  HU(21575, "HUNGARY", 202),
  CZ(21576, "Czech Republic", 197),
  DK(21577, "Denmark", 198),
  NO(21578, "Norway", 209),
  FI(21579, "Finland", 199),
  RU(21580, "Russian Federation", 215),
  JP(21581, "Japan", 104),
  KR(21582, "Korea South", 226),
  TH(21583, "Thailand", 219);

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

  public static MPLXClientEnum getByClientId(Integer mplxClientId) {
    for (MPLXClientEnum mplxClientEnum : MPLXClientEnum.values()) {
      if (mplxClientEnum.mplxClientId.equals(mplxClientId)) {
        return mplxClientEnum;
      }
    }
    return null;
  }
}
