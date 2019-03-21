package com.ebay.traffic.chocolate.sparknrt.epnnrt;

public enum ReasonCodeEnum{

  REASON_CODE0(0),//valid transaction
  REASON_CODE2(2),//invalid prog pub map
  REASON_CODE3(3),//invalid pub camp
  REASON_CODE4(4),//inactive pub
  REASON_CODE5(5),//inative prog pub map
  REASON_CODE6(6),//click filtered
  REASON_CODE7(7),//deactive camp
  REASON_CODE8(8),//roi filtered
  REASON_CODE10(10);//referrring domain roi

  int reasonCode;
  ReasonCodeEnum(int r){
    reasonCode = r;
  }

  public String getReasonCode(){
    return Integer.toString(reasonCode);
  }

}