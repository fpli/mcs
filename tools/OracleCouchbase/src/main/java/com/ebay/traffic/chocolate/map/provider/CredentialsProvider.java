package com.ebay.traffic.chocolate.map.provider;

import com.ebay.security.exceptions.EsamsException;
import com.ebay.security.holders.NonKeyHolder;
import com.ebay.security.nameservice.NameService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * get name password from esams for oracle
 */
public class CredentialsProvider {
  public static class NamePassword {
    public final String name;
    public final String password;

    public NamePassword(String n, String p) {
      this.name = n;
      this.password = p;
    }
  }

  /**
   * Names of credentials in configuration and eSAMS
   */
  public static final String
    ESAMS_CHANNEL_ORACLE_USER = "site.epndal.oracleuser.nonkey",
    ESAMS_CHANNEL_ORACLE_PWD = "site.epndal.oraclepassword.nonkey",
    ESAMS_EMPTY_NONKEY = "empty-string";

  private static final Logger log = LoggerFactory.getLogger(CredentialsProvider.class);

  private NameService nameSvc;

  public CredentialsProvider(NameService nameSvc) {
    this.nameSvc = nameSvc;
  }

  public CredentialsProvider(Properties properties, NameService nameSvc) {
    // properties parameter is not used. We keep this constructor for compatibility with epnsync use.
    this.nameSvc = nameSvc;
  }

  /**
   * get name password from esams
   */
  private NamePassword getCredentialsFromEsams(String usernameChannel, String passwordChannel, String target) throws EsamsException {

    NonKeyHolder usernameHolder = nameSvc.getNonKey(usernameChannel, NameService.VERSION_LAST_ENABLED);
    if (usernameHolder == null || usernameHolder.hasError()) {
      log.error(String.format("Failed to get %s user name from eSAMS! ", target));
      if (usernameHolder != null) {
        log.error(usernameHolder.getErrMsg());
      }
      throw new EsamsException(usernameChannel + " value not found");
    }

    NonKeyHolder pwdHolder = nameSvc.getNonKey(passwordChannel, NameService.VERSION_LAST_ENABLED);
    if (pwdHolder == null || pwdHolder.hasError()) {
      log.error(String.format("Failed to get %s password from eSAMS! ", target));
      if (pwdHolder != null) {
        log.error(pwdHolder.getErrMsg());
      }
      throw new EsamsException(passwordChannel + " value not found");
    }

    String userName = usernameHolder.getNonKey();
    if (ESAMS_EMPTY_NONKEY.equals(userName)) {
      userName = "";
    }
    String pwd = pwdHolder.getNonKey();
    if (ESAMS_EMPTY_NONKEY.equals(pwd)) {
      pwd = "";
    }

    log.info(String.format("eSAMS - retrieved %s credentials, user name %s", target, userName));
    return new NamePassword(userName, pwd);
  }

  /**
   * get name password for oracle
   */
  public NamePassword forOracle() throws EsamsException {
    return getCredentialsFromEsams(ESAMS_CHANNEL_ORACLE_USER, ESAMS_CHANNEL_ORACLE_PWD, "Oracle");
  }
}
