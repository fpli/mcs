CREATE TABLE "thirdparty_whitelist" (
  ID INTEGER NOT NULL,
  TYPE_ID INTEGER NOT NULL,
  VERSION INTEGER NOT NULL,
  NAME VARCHAR(64) NOT NULL,
  VALUE VARCHAR(255) NOT NULL,
  DESCRIPTION VARCHAR(255) NOT NULL,
  CREATION_DATE TIMESTAMP NOT NULL,
  LAST_MODIFIED_DATE TIMESTAMP NOT NULL,
  UTF8_STATUS
  PRIMARY KEY ("id")
);

INSERT INTO thirdparty_whitelist (name, url)
    VALUES("thirdparty1", "https://thirdparty1");

INSERT INTO thirdparty_whitelist (name, url)
    VALUES("thirdparty2", "https://thirdparty2");