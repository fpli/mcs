package com.ebay.traffic.chocolate.email;

import com.ebay.traffic.chocolate.parse.HTMLParse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;

/**
 * @author lxiong1
 */
public class SendEmail {

  private static final Logger logger = LoggerFactory.getLogger(SendEmail.class);
  public static SendEmail sendEmail = new SendEmail();

  public String emailHostServer;
  public String toEmail;
  public String date;
  public String time;
  public String runPeriod;

  private SendEmail() {
  }

  public void init(String emailHostServer, String toEmail, String date, String time, String runPeriod) {
    this.emailHostServer = emailHostServer;
    this.toEmail = toEmail;
    this.date = date;
    this.time = time;
    this.runPeriod = runPeriod;
  }

  public static SendEmail getInstance() {
    return sendEmail;
  }

  public void send() {
    if (toEmail == null || toEmail.length() < 1) {
      return;
    }

    String[] users = toEmail.split(",");
    if(users == null || users.length ==0){
      logger.error("Email recipient is empty");
      return;
    }
    send(users);
  }

  public void send(String[] users) {

    // sender email address
    String from = "dl-ebay-performance-marketing-oncall@ebay.com";

    // system property
    Properties properties = System.getProperties();

    // set smtp server name
    properties.setProperty("mail.smtp.host", emailHostServer);

    // get the default session
    Session session = Session.getDefaultInstance(properties);

    try {
      // MimeMessage
      MimeMessage message = new MimeMessage(session);

      // Set From: header
      message.setFrom(new InternetAddress(from));

      Address[] recipients = new Address[users.length];
      for (int i=0;i< users.length;i++){
        recipients[i] = new InternetAddress(users[i]);
      }

      // Set To: header
      message.addRecipients(Message.RecipientType.TO,
              recipients);

      // Set Subject: header
      switch (runPeriod) {
        case "daily":
          message.setSubject("Daily report for tracking (Chocolate team)! " + date);
          break;
        case "hourly":
          message.setSubject("Hourly report for tracking (Chocolate team)! " + time);
          break;
        default:
          message.setSubject("Wrong email message" + time);
          break;
      }

      // set message entity
      message.setContent(HTMLParse.parse(runPeriod), "text/html");

      logger.info("Start to sent message to: " + toEmail);
      System.out.println("Start to sent message to: " + toEmail);
      Transport.send(message);
      logger.info("Sent message to: " + toEmail + " successfully.");
      System.out.println("Sent message to: " + toEmail + " successfully.");
    } catch (MessagingException mex) {
      logger.info(mex.getMessage());
    }
  }

}
