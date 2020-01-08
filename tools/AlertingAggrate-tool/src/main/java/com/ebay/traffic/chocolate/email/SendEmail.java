package com.ebay.traffic.chocolate.email;

import com.ebay.traffic.chocolate.pojo.AzkabanFlow;
import com.ebay.traffic.chocolate.pojo.MetricCount;
import com.ebay.traffic.chocolate.parse.AzkabanHTMLParse;
import com.ebay.traffic.chocolate.parse.HTMLParse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.ArrayList;
import java.util.HashMap;
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

  public void send(HashMap<String, ArrayList<MetricCount>> map) {
    if (toEmail == null || toEmail.length() < 1) {
      return;
    }

    String[] users = toEmail.split(",");
    for (String user : users) {
      send(map, null, user);
    }

  }

  public void send(HashMap<String, ArrayList<MetricCount>> map, HashMap<String, ArrayList<MetricCount>> historymap) {
    if (toEmail == null || toEmail.length() < 1) {
      return;
    }

    String[] users = toEmail.split(",");
    for (String user : users) {
      send(map, historymap, user);
    }

  }

  public void sendAzkaban(HashMap<String, ArrayList<AzkabanFlow>> map) {
    if (toEmail == null || toEmail.length() < 1) {
      return;
    }

    String[] users = toEmail.split(",");
    for (String user : users) {
      sendAzkaban(map, user);
    }

  }

  public void send(HashMap<String, ArrayList<MetricCount>> map, HashMap<String, ArrayList<MetricCount>> historymap, String emailAccount) {

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

      // Set To: header
      message.addRecipient(Message.RecipientType.TO,
              new InternetAddress(emailAccount));

      // Set Subject: header
      if(runPeriod.equalsIgnoreCase("daily")) {
        message.setSubject("Daily report for tracking! " + date);
      }else if(runPeriod.equalsIgnoreCase("hourly")){
        message.setSubject("Hourly report for tracking! " + time);
      }

      // set message entity
      message.setContent(HTMLParse.parse(map, historymap, runPeriod), "text/html");

      // send message
      Transport.send(message);
      System.out.println("Sent message successfully....");
    } catch (MessagingException mex) {
      mex.printStackTrace();
    }
  }

  public void sendAzkaban(HashMap<String, ArrayList<AzkabanFlow>> map, String emailAccount) {

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

      // Set To: header
      message.addRecipient(Message.RecipientType.TO,
        new InternetAddress(emailAccount));

      message.setSubject("Hourly azkaban report for tracking! " + time);

      // set message entity
      message.setContent(AzkabanHTMLParse.parse(map), "text/html");

      // send message
      Transport.send(message);
      System.out.println("Sent message successfully....");
    } catch (MessagingException mex) {
      mex.printStackTrace();
    }
  }

}
