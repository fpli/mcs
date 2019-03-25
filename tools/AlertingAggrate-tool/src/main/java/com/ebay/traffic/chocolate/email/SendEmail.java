package com.ebay.traffic.chocolate.email;

import com.ebay.traffic.chocolate.pojo.Metric;
import com.ebay.traffic.chocolate.pojo.MetricCount;
import com.ebay.traffic.chocolate.util.HTMLParse;
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

  private SendEmail () { }

  public void init(String emailHostServer, String toEmail){
    this.emailHostServer = emailHostServer;
    this.toEmail = toEmail;
  }

  public static SendEmail getInstance(){
    return sendEmail;
  }

  public void send(HashMap<String, ArrayList<MetricCount>> map) {
    if(toEmail == null || toEmail.length() < 1){
      return;
    }

    String[] users = toEmail.split(",");
    for (String user: users) {
      send(map, user);
    }

  }

  public void send(HashMap<String, ArrayList<MetricCount>> map, String emailAccount) {

    // 发件人电子邮箱
    String from = "lxiong1@ebay.com";

    // 获取系统属性
    Properties properties = System.getProperties();

    // 设置邮件服务器
    properties.setProperty("mail.smtp.host", emailHostServer);

    // 获取默认session对象
    Session session = Session.getDefaultInstance(properties);

    try{
      // 创建默认的 MimeMessage 对象
      MimeMessage message = new MimeMessage(session);

      // Set From: 头部头字段
      message.setFrom(new InternetAddress(from));

      // Set To: 头部头字段
      message.addRecipient(Message.RecipientType.TO,
              new InternetAddress(emailAccount));

      // Set Subject: 头部头字段
      message.setSubject("Chocolate Alerting Aggregation!");

      // 设置消息体
      message.setContent(HTMLParse.parse(map), "text/html");

      // 发送消息
      Transport.send(message);
      System.out.println("Sent message successfully....");
    }catch (MessagingException mex) {
      mex.printStackTrace();
    }
  }

}
