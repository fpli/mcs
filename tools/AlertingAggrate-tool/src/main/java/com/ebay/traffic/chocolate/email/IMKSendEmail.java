package com.ebay.traffic.chocolate.email;

import com.ebay.traffic.chocolate.pojo.IMKHourlyClickCount;
import com.ebay.traffic.chocolate.util.IMKHTMLParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.mail.Session;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

/**
 * Created by shuangxu on 10/24/19.
 */
public class IMKSendEmail {
    private static final Logger logger = LoggerFactory.getLogger(EPNSendEmail.class);
    public static IMKSendEmail sendEmail = new IMKSendEmail();

    public String emailHostServer;
    public String toEmail;

    private IMKSendEmail() {
    }

    public void init(String emailHostServer, String toEmail) {
        this.emailHostServer = emailHostServer;
        this.toEmail = toEmail;
    }

    public static IMKSendEmail getInstance() {
        return sendEmail;
    }

    public void send(Map<String, List<IMKHourlyClickCount>> hourlyClickCountMap, String title) {
        if (toEmail == null || toEmail.length() < 1) {
            return;
        }

        String[] users = toEmail.split(",");
        for (String user : users) {
            send(hourlyClickCountMap, user, title);
        }

    }

    public void send(Map<String, List<IMKHourlyClickCount>> hourlyClickCountMap, String emailAccount, String title) {

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
            message.setSubject(title);

            // set message entity
            message.setContent(IMKHTMLParser.parse(hourlyClickCountMap), "text/html");

            // send message
            Transport.send(message);
            System.out.println("Sent message successfully....");
        } catch (MessagingException mex) {
            mex.printStackTrace();
        }
    }



}
