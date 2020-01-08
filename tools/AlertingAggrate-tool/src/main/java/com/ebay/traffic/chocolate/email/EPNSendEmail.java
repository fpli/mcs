package com.ebay.traffic.chocolate.email;

import com.ebay.traffic.chocolate.pojo.DailyClickTrend;
import com.ebay.traffic.chocolate.pojo.DailyDomainTrend;
import com.ebay.traffic.chocolate.pojo.HourlyClickCount;
import com.ebay.traffic.chocolate.parse.EPNHTMLParse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.List;
import java.util.Properties;

/**
 * @author lxiong1
 */
public class EPNSendEmail {

	private static final Logger logger = LoggerFactory.getLogger(EPNSendEmail.class);
	public static EPNSendEmail sendEmail = new EPNSendEmail();

	public String emailHostServer;
	public String toEmail;

	private EPNSendEmail() {
	}

	public void init(String emailHostServer, String toEmail) {
		this.emailHostServer = emailHostServer;
		this.toEmail = toEmail;
	}

	public static EPNSendEmail getInstance() {
		return sendEmail;
	}

	public void send(List<DailyClickTrend> dailyClickTrend, List<DailyDomainTrend> dailyDomainTrend) {
		if (toEmail == null || toEmail.length() < 1) {
			return;
		}

		String[] users = toEmail.split(",");
		for (String user : users) {
			send(dailyClickTrend, dailyDomainTrend, user);
		}

	}

	public void send(List<HourlyClickCount> hourlyClickCount) {
		if (toEmail == null || toEmail.length() < 1) {
			return;
		}

		String[] users = toEmail.split(",");
		for (String user : users) {
			send(hourlyClickCount, user);
		}

	}

	public void send(List<DailyClickTrend> dailyClickTrend, List<DailyDomainTrend> dailyDomainTrend, String emailAccount) {

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
			message.setSubject("Daily report for ams_click (chocolate hdfs)!");

			// set message entity
			message.setContent(EPNHTMLParse.parse(dailyClickTrend, dailyDomainTrend), "text/html");

			// send message
			Transport.send(message);
			System.out.println("Sent message successfully....");
		} catch (MessagingException mex) {
			mex.printStackTrace();
		}
	}

	public void send(List<HourlyClickCount> hourlyClickCount, String emailAccount) {

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
			message.setSubject("Hourly report for ams_click (chocolate hdfs)! ");

			// set message entity
			message.setContent(EPNHTMLParse.parse(hourlyClickCount), "text/html");

			// send message
			Transport.send(message);
			System.out.println("Sent message successfully....");
		} catch (MessagingException mex) {
			mex.printStackTrace();
		}
	}

}
