package com.ebay.traffic.chocolate.email;

import com.ebay.traffic.chocolate.pojo.DailyClickTrend;
import com.ebay.traffic.chocolate.pojo.DailyDomainTrend;
import com.ebay.traffic.chocolate.pojo.HourlyClickCount;
import com.ebay.traffic.chocolate.util.EPNHTMLParse;
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

		// 发件人电子邮箱
		String from = "dl-ebay-performance-marketing-oncall@ebay.com";

		// 获取系统属性
		Properties properties = System.getProperties();

		// 设置邮件服务器
		properties.setProperty("mail.smtp.host", emailHostServer);

		// 获取默认session对象
		Session session = Session.getDefaultInstance(properties);

		try {
			// 创建默认的 MimeMessage 对象
			MimeMessage message = new MimeMessage(session);

			// Set From: 头部头字段
			message.setFrom(new InternetAddress(from));

			// Set To: 头部头字段
			message.addRecipient(Message.RecipientType.TO,
				new InternetAddress(emailAccount));

			// Set Subject: 头部头字段
			message.setSubject("Daily report for ams_click!");

			// 设置消息体
			message.setContent(EPNHTMLParse.parse(dailyClickTrend, dailyDomainTrend), "text/html");

			// 发送消息
			Transport.send(message);
			System.out.println("Sent message successfully....");
		} catch (MessagingException mex) {
			mex.printStackTrace();
		}
	}

	public void send(List<HourlyClickCount> hourlyClickCount, String emailAccount) {

		// 发件人电子邮箱
		String from = "lxiong1@ebay.com";

		// 获取系统属性
		Properties properties = System.getProperties();

		// 设置邮件服务器
		properties.setProperty("mail.smtp.host", emailHostServer);

		// 获取默认session对象
		Session session = Session.getDefaultInstance(properties);

		try {
			// 创建默认的 MimeMessage 对象
			MimeMessage message = new MimeMessage(session);

			// Set From: 头部头字段
			message.setFrom(new InternetAddress(from));

			// Set To: 头部头字段
			message.addRecipient(Message.RecipientType.TO,
				new InternetAddress(emailAccount));

			// Set Subject: 头部头字段
			message.setSubject("Hourly report for ams_click! ");

			// 设置消息体
			message.setContent(EPNHTMLParse.parse(hourlyClickCount), "text/html");

			// 发送消息
			Transport.send(message);
			System.out.println("Sent message successfully....");
		} catch (MessagingException mex) {
			mex.printStackTrace();
		}
	}

}
