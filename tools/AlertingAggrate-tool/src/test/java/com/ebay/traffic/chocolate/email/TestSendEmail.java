package com.ebay.traffic.chocolate.email;

import org.junit.Test;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;

public class TestSendEmail {

	@Test
	public void testSend(){
		send();
	}

	public void send() {

		// 发件人电子邮箱
		String from = "lxiong1@ebay.com";

		// 获取系统属性
		Properties properties = System.getProperties();

		// 设置邮件服务器
		properties.setProperty("mail.smtp.host", "atom.corp.ebay.com");

		// 获取默认session对象
		Session session = Session.getDefaultInstance(properties);

		try {
			// 创建默认的 MimeMessage 对象
			MimeMessage message = new MimeMessage(session);

			// Set From: 头部头字段
			message.setFrom(new InternetAddress(from));

			// Set To: 头部头字段
			message.addRecipient(Message.RecipientType.TO,
				new InternetAddress("lxiong1@ebay.com"));

			message.setSubject("Test email.");
			// 设置消息体
			message.setContent(new HTMLTemplate().getContent(),  "text/html");

			// 发送消息
			Transport.send(message);
			System.out.println("Sent message successfully....");
		} catch (MessagingException mex) {
			mex.printStackTrace();
		}
	}
}
