/**
 * 
 */
package com.wpl.bidding.utils;
import org.springframework.mail.MailSender;
import org.springframework.mail.SimpleMailMessage;

/**
 * @author Sushma
 *
 */
public class SendMail {
	
	
	public int sendMail(MailSender mailSender,String email, String itemName, String name )
	{
	SimpleMailMessage message = new SimpleMailMessage();
	message.setTo(email);
	message.setSubject("Congragulations!! You have won the bid");
	message.setText("Hi "+name+",\n\n" + "You have won the item "+itemName+" that you have bidded for.\n\n"+"Thanks,\nSmartBidTeam.");
	mailSender.send(message);
	return 1;
	}
	

}
