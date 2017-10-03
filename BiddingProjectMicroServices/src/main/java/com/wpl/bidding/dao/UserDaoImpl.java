package com.wpl.bidding.dao;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mail.MailSender;

import com.wpl.bidding.common.ConstantMessages;
import com.wpl.bidding.model.Response;
import com.wpl.bidding.persist.BidItem;
import com.wpl.bidding.persist.Item;
import com.wpl.bidding.persist.User;
import com.wpl.bidding.persist.UserBids;
import com.wpl.bidding.utils.SendMail;


public class UserDaoImpl implements UserDao {
	@Autowired
	SessionFactory sessionFactory;
	
	@Autowired
    MailSender mailSender;

	protected Session getCurrentSession() {
		return sessionFactory.getCurrentSession();
	}

	@SuppressWarnings("unchecked")
	public User getUserInfo(int userId)
	{
		User results = new User();
		try{
		Session session = sessionFactory.openSession();
		session.beginTransaction();
		Query query = session.createQuery("from User where userId="+userId);
		results = (User) query.list().get(0);
		session.getTransaction().commit();
		}catch(Exception e)
		{
			System.out.print("Error while getting user"+e);
			results = null;
		}
		return results;
	}

	public int updateUserInfo(User userInfo) {
		try{
			Session session = sessionFactory.openSession();
			session.beginTransaction();
			userInfo.setModifiedDate(new Date(System.currentTimeMillis()));
			session.update(userInfo);
			session.getTransaction().commit();
			}catch(Exception e)
			{
				System.out.println("Error while creating user"+e);
				return 0;
			}
			return 1;
	}
	
	

	@Override
	public int createNewBid(Item item) {
		// TODO Auto-generated method stub
		try{
			Session session = sessionFactory.openSession();
			session.beginTransaction();
			item.setCreatedDate(new Date(System.currentTimeMillis()));
			item.setStatus("CREATED");
			System.out.println("UserId"+item.getUserId().getUserId());
			session.save(item);
			session.getTransaction().commit();
			}
			catch(Exception e){
				System.out.println("Error while creating user"+e);
				return 0;
			}
			return 1;
	}

	@Override
	public Response bidForAnItem(float bidValue, int userId,int itemId) {
		Session session = sessionFactory.openSession();
		session.beginTransaction();
		Response response = new Response();
		BidItem bidItem = new BidItem();
		Item item = new Item();
		item.setItemId(itemId);
		User user = new User();
		user.setUserId(userId);
		bidItem.setBidValue(bidValue);
		bidItem.setItemId(item);
		bidItem.setUserId(user);
		session.save(bidItem);	
		session.getTransaction().commit();
			if( bidItem.getBidItemId() > 0)
			{
				response.setResponseCode(ConstantMessages.successCode);
				response.setResponseMessage("Thank you for Bidding!! Result will be notified in a mail.");
			}
			else
			{
				response.setResponseCode(ConstantMessages.serverErrorCode);
				response.setResponseMessage(ConstantMessages.technicalError);
			}		
		Query updateQuery = session.createQuery("update Item  set status='ONGOING' where itemId="+itemId);
		updateQuery.executeUpdate();
		return response;
	}

	@Override
	public Response closeBid(int userId, int itemId) {
		Session session = sessionFactory.openSession();
		session.beginTransaction();
		Response response = new Response();
		Query query = session.createQuery("from BidItem where itemId ='"+itemId+"' order by bidValue desc");
		query.setMaxResults(1);
		BidItem biddedUser = (BidItem) query.list().get(0);
		session.getTransaction().commit();
		Item item = new Item();
		User user = new User();
		item.setItemId(itemId);
		user.setUserId(biddedUser.getUserId().getUserId());
		System.out.println("Email:"+biddedUser.getUserId().getEmail());
		System.out.println("ItemId:"+biddedUser.getItemId().getItemName());
		System.out.println("FirstName:"+biddedUser.getUserId().getFirstName());
		SendMail send = new SendMail();
		send.sendMail(mailSender,biddedUser.getUserId().getEmail(),  biddedUser.getItemId().getItemName(), biddedUser.getUserId().getFirstName());
		Query updateQuery = session.createQuery("update Item set status='BIDDED' where itemId="+itemId);
		updateQuery.executeUpdate();
		UserBids user_bids = new UserBids();
		user_bids.setItemId(item);
		user_bids.setUserId(user);
		saveUserBid(user_bids); 
		response.setResponseCode(ConstantMessages.successCode);
		response.setResponseMessage("Notification has been sent to the user with highest bid.");
		return response;
	}
	
	public void saveUserBid(UserBids bidUser)
	{
		Session session = sessionFactory.openSession();
		session.beginTransaction();	
		session.save(bidUser);
		session.getTransaction().commit();
	}

	@Override
	public List<Item> purchaseHistory(int userId) {
		Session session = sessionFactory.openSession();
		session.beginTransaction();
		List<Item> items = new ArrayList<Item>();
		List<UserBids> userBids = new ArrayList<UserBids>();
		Query query = session.createQuery("from UserBids where userId="+userId);
		userBids = (List<UserBids>) query.list();
		for(int i=0;i<userBids.size();i++)
		{
			items.add(userBids.get(i).getItemId());
		}
		return items;
	}
	
}
