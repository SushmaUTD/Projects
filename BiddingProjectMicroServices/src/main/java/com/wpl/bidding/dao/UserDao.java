package com.wpl.bidding.dao;

import java.util.List;

import com.wpl.bidding.model.Response;
import com.wpl.bidding.persist.Item;
import com.wpl.bidding.persist.User;


public interface UserDao {
	
	public User getUserInfo(int userId);
	public int updateUserInfo(User userInfo);
	public int createNewBid(Item item);
	public Response bidForAnItem(float bidValue, int userId, int itemId);
	public Response closeBid(int userId, int itemId);
	public List<Item> purchaseHistory(int userId);
}
