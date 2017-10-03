/**
 * 
 */
package com.wpl.bidding.service;

import com.wpl.bidding.model.Response;
import com.wpl.bidding.persist.Item;
import com.wpl.bidding.persist.User;

/**
 * @author Sushma
 *
 */
public interface UserService {
	
	public User getUserInfo(int userId);

	public int updateUserInfo(User userInfo);
	
	public int createNewBid(Item item);
	
	public Response bidAnItem(float bidValue, int userId,int itemId);
	

}

