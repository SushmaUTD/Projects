/**
 * 
 */
package com.wpl.bidding.service;

import com.wpl.bidding.model.Item;
import com.wpl.bidding.model.ItemModel;
import com.wpl.bidding.model.Response;
import com.wpl.bidding.model.UserModel;
import com.wpl.bidding.persist.User;

/**
 * @author Sushma
 *
 */
public interface UserService {
	
	public UserModel getUserInfo(int userId);
	public ItemModel updateUserInfo(User userInfo);
	public Response createNewBid(Item item);
}
