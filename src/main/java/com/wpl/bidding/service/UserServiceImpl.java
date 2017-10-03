/**
 * 
 */
package com.wpl.bidding.service;

import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import com.wpl.bidding.common.Route;
import com.wpl.bidding.model.Item;
import com.wpl.bidding.model.ItemModel;
import com.wpl.bidding.model.Response;
import com.wpl.bidding.model.UserModel;
import com.wpl.bidding.persist.User;

/**
 * @author Sushma
 *
 */
public class UserServiceImpl implements UserService{
	
	public UserModel getUserInfo(int userId) {
		UserModel customer = new UserModel();
		RestTemplate restTemplate = new RestTemplate();
		UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(Route.basePath+Route.getProfileInfoUrl);
		builder.queryParam("userId", userId);
	    customer = restTemplate.getForObject(builder.build().encode().toUri(), UserModel.class);
	    return customer;
	}
	
	public ItemModel updateUserInfo(User userInfo) {
		Response response = new Response();
		ItemModel items = new ItemModel();
		RestTemplate restTemplate = new RestTemplate();
	    items = restTemplate.postForObject(Route.basePath+Route.editProfileInfoUrl,userInfo,ItemModel.class);
		return items;
	}

	@Override
	public Response createNewBid(Item item) {
		ItemModel items = new ItemModel();
		Response response = new Response();
		RestTemplate restTemplate = new RestTemplate();
	    response = restTemplate.postForObject(Route.basePath+Route.createNewBid,item,Response.class);
	    System.out.print("Received response");
		return response;
	}
	
}
