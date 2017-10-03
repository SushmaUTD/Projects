/**
 * 
 */
package com.wpl.bidding.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.wpl.bidding.common.ConstantMessages;
import com.wpl.bidding.common.Route;
import com.wpl.bidding.dao.UserDaoImpl;
import com.wpl.bidding.model.ItemModel;
import com.wpl.bidding.model.Response;
import com.wpl.bidding.model.UserModel;
import com.wpl.bidding.persist.Item;
import com.wpl.bidding.persist.User;
import com.wpl.bidding.service.UserService;

/**
 * @author Sushma
 *
 */
@Controller
public class UserController {
	
	@Autowired UserService userService;
	
	@Autowired UserDaoImpl userDao;
	
	@RequestMapping(value = Route.getUserInfo, method = RequestMethod.GET,produces="application/json")	
	public @ResponseBody UserModel getUserInfo(@RequestParam(value = "userId") int userId) {
		UserModel userModel = new UserModel();
		User user = new User();
			user = userService.getUserInfo(userId);
			if (user == null)
			{
				userModel.setResponse(new Response(ConstantMessages.noUser,ConstantMessages.emptyResponseCode));
			}
			else
			{
				userModel.setResponse(new Response(ConstantMessages.foundUser,ConstantMessages.successCode));	
			}
		userModel.setUserDetails(user);	
		return userModel;
	}
	
	@RequestMapping(value = Route.getUserBiddedItems, method = RequestMethod.GET,produces="application/json")	
	public @ResponseBody UserModel getUserBiddedItems(@RequestParam(value = "userId") int userId) {
		UserModel userModel = new UserModel();
		User user = new User();
		
			user = userService.getUserInfo(userId);
			if (user == null)
			{
				userModel.setResponse(new Response(ConstantMessages.noUser,ConstantMessages.emptyResponseCode));
			}
			else
			{
				userModel.setResponse(new Response(ConstantMessages.foundUser,ConstantMessages.successCode));	
			}
		userModel.setUserDetails(user);	
		return userModel;
	}
	
	@RequestMapping(value = Route.editUserInfo, method = RequestMethod.POST,produces="application/json")	
	public @ResponseBody ItemModel getUserInfo(@RequestBody User userInfo) {
		Response response = new Response();
		ItemModel items = new ItemModel();
		List<Item> item = new ArrayList<Item>();
		int result;
			result = userService.updateUserInfo(userInfo);
			if (result == 0)
			{
				response.setResponseCode(ConstantMessages.emptyResponseCode);
				response.setResponseMessage(ConstantMessages.noUser);
			}
			else
			{
				response.setResponseCode(ConstantMessages.successCode);
				response.setResponseMessage(ConstantMessages.updatedUserInfo);
			}
			items.setItemList(item);
			items.setResponse(response);
		return items;
	}
	
	@RequestMapping(value = "/purchaseHistory", method = RequestMethod.GET)
	@ResponseBody
	public ItemModel purchaseItem(@RequestParam("userId") int userId) {
		List<Item> result = new ArrayList<Item>();
		ItemModel itemResponse = new ItemModel();
		Response response = new Response();
		System.out.print("Request received");
		try{
		
		result = userDao.purchaseHistory(userId);
		if(result != null )
		{
			response.setResponseCode(ConstantMessages.successCode);
			response.setResponseMessage(ConstantMessages.createdNewItem);
			itemResponse.setItemList(result);
			itemResponse.setResponse(response);
		}
		else{
			response.setResponseCode(ConstantMessages.serverErrorCode);
			response.setResponseMessage(ConstantMessages.technicalError);
			itemResponse.setItemList(result);
			itemResponse.setResponse(response);
		}
		
	} catch (Exception e) {
		e.printStackTrace();
	}
		return itemResponse;
	}

	
	
	
	@RequestMapping(value = "/createNewBid", method = RequestMethod.POST)
	@ResponseBody
	public  Response createNewBidItem(@RequestBody Item item) {
		Response response = new Response();
		ItemModel res = new ItemModel();
		List<Item> items = new ArrayList<Item>();
		System.out.print("Request received");
		try{
		
		int result = userService.createNewBid(item);
		if(result == 1)
		{
			response.setResponseCode(ConstantMessages.successCode);
			response.setResponseMessage(ConstantMessages.createdNewItem);
		}
		else{
			response.setResponseCode(ConstantMessages.serverErrorCode);
			response.setResponseMessage(ConstantMessages.technicalError);
		}
		
	} catch (Exception e) {
		e.printStackTrace();
	}
		res.setResponse(response);
		res.setItemList(items);
		return response;
	}

	
}
