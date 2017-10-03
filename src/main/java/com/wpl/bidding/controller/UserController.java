/**
 * 
 */
package com.wpl.bidding.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.wpl.bidding.common.ConstantMessages;
import com.wpl.bidding.common.Route;
import com.wpl.bidding.model.Item;
import com.wpl.bidding.model.ItemModel;
import com.wpl.bidding.model.Response;
import com.wpl.bidding.model.UserModel;
import com.wpl.bidding.persist.User;
import com.wpl.bidding.service.UserService;

/**
 * @author Sushma
 *
 */
@Controller
public class UserController {
	
	@Autowired UserService userService;
	
	@RequestMapping(value = "/createNewBid", method = RequestMethod.POST)
	public @ResponseBody ItemModel createNewBidItem(@RequestBody Item item) {
		System.out.print("Request received Bidding");
		ItemModel items = new ItemModel();
		List<Item> itemList = new ArrayList<Item>();
		Map<String,Object> map = new HashMap<String, Object>();
		Response response = new Response();
		try{
		response = userService.createNewBid(item);
		map.put("response", response);
		items.setResponse(response);
		items.setItemList(itemList);
		return items;
	} catch (Exception e) {
		e.printStackTrace();
	}
		return items;
	}
	
	@RequestMapping(value = Route.getUserInfo, method = RequestMethod.GET,produces="application/json")	
	public @ResponseBody UserModel getUserInfo(HttpServletRequest request,@RequestParam(value = "userId") int userId) {

		UserModel userModel = new UserModel();
		User user = new User();
		if (userId == 0)
		{
			userModel.setResponse(new Response(ConstantMessages.MandatoryParamsMissing, ConstantMessages.mandatoryParameterMissingCode));
			userModel.setUserDetails(user);
			return userModel;
		}
		else {
			userModel = userService.getUserInfo(userId);
		}
		return userModel;
	}
	
	@RequestMapping(value = Route.getInfo, method = RequestMethod.GET,produces="application/json")	
	public @ResponseBody UserModel getProfileInfo(HttpServletRequest request,@RequestParam(value = "userId") int userId) {
		String xHeader=request.getHeader("accessToken");
		UserModel userModel = new UserModel();
		User user = new User();
		if(!xHeader.equalsIgnoreCase("sushma"))
		{
			userModel.setResponse(new Response("Authorization is Not Provided", ConstantMessages.mandatoryParameterMissingCode));
			userModel.setUserDetails(user);
			return userModel;
		}
		if (userId == 0)
		{
			userModel.setResponse(new Response(ConstantMessages.MandatoryParamsMissing, ConstantMessages.mandatoryParameterMissingCode));
			userModel.setUserDetails(user);
			return userModel;
		}
		else {
			userModel = userService.getUserInfo(userId);
		}
		return userModel;
	}
	
	
	@RequestMapping(value = Route.editUserInfo, method = RequestMethod.POST,produces="application/json")	
	public @ResponseBody ItemModel editUserInfo(@RequestBody User userInfo) {
		Response response = new Response();
		ItemModel items = new ItemModel();
		if (userInfo.getUserId() == 0)
		{
			response.setResponseCode(ConstantMessages.mandatoryParameterMissingCode);
			response.setResponseMessage(ConstantMessages.MandatoryParamsMissing);
			return items;
		}
		else {
			items = userService.updateUserInfo(userInfo);
			}
		return items;
	}
	

}
