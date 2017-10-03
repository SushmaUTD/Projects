package com.wpl.bidding.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.wpl.bidding.common.ConstantMessages;
import com.wpl.bidding.common.Route;
import com.wpl.bidding.model.Login;
import com.wpl.bidding.model.Response;
import com.wpl.bidding.model.UserModel;
import com.wpl.bidding.persist.User;
import com.wpl.bidding.service.LoginServiceImpl;
import com.wpl.bidding.utils.EncryptionUtils;

@Controller
public class LoginController {

	@Autowired
	LoginServiceImpl loginService;
	
	@RequestMapping(value = Route.registerPage, method = RequestMethod.POST,produces="application/json")	
	public @ResponseBody UserModel register(@RequestBody User user) {
		UserModel userModel = new UserModel();
		User newUser = new User();
		newUser = loginService.register(user);
			if (user == null)
			{
				userModel.setResponse(new Response(ConstantMessages.registerFailureMessage,ConstantMessages.emptyResponseCode));
			}
			else
			{
				userModel.setResponse(new Response(ConstantMessages.creationMessage,ConstantMessages.creationCode));	
			}
		userModel.setUserDetails(newUser);	
		return userModel;
	}

	@RequestMapping(value = Route.loginPage, method = RequestMethod.POST,produces="application/json")	
	public @ResponseBody UserModel homePage(@RequestBody Login userDetails) {
		UserModel userModel = new UserModel();
		User user = new User();
			user = loginService.login(userDetails.getUsername(), userDetails.getPassword(),userDetails.getLocation());
			if (user == null)
			{
				userModel.setResponse(new Response(ConstantMessages.loginFailureMessage,ConstantMessages.emptyResponseCode));
			}
			else
			{
				userModel.setResponse(new Response(ConstantMessages.loginSuccessMessage,ConstantMessages.successCode));	
			}
		userModel.setUserDetails(user);	
		return userModel;
	}
}
