package com.wpl.bidding.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.wpl.bidding.common.Route;
import com.wpl.bidding.model.Login;
import com.wpl.bidding.model.UserModel;
import com.wpl.bidding.persist.User;
import com.wpl.bidding.service.LoginServiceImpl;

@Controller
public class LoginController {

	@Autowired
	LoginServiceImpl loginService;
	
	@RequestMapping(value = Route.registerPage, method = RequestMethod.POST,produces="application/json")	
	public @ResponseBody UserModel register(@RequestBody User user) {
		UserModel userModel = new UserModel();
		userModel = loginService.register(user);
		return userModel;
	}

	@RequestMapping(value = Route.loginPage, method = RequestMethod.POST,produces="application/json")	
	public @ResponseBody UserModel homePage(@RequestBody Login userDetails, ModelMap model) {
		UserModel userModel = new UserModel();
		userModel = loginService.login(userDetails);
		return userModel;
	}
}