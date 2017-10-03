package com.wpl.bidding.service;

import org.springframework.web.client.RestTemplate;

import com.wpl.bidding.common.Route;
import com.wpl.bidding.model.Login;
import com.wpl.bidding.model.UserModel;
import com.wpl.bidding.persist.User;

/**
 * @author Sushma
 *
 */
public class LoginServiceImpl implements LoginService {
	
	@Override
	public UserModel login(Login userDetails) {
		UserModel customer = new UserModel();
		RestTemplate restTemplate = new RestTemplate();
	    customer = restTemplate.postForObject(Route.basePath+Route.loginUrl,userDetails, UserModel.class);
		return customer;
	}

	@Override
	public UserModel register(User user) {
		// TODO Auto-generated method stub
		UserModel customer = new UserModel();
		RestTemplate restTemplate = new RestTemplate();
	    customer = restTemplate.postForObject(Route.basePath+Route.registerUrl,user, UserModel.class);
		return customer;
	}
	
}
