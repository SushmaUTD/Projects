package com.wpl.bidding.service;

import com.wpl.bidding.model.Login;
import com.wpl.bidding.model.UserModel;
import com.wpl.bidding.persist.User;

public interface LoginService {

	public UserModel login(Login userDetails);
	public UserModel register(User user);
	
}
