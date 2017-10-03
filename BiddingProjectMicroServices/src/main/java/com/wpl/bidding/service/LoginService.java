package com.wpl.bidding.service;

import com.wpl.bidding.persist.User;

public interface LoginService {

	public User register(User user);
	User login(String username, String password, String location);
	
}
