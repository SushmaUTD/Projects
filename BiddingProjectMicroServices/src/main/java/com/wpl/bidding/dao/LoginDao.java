package com.wpl.bidding.dao;

import com.wpl.bidding.persist.User;

public interface LoginDao {

	public User register(User user);
	User login(String userName, String password, String location);
}
