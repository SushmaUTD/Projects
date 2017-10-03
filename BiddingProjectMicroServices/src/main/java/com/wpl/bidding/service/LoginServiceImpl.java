package com.wpl.bidding.service;

import org.springframework.beans.factory.annotation.Autowired;

import com.wpl.bidding.dao.LoginDaoImpl;
import com.wpl.bidding.persist.User;

/**
 * @author Sushma
 *
 */
public class LoginServiceImpl implements LoginService {
	
	@Autowired
	LoginDaoImpl loginDao;

	@Override
	public User login(String username, String password,String location) {
		User users = loginDao.login(username, password,location);
		return users;
	}

	@Override
	public User register(User user) {
		// TODO Auto-generated method stub
		User users = loginDao.register(user);
		return users;
	}
	 
	

}
