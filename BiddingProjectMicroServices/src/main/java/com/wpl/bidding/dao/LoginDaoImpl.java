/**
 * 
 */
package com.wpl.bidding.dao;
import java.util.Date;

import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.exception.ConstraintViolationException;
import org.springframework.beans.factory.annotation.Autowired;

import com.wpl.bidding.persist.User;

/**
 * @author Sushma
 *
 */
public class LoginDaoImpl implements LoginDao {
	@Autowired
	SessionFactory sessionFactory;

	protected Session getCurrentSession() {
		return sessionFactory.getCurrentSession();
	}

	@SuppressWarnings("unchecked")
	@Override
	public User login(String userName, String password, String location) {
		User results = new User();
		User temp = new User();
		try{
		Session session = sessionFactory.openSession();
		session.beginTransaction();
		Query query = session.createQuery("from User where userName='"
				+ userName + "' and password='" + password + "'");
		results = (User) query.list().get(0);
		session.getTransaction().commit();
		if(results != null)
		{
			temp = results;
			session.beginTransaction();
			temp.setLocation(location);
			temp.setModifiedDate(new Date(System.currentTimeMillis()));
			session.save(temp);
			session.getTransaction().commit();
		}
		}catch(Exception e)
		{
			System.out.print("Error while getting user"+e);
			results = null;
		}
		return results;
	}

	@Override
	public User register(User user) {
		try{
		Session session = sessionFactory.openSession();
		session.beginTransaction();
		user.setModifiedDate(new Date(System.currentTimeMillis()));
		user.setCreatedDate(new Date(System.currentTimeMillis()));
		session.save(user);
		session.getTransaction().commit();
		}catch(ConstraintViolationException ce){
			System.out.println("User Name Already exists");
			user = null;
		}
		catch(Exception e){
			System.out.println("Error while creating user"+e);
			user = null;
		}
		return user;
	}

}
