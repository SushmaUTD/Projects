/**
 * 
 */
package com.wpl.bidding.dao;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.wpl.bidding.model.Items;
import com.wpl.bidding.persist.BidItem;
import com.wpl.bidding.persist.Item;


/**
 * @author Sushma
 *
 */
public class SearchDaoImpl implements SearchDao {
	@Autowired
	SessionFactory sessionFactory;

	protected Session getCurrentSession() {
		return sessionFactory.getCurrentSession();
	}

	@Override
	public List<Item> searchItems(String keywords,int userId) {
		// TODO Auto-generated method stub
		List<Item> results = new ArrayList<Item>();
		try{
		Session session = sessionFactory.openSession();
		session.beginTransaction();
		Query query = session.createQuery("from Item where itemName like :keyword and itemDesc like :keyword and userId !="+userId);
		
		results = (List<Item>) query.setParameter("keyword", "%"+keywords+"%").list();
		session.getTransaction().commit();
		}catch(Exception e)
		{
			System.out.print("Error while getting user"+e);
			results = null;
		}
		return results;
	}
	
	@Override
	public List<Item> myBids(int userId) {
		Session session = sessionFactory.openSession();
		session.beginTransaction();
		List<BidItem> item = new ArrayList<BidItem>();
		List<Item> items = new ArrayList<Item>();
		Query query = session.createQuery("from BidItem bi where bi.userId ="+userId);
		item = (List<BidItem>)query.list();
		for(int i =0;i<item.size();i++)
			items.add(item.get(i).getItemId());
		session.getTransaction().commit();
		return items;
	}
	
	@Override
	public List<Item> myPostedItems(int userId) {
		Session session = sessionFactory.openSession();
		session.beginTransaction();
		List<Item> items = new ArrayList<Item>();
		Query query = session.createQuery("from Item i where userId ="+userId);
		items = (List<Item>)query.list();
		session.getTransaction().commit();
		return items;
	}

}
