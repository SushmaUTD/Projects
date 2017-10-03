/**
 * 
 */
package com.wpl.bidding.dao;

import java.util.List;

import com.wpl.bidding.model.Items;
import com.wpl.bidding.persist.Item;

/**
 * @author Sushma
 *
 */
public interface SearchDao {
	
	public List<Item> searchItems(String keywords,int userId);

	List<Item> myPostedItems(int userId);

	List<Item> myBids(int userId);

}
