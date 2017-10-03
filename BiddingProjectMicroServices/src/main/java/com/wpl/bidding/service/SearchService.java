/**
 * 
 */
package com.wpl.bidding.service;

import java.util.List;

import com.wpl.bidding.model.Items;
import com.wpl.bidding.persist.Item;

/**
 * @author Sushma
 *
 */
public interface SearchService {
	
	public List<Item> searchItems(String keywords,int userId);

}
