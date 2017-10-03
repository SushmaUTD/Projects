/**
 * 
 */
package com.wpl.bidding.service;

import com.wpl.bidding.model.ItemModel;

/**
 * @author Sushma
 *
 */
public interface SearchService {
	
	public ItemModel searchItems(String keywords,int userId);

}
