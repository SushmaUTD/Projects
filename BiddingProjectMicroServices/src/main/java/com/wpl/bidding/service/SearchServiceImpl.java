/**
 * 
 */
package com.wpl.bidding.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;

import com.wpl.bidding.dao.SearchDaoImpl;
import com.wpl.bidding.persist.Item;

/**
 * @author Sushma
 *
 */
public class SearchServiceImpl implements SearchService {

	@Autowired
	SearchDaoImpl searchDao;
	@Override
	public List<Item> searchItems(String keywords,int userId) {
		List<Item> items = searchDao.searchItems(keywords, userId);
		return items;
	}

}
