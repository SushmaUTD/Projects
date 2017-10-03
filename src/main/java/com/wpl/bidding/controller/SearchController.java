/**
 * 
 */
package com.wpl.bidding.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.wpl.bidding.common.Route;
import com.wpl.bidding.model.ItemModel;
import com.wpl.bidding.service.SearchService;

/**
 * @author Sushma
 *
 */
@Controller
public class SearchController {
	
	@Autowired SearchService searchService;

	@RequestMapping(value = Route.searchItems, method = RequestMethod.GET,produces="application/json")	
	public @ResponseBody ItemModel searchItem(@RequestParam(value = "searchKeyword") String keyword,@RequestParam(value="userId") int userId) {
		ItemModel itemModel = new ItemModel();
		itemModel = searchService.searchItems(keyword, userId);
		return itemModel;
	}
}
