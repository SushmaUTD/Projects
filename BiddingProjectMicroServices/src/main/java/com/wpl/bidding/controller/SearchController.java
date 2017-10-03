/**
 * 
 */
package com.wpl.bidding.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.wpl.bidding.common.ConstantMessages;
import com.wpl.bidding.common.Route;
import com.wpl.bidding.dao.SearchDaoImpl;
import com.wpl.bidding.dao.UserDaoImpl;
import com.wpl.bidding.model.ItemModel;
import com.wpl.bidding.model.Items;
import com.wpl.bidding.model.Response;
import com.wpl.bidding.persist.Item;
import com.wpl.bidding.service.SearchService;

/**
 * @author Sushma
 *
 */
@Controller
public class SearchController {
	
	@Autowired SearchService searchService;
	
	@Autowired UserDaoImpl userDao;
	
	@Autowired SearchDaoImpl searchDao;
	
	@RequestMapping(value= "/myBids",method = RequestMethod.GET)
	public @ResponseBody ItemModel myBidItems(@RequestParam(value = "userId") int userId,HttpServletRequest request,ModelMap model) throws IOException
	{	
		ItemModel itemResponse = new ItemModel();
		Response response = new Response();
		List<Item> items = new ArrayList<Item>();
	    items = searchDao.myBids(userId);
	    if(items != null && items.size() > 0)
	    {
	    	response.setResponseCode("B200");
	    	response.setResponseMessage("My Bids");
	    	
	    	itemResponse.setItemList((List<Item>)items);
	    	itemResponse.setResponse(response);
	    }
	    else
	    {
	    	response.setResponseCode("B200");
	    	response.setResponseMessage("No Bids Yet");
	    	itemResponse.setItemList(items);
	    	itemResponse.setResponse(response);
	    }
	    return itemResponse;
	}	

	@RequestMapping(value = Route.searchItems, method = RequestMethod.GET,produces="application/json")	
	public @ResponseBody ItemModel searchItem(@RequestParam(value = "searchKeyword") String keyword,@RequestParam(value="userId") int userId) {
		List<Item> biddingItems = new ArrayList<Item>();
		Response response = new Response();
		ItemModel itemModel = new ItemModel();
			if (keyword == null)
			{
				response.setResponseCode(ConstantMessages.mandatoryParameterMissingCode);
				response.setResponseMessage(ConstantMessages.noSearchResult);
				//Response to the search api
				itemModel.setResponse(response);
				itemModel.setItemList(biddingItems);
			}
			else
			{
					biddingItems = searchService.searchItems(keyword, userId);
					//Response to the search result
					response.setResponseCode(ConstantMessages.successCode);
					response.setResponseMessage(ConstantMessages.foundResult);
			
			}
			response.setResponseCode(ConstantMessages.successCode);
			response.setResponseMessage(ConstantMessages.foundResult);
			itemModel.setResponse(response);
			itemModel.setItemList(biddingItems);
		return itemModel;
	}
}
