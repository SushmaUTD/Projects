/**
 * 
 */
package com.wpl.bidding.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import com.wpl.bidding.model.Response;
import com.wpl.bidding.persist.Item;

/**
 * @author Sushma
 *
 */
@Controller
public class BidController {
	@Autowired
	UserDaoImpl userDao;
	
	@Autowired SearchDaoImpl searchDao;

	@RequestMapping(value = Route.bidForItem, method = RequestMethod.GET,produces="application/json")	
	public @ResponseBody ItemModel bidItem(@RequestParam(value = "bidValue") float bidValue, @RequestParam(value="userId") int userId, @RequestParam(value="itemId") int itemId) {
		Response response = new Response();
		ItemModel res = new ItemModel();
		Map<String,Object> map = new HashMap<String,Object>();
		response = userDao.bidForAnItem(bidValue, userId, itemId);
		System.out.print("Response Message"+response.getResponseCode());
		map.put("response", response);
		res.setResponse(response);
		return res;
	}	

	@RequestMapping(value = Route.closeBid, method = RequestMethod.GET,produces="application/json")	
	public @ResponseBody ItemModel closeBid(@RequestParam(value="userId") int userId, @RequestParam(value="itemId") int itemId) {
		Response response = new Response();
		ItemModel res = new ItemModel();
		response = userDao.closeBid(userId, itemId);
		Map<String,Object> map = new HashMap<String,Object>();
		map.put("response", response);
		res.setResponse(response);
		return res;
	}
	
	@RequestMapping(value= "/myPostedItems",method = RequestMethod.GET)
	public @ResponseBody ItemModel myPostedItems(@RequestParam(value = "userId") int userId,HttpServletRequest request,ModelMap model) throws IOException
	{	
		ItemModel itemResponse = new ItemModel();
		Response response = new Response();
		List<Item> items = new ArrayList<Item>();
	    items = searchDao.myPostedItems(userId);
	    if(items != null && items.size() > 0)
	    {
	    	response.setResponseCode("B200");
	    	response.setResponseMessage("My Posted Items");
	    	itemResponse.setItemList(items);
	    	itemResponse.setResponse(response);
	    }
	    else
	    {
	    	response.setResponseCode("B200");
	    	response.setResponseMessage("No Items posted yet");
	    	itemResponse.setItemList(items);
	    	itemResponse.setResponse(response);
	    }
	    return itemResponse;
	}	
}
