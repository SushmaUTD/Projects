/**
 * 
 */
package com.wpl.bidding.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import com.wpl.bidding.common.Route;
import com.wpl.bidding.model.Item;
import com.wpl.bidding.model.ItemModel;

/**
 * @author Sushma
 *
 */
@Controller
public class BidController {
	
	List<Item> items = new ArrayList<Item>();

	@RequestMapping(value = Route.bidForItem, method = RequestMethod.GET,produces="application/json")	
	public @ResponseBody ItemModel bidItem(@RequestParam(value = "bidValue") float bidValue, @RequestParam(value="userId") int userId, @RequestParam(value="itemId") int itemId) {
		ItemModel res = new ItemModel();
		RestTemplate restTemplate = new RestTemplate();
		UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(Route.basePath+Route.bidForItem);
		builder.queryParam("bidValue", bidValue);
		builder.queryParam("userId", userId);
		builder.queryParam("itemId", itemId);
	    res = restTemplate.getForObject(builder.build().encode().toUri(), ItemModel.class);	
		return res;
	}
	
	@RequestMapping(value = "/purchaseHistory", method = RequestMethod.GET)
	@ResponseBody
	public ItemModel purchaseItem(@RequestParam("userId") int userId) {
		ItemModel itemResponse = new ItemModel();
		System.out.print("Request received");
		RestTemplate restTemplate = new RestTemplate();
		UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(Route.basePath+"/purchaseHistory");
		builder.queryParam("userId", userId);
		itemResponse = restTemplate.getForObject(builder.build().encode().toUri(), ItemModel.class);	
		return itemResponse;
	}
	
	@RequestMapping(value = "/closeBid", method = RequestMethod.GET,produces="application/json")	
	public @ResponseBody ItemModel closeBid(@RequestParam(value="userId") int userId, @RequestParam(value="itemId") int itemId) {
		ItemModel item = new ItemModel();
		RestTemplate restTemplate = new RestTemplate();
		UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(Route.basePath+Route.closeBid);
		builder.queryParam("userId", userId);
		builder.queryParam("itemId", itemId);
		item = restTemplate.getForObject(builder.build().encode().toUri(), ItemModel.class);
		return item;
	}
	
	
	@RequestMapping(value= "/myPostedItems",method = RequestMethod.GET)
	public @ResponseBody ItemModel myPostedItems(@RequestParam(value = "userId") int userId,HttpServletRequest request,ModelMap model) throws IOException
	{	
			ItemModel itemResponse = new ItemModel();
			RestTemplate restTemplate = new RestTemplate();
			UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(Route.basePath+Route.myPostedItems);
			builder.queryParam("userId", userId);
			itemResponse  = restTemplate.getForObject(builder.build().encode().toUri(), ItemModel.class);
			return itemResponse;
	}
	
	
	@RequestMapping(value= "/myBids",method = RequestMethod.GET)
	public @ResponseBody ItemModel myBidItems(@RequestParam(value = "userId") int userId,HttpServletRequest request,ModelMap model) throws IOException
	{	
			ItemModel itemResponse = new ItemModel();
			RestTemplate restTemplate = new RestTemplate();
			UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(Route.basePath+Route.myBids);
			builder.queryParam("userId", userId);
			itemResponse  = restTemplate.getForObject(builder.build().encode().toUri(), ItemModel.class);
			return itemResponse;
	
	}
	
}
