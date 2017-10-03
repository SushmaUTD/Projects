/**
 * 
 */
package com.wpl.bidding.service;

import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import com.wpl.bidding.common.Route;
import com.wpl.bidding.model.ItemModel;

/**
 * @author Sushma
 *
 */
public class SearchServiceImpl implements SearchService {

	@Override
	public ItemModel searchItems(String keywords,int userId) {
		
		ItemModel itemList = new ItemModel();
		RestTemplate restTemplate = new RestTemplate();
		UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(Route.basePath+Route.searchItems);
		builder.queryParam("searchKeyword", keywords);
		builder.queryParam("userId", userId);
	    itemList = restTemplate.getForObject(builder.build().encode().toUri(), ItemModel.class);
	    if(itemList.getItemList().size() > 0)
	    {
	    	System.out.print("Micro Service has returned correctly");
	    }
	    return itemList;
	}

}
