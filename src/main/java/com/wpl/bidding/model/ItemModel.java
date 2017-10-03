/**
 * 
 */
package com.wpl.bidding.model;

import java.util.List;

/**
 * @author Sushma
 *
 */
public class ItemModel {
	
	public Response response;
	public List<Item> itemList;
	public Response getResponse() {
		return response;
	}
	public void setResponse(Response response) {
		this.response = response;
	}
	public List<Item> getItemList() {
		return itemList;
	}
	public void setItemList(List<Item> itemList) {
		this.itemList = itemList;
	}

}
