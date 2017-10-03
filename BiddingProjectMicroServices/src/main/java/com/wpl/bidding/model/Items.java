/**
 * 
 */
package com.wpl.bidding.model;

import java.util.Date;

import com.wpl.bidding.persist.User;

/**
 * @author Sushma
 *
 */
public class Items {
	int itemId;
	String itemName;
	String itemDesc;
	float itemPrice;
	User userId;
	String imageUrl;
	Date createdDate;
	String status;
	
	
	public String getStatus() {
		return status;
	}
	public Date getCreatedDate() {
		return createdDate;
	}
	public void setCreatedDate(Date createdDate) {
		this.createdDate = createdDate;
	}
	public User getUserId() {
		return userId;
	}
	public String getImageUrl() {
		return imageUrl;
	}
	public void setImageUrl(String imageUrl) {
		this.imageUrl = imageUrl;
	}
	public int getItemId() {
		return itemId;
	}
	public void setItemId(int itemId) {
		this.itemId = itemId;
	}
	public String getItemName() {
		return itemName;
	}
	public void setItemName(String itemName) {
		this.itemName = itemName;
	}
	public String getItemDesc() {
		return itemDesc;
	}
	public void setItemDesc(String itemDesc) {
		this.itemDesc = itemDesc;
	}
	public float getItemPrice() {
		return itemPrice;
	}
	public void setItemPrice(float itemPrice) {
		this.itemPrice = itemPrice;
	}
	public User User() {
		return userId;
	}
	public void setUserId(User userId) {
		this.userId = userId;
	}
	public void setStatus(String string) {
		// TODO Auto-generated method stub
		
	}
}
