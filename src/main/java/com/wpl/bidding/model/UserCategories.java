/**
 * 
 */
package com.wpl.bidding.model;

/**
 * @author Sushma
 *
 */
public class UserCategories {
	
	int userId;
	String categoryName;
	int categoryLimit;
	
	public int getUserId() {
		return userId;
	}
	public void setUserId(int userId) {
		this.userId = userId;
	}
	public String getCategoryName() {
		return categoryName;
	}
	public void setCategoryName(String categoryName) {
		this.categoryName = categoryName;
	}
	public int getCategoryLimit() {
		return categoryLimit;
	}
	public void setCategoryLimit(int categoryLimit) {
		this.categoryLimit = categoryLimit;
	}
	
}
