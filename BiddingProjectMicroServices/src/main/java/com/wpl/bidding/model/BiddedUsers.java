/**
 * 
 */
package com.wpl.bidding.model;

/**
 * @author Sushma
 *
 */
public class BiddedUsers {
	
	public BiddedUsers(String firstName, String email, String itemName) {
		super();
		this.firstName = firstName;
		this.email = email;
		this.itemName = itemName;
	}
	String firstName;
	String email;
	String itemName;
	public String getFirstName() {
		return firstName;
	}
	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}
	public String getEmail() {
		return email;
	}
	public void setEmail(String email) {
		this.email = email;
	}
	public String getItemName() {
		return itemName;
	}
	public void setItemName(String itemName) {
		this.itemName = itemName;
	}
	
	

}
