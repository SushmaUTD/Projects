/**
 * 
 */
package com.wpl.bidding.persist;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;
import javax.persistence.Table;
/**
 * @author Sushma
 *
 */
@Entity
@Table(name="user_bid")
public class UserBids {
	
	@Id
	@Column(name="userbid_Id")
	@GeneratedValue(strategy=GenerationType.AUTO)
	Integer userbid_Id;
	
	@OneToOne
	@JoinColumn(name="userId")
	User userId;
	
	@OneToOne
	@JoinColumn(name="itemId")
	Item itemId;
	
	public Integer getUserbid_Id() {
		return userbid_Id;
	}
	public void setUserbid_Id(Integer userbid_Id) {
		this.userbid_Id = userbid_Id;
	}
	public User getUserId() {
		return userId;
	}
	public void setUserId(User userId) {
		this.userId = userId;
	}
	public Item getItemId() {
		return itemId;
	}
	public void setItemId(Item itemId) {
		this.itemId = itemId;
	}

}
