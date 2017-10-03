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
@Table(name = "bidItem")
public class BidItem {
	@Id
	@Column(name="bidItemId")
	@GeneratedValue(strategy=GenerationType.AUTO)
	int bidItemId;
	
	public int getBidItemId() {
		return bidItemId;
	}

	public void setBidItemId(int bidItemId) {
		this.bidItemId = bidItemId;
	}

	@JoinColumn(name="itemId")
	@OneToOne
	Item itemId;
	
	@JoinColumn(name="userId")
	@OneToOne
	User userId;
	
	float bidValue;

	public Item getItemId() {
		return itemId;
	}

	public void setItemId(Item itemId) {
		this.itemId = itemId;
	}

	public User getUserId() {
		return userId;
	}

	public void setUserId(User userId) {
		this.userId = userId;
	}

	public float getBidValue() {
		return bidValue;
	}

	public void setBidValue(float bidValue) {
		this.bidValue = bidValue;
	}
}
