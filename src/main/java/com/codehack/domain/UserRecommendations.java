package com.codehack.domain;

import java.io.Serializable;
import java.util.Set;

public class UserRecommendations implements Serializable{
 /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
int user;
 Set<Integer> productSet;
 public UserRecommendations() {
	 
 }
 public  UserRecommendations (int user,Set<Integer> productSet) {
	 this.user = user;
	 this.productSet=productSet;
	 
 }
public int getUser() {
	return user;
}
public void setUser(int user) {
	this.user = user;
}
public Set<Integer> getProductSet() {
	return productSet;
}
public void setProductSet(Set<Integer> productSet) {
	this.productSet = productSet;
}
 
}
