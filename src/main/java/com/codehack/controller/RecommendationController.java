package com.codehack.controller;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.cache.CacheManager;

import com.codehack.domain.User;
import com.codehack.domain.VisitedPages;

@RestController
@RequestMapping("/cachecontroller")
public class RecommendationController {
	
	@Autowired
	CacheManager cacheManager;
	
	@RequestMapping(value = "/getNextPages",
			method = RequestMethod.POST,produces = "application/json")
	 @ResponseStatus(HttpStatus.CREATED)
	@ResponseBody
	public List<String> getFPGrowthCache(@RequestBody VisitedPages visitedPages){
		List<String> resultList = new ArrayList<String>();
		System.out.println(">>>>>>>>inside the service>>>>>>>>>>.");
		String sessionVisitedPages = String.join(",", visitedPages.getVisitedPages());
		System.out.println(">>>>>>>>inside the service>>>>>>>>>>."+sessionVisitedPages);
		org.springframework.cache.Cache cache = cacheManager.getCache("fpgrowth");
		Map<String,String> resultMap = null;
		if(cache.get("fp")!=null) {
		Object ob = cache.get("fp").get();
		resultMap = (Map<String,String>)  ob;
		if(resultMap!=null) {
		Set<Entry<String, String>> entrySet = resultMap.entrySet();
		if(entrySet.isEmpty()) {
			System.out.println("<<<<<<<<<<<entry set is empty>>>>>>>>>>>>");
		}
		/*for(Entry entry :entrySet) {
			System.out.println(">>>>>entry set key>>>>>>>>"+entry.getKey());
			System.out.println(">>>>>entry set value>>>>>>>>"+entry.getValue());
		}*/
		String fpPredictions = resultMap.get(sessionVisitedPages);
		if(fpPredictions!=null) {
			fpPredictions = fpPredictions.replaceAll(",$","");
			resultList =Arrays.asList(fpPredictions.split(","));
			System.out.println(">>>>>>predictions found>>>>"+fpPredictions);
		}else {
			System.out.println(">>>>>>predictions not found>>>>");
		}
		}else {
		System.out.println(">>>>>>>>>entry set is null>>>>>>");
		}
		}else {
			System.out.println(">>>>>>>>>>>>>>>>>>cache empty>>>>>>>>>>>>>>");
		}
		return resultList;
	}
	

	@RequestMapping(value = "/getRecommendedRatings",
			method = RequestMethod.POST,produces = "application/json")
	 @ResponseStatus(HttpStatus.CREATED)
	@ResponseBody
	public List<String> getFPGrowthCache(@RequestBody User user){
		List<String> resultList = new ArrayList<String>();
		System.out.println(">>>>>>>>inside the service>>>>>>>>>>.");
		String customerId= user.getUserId();
		System.out.println(">>>>>>>>inside the service>>>>>>>>>>."+customerId);
		org.springframework.cache.Cache cache = cacheManager.getCache("collabrationfilter");
		Map<String,String> resultMap = null;
		if(cache.get("cf")!=null) {
		Object ob = cache.get("cf").get();
		resultMap = (Map<String,String>)  ob;
		if(resultMap!=null) {
		Set<Entry<String, String>> entrySet = resultMap.entrySet();
		if(entrySet.isEmpty()) {
			System.out.println("<<<<<<<<<<<entry set is empty>>>>>>>>>>>>");
		}
		/*for(Entry entry :entrySet) {
			System.out.println(">>>>>entry set key>>>>>>>>"+entry.getKey());
			System.out.println(">>>>>entry set value>>>>>>>>"+entry.getValue());
		}*/
		
		String cfPredictions = resultMap.get(customerId);
		if(cfPredictions!=null) {
			resultList = Arrays.asList(cfPredictions.split(","));
			System.out.println(">>>>>>predictions found>>>>"+cfPredictions);
		}else {
			System.out.println(">>>>>>predictions not found>>>>");
		}
		}else {
		System.out.println(">>>>>>>>>entry set is null>>>>>>");
		}
		}else {
			System.out.println(">>>>>>>>>>>>>>>>>>cache empty>>>>>>>>>>>>>>");
		}
		return resultList;
	}

	
}
