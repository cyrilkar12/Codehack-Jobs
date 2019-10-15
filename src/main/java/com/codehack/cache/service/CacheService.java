package com.codehack.cache.service;

import java.util.List;
import java.util.Map;

import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Service
public class CacheService {
	@Cacheable(
		      value = "fpgrowth", 
		      key = "'fp'" 
		      )
		    public Map<String,String> fpGrowthCache( Map<String,String> predictions) {
		System.out.println("result map size>>>>>>>>>>>"+predictions.size());
		        return  predictions;
		    }
	
	@Cacheable(
		      value = "collabrationfilter", 
		      key = "'cf'" 
		      )
		    public Map<String,String> collabrationCatch(Map<String,String> predictions) {
		System.out.println("CF result map size>>>>>>>>>>>"+predictions.size());
		        return  predictions;
		    }

}
