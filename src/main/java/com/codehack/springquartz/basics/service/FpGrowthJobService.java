package com.codehack.springquartz.basics.service;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.codehack.cache.service.CacheService;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class FpGrowthJobService {

    public static final long EXECUTION_TIME = 5000L;

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired(required=true)
    CacheService cacheService;

    public void executeFpGrowthJob() {
        Map<String,String> resultMap = new HashMap<>();
        logger.info("The sample job has begun...");
        try {
           	SparkConf conf = new SparkConf().setAppName("FP-Growth_ItemFrequency").setMaster("local");
            JavaSparkContext sc = new JavaSparkContext(conf);

            JavaRDD<String> data = sc.textFile("C:\\Cyril\\Eclipse-ws\\Codehack-Jobs\\src\\main\\resources\\mlib\\sample_fpgrowth1.txt");

        	JavaRDD<List<String>> transactions = data.map(line -> Arrays.asList(line.split(" ")));

        	FPGrowth fpg = new FPGrowth()
        	  .setMinSupport(0.3)
        	  .setNumPartitions(5);
        	FPGrowthModel<String> model = fpg.run(transactions);

    /*    	for (FPGrowth.FreqItemset<String> itemset: model.freqItemsets().toJavaRDD().collect()) {
        	  System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>[" + itemset.javaItems() + "], " + itemset.freq());
        	  
        	}*/

        	double minConfidence = 0.9;
        	for (AssociationRules.Rule<String> rule
        	  : model.generateAssociationRules(minConfidence).toJavaRDD().collect()) {
        		List<String> previouspageLst = rule.javaAntecedent();
        		String previouspages =  String.join(",", previouspageLst);
        		String predictions = String.join(",", rule.javaConsequent());
        		if(resultMap.get(previouspages)!=null) {
        			String resultFp = resultMap.get(previouspages);
        			resultMap.put(previouspages,  resultFp.concat(predictions)+",");
        		}else {
        			resultMap.put(previouspages, predictions+",");
        		}
        	  System.out.println(
        	    rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence());
        	}
        	sc.close();
        	cacheService.fpGrowthCache(resultMap);

        } catch (Exception e) {
            logger.error("Error while executing sample job", e);
        } finally {
            
            logger.info("Sample job has finished...");
        }
    }
   
}
