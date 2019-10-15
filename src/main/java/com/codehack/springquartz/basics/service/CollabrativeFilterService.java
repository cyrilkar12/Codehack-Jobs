package com.codehack.springquartz.basics.service;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.codehack.cache.service.CacheService;
import com.codehack.domain.UserRecommendations;

import scala.Tuple2;

@Service
public class CollabrativeFilterService {
	
	@Autowired
	CacheService cacheService; 
	
	public void executeCFService() {
	 	SparkConf conf = new SparkConf().setAppName("Collaborative_Filtering").setMaster("local");
	    JavaSparkContext jsc = new JavaSparkContext(conf);
		// Load and parse the data
		String path = "C:\\Cyril\\Eclipse-ws\\Codehack-Jobs\\src\\main\\resources\\mlib\\test_collabrative.data";
		JavaRDD<String> data = jsc.textFile(path);
		JavaRDD<Rating> ratings = data.map(s -> {
		  String[] sarray = s.split(",");
		  return new Rating(Integer.parseInt(sarray[0]),
		    Integer.parseInt(sarray[1]),
		    Double.parseDouble(sarray[2]));
		});

		// Build the recommendation model using ALS
		int rank = 10;
		int numIterations = 10;
		MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(ratings), rank, numIterations, 0.01);

		// Evaluate the model on rating data
		JavaRDD<Tuple2<Object, Object>> userProducts =
		  ratings.map(r -> new Tuple2<>(r.user(), r.product()));
		JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD.fromJavaRDD(
		  model.predict(JavaRDD.toRDD(userProducts)).toJavaRDD()
		      .map(r -> new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating()))
		);
		JavaRDD<Tuple2<Double, Double>> ratesAndPreds = JavaPairRDD.fromJavaRDD(
		    ratings.map(r -> new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating())))
		  .join(predictions).values();
		double MSE = ratesAndPreds.mapToDouble(pair -> {
		  double err = pair._1() - pair._2();
		  return err * err;
		}).mean();
		System.out.println(">>>>>>>>>>>>>>>Mean Squared Error = " + MSE);

		// Save and load model
	/*	model.save(jsc.sc(), "target/tmp/myCollaborativeFilter");
		MatrixFactorizationModel sameModel = MatrixFactorizationModel.load(jsc.sc(),
		  "target/tmp/myCollaborativeFilter");
	*/
		JavaRDD<Tuple2<Object, Rating[]>>  ratesAndPreds1 =  model.recommendProductsForUsers(5).toJavaRDD();
		
		JavaRDD<UserRecommendations> userRecommendationsRDD = ratesAndPreds1.map(tuple -> {
	        Set<Integer> products = new HashSet<>();
	        for (Rating rating : tuple._2) {
	            products.add(rating.product());
	        }

	        return new UserRecommendations((int) tuple._1(), products);
	    });
		
		List<UserRecommendations> lstRecomm = userRecommendationsRDD.collect();
		Map<String, String> resultMap = new HashMap<>();
		for(UserRecommendations user: lstRecomm) {
			System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>"+user.getUser()+">>>>>>>>>"+user.getProductSet());
			Set<String> productSetStr = user.getProductSet().stream() 
                    .map(String::valueOf) 
                    .collect(Collectors.toSet());
			String pagesStr = String.join(",", productSetStr);
			resultMap.put(user.getUser()+"",pagesStr);
			
		}
		cacheService.collabrationCatch(resultMap);
		/*System.out.println(">>>>>>>>>>>recommendation Array"+ratArray.length);
		for(Rating r: ratArray) {
			System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>"+r.product());
		}*/
		jsc.close();

	}

}
