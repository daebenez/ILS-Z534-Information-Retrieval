
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.Version;

import static java.util.concurrent.TimeUnit.SECONDS;
/*
 * @author : Team 2 
 * This class contains some helper functions used throughout the project
 * 
*/
class helperFunctions{
	
	public static void main(String[] args)throws IOException{
		MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
		DB db = mongoClient.getDB( "yelpDB" );
		//All collections
		Set<String> colls = db.getCollectionNames();
		//System.out.println(colls);
		//Collections that I need
		DBCollection indRes = db.getCollection("IndRes");
		DBCollection indRev = db.getCollection("IndRew");
		DBCollection pReview = db.getCollection("positive");
		DBCollection nReview = db.getCollection("negative");
		
		//Creating arraylist of Cities
		List<String> cities = new ArrayList<String>();
		cities = indRes.distinct("city");
		//System.out.println(cities);

		HashMap<String, ArrayList<String>> cityBussMap = new HashMap<String, ArrayList<String>>();
		HashMap<String, String> BussRevMap = new HashMap<String, String>();
		//cityBussMap = getCityBusMapping(indRes, cityBussMap);
		//System.out.println(cityBussMap);
		BussRevMap = getBusRevMapping(indRev, BussRevMap);
		//System.out.println(BussRevMap);
		//createReviewString(pReview, "output/positive.txt");
		//createReviewString(nReview, "output/negative.txt");
}
	//Mapping each city with business ids
	@SuppressWarnings("deprecation")
	public static HashMap<String, String> getBusRevMapping(DBCollection indRev, HashMap<String, String> BussRevMap) throws IOException {
		DBCursor cursor = indRev.find();
		
		//Custom stopword list
		ArrayList<String> stop_Words = new ArrayList<String>();		
		File file = new File("C:\\Users\\Vidhixa\\Desktop\\stopwords.txt");
		BufferedReader br = new BufferedReader(new FileReader(file));
		String line = br.readLine();
		while(line!=null){
			stop_Words.add(line);
			line = br.readLine();
		}
		CharArraySet stopSet = new CharArraySet(stop_Words, true);	
		
		try {
		   while(cursor.hasNext()) {
			   String current = " ";
			   DBObject obj = cursor.next();
			   String buss_id = ((String) obj.get("business_id"));
			   if(BussRevMap.containsKey(buss_id)){
				   current = BussRevMap.get(buss_id);
				   String s = (String) obj.get("text");
				   s.toLowerCase();
				   StringReader reader = new StringReader(s);
				   TokenStream tokens = new StandardTokenizer(reader);
				   tokens = new StopFilter(tokens, StandardAnalyzer.STOP_WORDS_SET);
				   tokens = new StopFilter(tokens, stopSet);
				   
				   StringBuilder sb = new StringBuilder();
			       OffsetAttribute offsetAttribute = tokens.addAttribute(OffsetAttribute.class);
			       CharTermAttribute charTermAttribute = tokens.addAttribute(CharTermAttribute.class);
			       tokens.reset();

			       while (tokens.incrementToken()) {
			           int startOffset = offsetAttribute.startOffset();
			           int endOffset = offsetAttribute.endOffset();
			           String term = charTermAttribute.toString();
			           current = current +term+" ";			           
			       }
				   BussRevMap.put(buss_id, current);
			   }else{
				   String s = (String) obj.get("text");
				   s.toLowerCase();
				   StringReader reader = new StringReader(s);  
				   TokenStream tokens = new StandardTokenizer(reader);
				   tokens = new StopFilter(tokens, StandardAnalyzer.STOP_WORDS_SET);
				   tokens = new StopFilter(tokens, stopSet);
				   StringBuilder sb = new StringBuilder();
			       OffsetAttribute offsetAttribute = tokens.addAttribute(OffsetAttribute.class);
			       CharTermAttribute charTermAttribute = tokens.addAttribute(CharTermAttribute.class);
			       tokens.reset();

			       while (tokens.incrementToken()) {
			           int startOffset = offsetAttribute.startOffset();
			           int endOffset = offsetAttribute.endOffset();
			           String term = charTermAttribute.toString();
			           current = current +term+" ";			           
			       }
				   BussRevMap.put(buss_id, current);
				   //System.out.println(buss_id+"  "+BussRevMap.get(buss_id));
			   }  
		  }
		   //System.out.println(BussRevMap);
		} finally {
		   cursor.close();
		}
		
		return BussRevMap;
	}
	
	//Mapping each city with business ids	
	public static HashMap<String, ArrayList<String>> getCityBusMapping(DBCollection indRes, HashMap<String, ArrayList<String>> cityBussMap) {
		DBCursor cursor = indRes.find();
		//DBCursor busCursor = busColl.find();
		try {
		   while(cursor.hasNext()) {
			   DBObject obj = cursor.next();
			   ArrayList<String> current = cityBussMap.get(obj.get("city"));
			  
			   //current.add((String) obj.get("business_id"));
			   if(current==null){
				   current = new ArrayList<String>();
				   //cityBussMap.put((String)obj.get("city"), current);
			   }
				  /* cityBussMap.put((String)obj.get("city"), cityBussMap.get(obj.get("city"))*/
			   current.add((String)obj.get("business_id"));
			   cityBussMap.put((String)obj.get("city"), current);
		   }
		  // System.out.println(cityBussMap);
		} finally {
		   cursor.close();
		}
		
		return cityBussMap;
	}}
	
		
	/*	public static void createReviewString(DBCollection coll, String name) throws IOException{
			
		File writeFile = new File(name);
		BufferedWriter writer = new BufferedWriter(new FileWriter(writeFile));
		
		DBCursor cursor = coll.find();
		String textReview = null ;
		try {
		   while(cursor.hasNext()) {
			   DBObject obj = cursor.next();
			   writer.write((String)obj.get("text"));
		   }
		}finally {
			cursor.close();
		}
		writer.close();
}
}*/

