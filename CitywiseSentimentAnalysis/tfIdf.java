

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
/*
 * @author : Team 2 
 * This class takes as input all the reviews and calculates TF-IDF value for top features
 * 
*/
public class tfIdf {
	
	public static void main(String[] args)throws IOException{
		MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
		DB db = mongoClient.getDB( "yelpDB" );
		DBCollection indRes = db.getCollection("IndRes");
		DBCollection indRev = db.getCollection("IndRew");
		DBCollection pReview = db.getCollection("positive");
		DBCollection nReview = db.getCollection("negative");
		
		//Creating arraylist of Cities
		List<String> cities = new ArrayList<String>();
		cities = indRes.distinct("city");

		//Getting maps City-Buss_id and Buss_id-Review
		helperFunctions obj = new helperFunctions();
		HashMap<String, ArrayList<String>> cityBussMap = new HashMap<String, ArrayList<String>>();
		HashMap<String, String> BussRevMap = new HashMap<String, String>();
		cityBussMap = obj.getCityBusMapping(indRes, cityBussMap);
		BussRevMap = obj.getBusRevMapping(indRev, BussRevMap);
		
		//Creating hashmap for adjective file
		countTopWords cobj = new countTopWords();
		File f = new File("C:\\Users\\Vidhixa\\Desktop\\adjectiveList.txt");
		Map<String, Integer> tfmap = new TreeMap<>();
		tfmap = cobj.fileToHash(f);		
		//tfmap = getTFvalue(indRev, tfmap);
		
		Map<String, Float> idfmap = new TreeMap<>();
		//idfmap = getIDFvalue(indRev, tfmap);
		
		DBCursor cursor = indRev.find();
		int totalDocs = indRev.find().count(); 
		Map<String, Float> tfidf = new TreeMap<String, Float>();
		tfidf = calculateTFIDF(tfmap, idfmap, totalDocs);

		//Citywise TF-IDF
		Iterator iter1 = cityBussMap.entrySet().iterator();
		Iterator iter2 = BussRevMap.entrySet().iterator();
		File f1 = new File("C:\\Users\\Vidhixa\\Desktop\\allCSV\\topWords\\TFIDFtopcity.txt");
		BufferedWriter buffwt = new BufferedWriter(new FileWriter(f1));
		
		while(iter1.hasNext()){
			Map.Entry keyValue = (Map.Entry)iter1.next();
			buffwt.write((String) keyValue.getKey());
			buffwt.newLine();
			System.out.println((String) keyValue.getKey());
			ArrayList<String> current_business = (ArrayList<String>) keyValue.getValue();
			
			Map<String, Integer> tfmapcity = new TreeMap<>();
			tfmapcity= cobj.fileToHash(f);
			Map<String, Float> idfmapcity = new TreeMap<>();
			int totalDocsCity = 0;
			
			for(String s: current_business){
				BasicDBObject query = new BasicDBObject("business_id", s);
				DBCursor c = indRev.find(query);
			   while(c.hasNext()) {
				   ++totalDocsCity;
				   DBObject o1 = c.next();
				   ArrayList<String> terms = new ArrayList<String>();
				   terms = countTopWords.tokenizeAndFilter((String) o1.get("text"));
				   	for (String str: terms){
					  if(tfmapcity.containsKey(str)){
						  int count = tfmap.get(str);
						  count = count+1;
						  tfmapcity.put(str, count);		    
					  }else{
						  continue;
					  }  
				   }
		   }}
			
			idfmapcity = copyHashMap(idfmapcity, tfmapcity);
			
			//Calculating IDF
			for(String s: current_business){
				BasicDBObject query = new BasicDBObject("business_id", s);
				DBCursor c = indRev.find(query);
				while(c.hasNext()) {
				   DBObject o1 = c.next();
				   String review = (String) o1.get("text");
				   Iterator<Entry<String, Float>> it = idfmapcity.entrySet().iterator();
				   while(it.hasNext()){
						Map.Entry kv = (Map.Entry)it.next();
						String word = (String) kv.getKey();
						if(review.contains(word)){
							float val = idfmapcity.get(kv.getKey());
							//System.out.println(val);
							val = val+ 1;
							idfmapcity.put((String) kv.getKey(),val);
						}
					}
			   }
		   }
			
			//Calulating TF-IDF
			idfmapcity = calculateTFIDF(tfmapcity, idfmapcity, totalDocsCity );
			
		//Top feature extract
		@SuppressWarnings("rawtypes")
		Iterator w = valueIteratorFloat(idfmapcity);
	    while(w.hasNext()) {
	    	@SuppressWarnings("rawtypes")
			Map.Entry pairs = (Map.Entry)w.next();
	    	buffwt.write((String)pairs.getKey()); 
	    	buffwt.newLine();
	    }
	   // System.out.println("\n");
		}
		buffwt.close();
}
	
	public static Map<String, Integer> getTFvalue(DBCollection coll,Map<String, Integer> tfmap) throws IOException{
		DBCursor cursor = coll.find();
		countTopWords cobj = new countTopWords();

	   while(cursor.hasNext()) {
		   DBObject obj = cursor.next();
		   ArrayList<String> terms = new ArrayList<String>();
		   terms = cobj.tokenizeAndFilter((String) obj.get("text"));
		   
		   	for (String s: terms){
			  if(tfmap.containsKey(s)){
				  int count = tfmap.get(s);
				  count = count+1;
				  tfmap.put(s, count);		    
			  }else{
				  continue;
			  }  
		   }
	   }
	@SuppressWarnings("rawtypes")
	Iterator w = countTopWords.valueIterator(tfmap);
    while(w.hasNext()) {
    	@SuppressWarnings("rawtypes")
		Map.Entry pairs = (Map.Entry)w.next();
    	//System.out.println(cnt +"\t"+pairs.getKey() +"\t"+pairs.
    }
	return tfmap;
}
	
	public static Map<String, Float> getIDFvalue(DBCollection coll, Map<String, Integer> tfmap) throws IOException{
		DBCursor cursor = coll.find();
		countTopWords cobj = new countTopWords();
		Map<String, Float> idfmap = new TreeMap<String, Float>();
		idfmap = copyHashMap(idfmap, tfmap);
		 while(cursor.hasNext()) {
			   DBObject obj = cursor.next();
			   String review = (String) obj.get("text");
			   Iterator i = tfmap.entrySet().iterator();
				while(i.hasNext()){
					Map.Entry keyValue = (Map.Entry)i.next();
					String word = (String) keyValue.getKey();
					if(review.contains(word)){
						float val = idfmap.get(keyValue.getKey());
						//System.out.println(val);
						val = val+ 1;
						idfmap.put((String) keyValue.getKey(),val);
					}
				}
		   }
		 return idfmap;
	}
		
	public static Map<String, Float> calculateTFIDF(Map<String, Integer> tfmap, Map<String, Float> idfmap, int totalDocs){
		Iterator i = tfmap.entrySet().iterator();
		Map<String, Float> tfidf = new TreeMap<String, Float>();
		while(i.hasNext()){
			Map.Entry keyValue = (Map.Entry)i.next();
			int tf = (int) keyValue.getValue();
			float idf = idfmap.get(keyValue.getKey());
			idf = (float) Math.log10(1+(totalDocs/idf));
			
			if(tf == 0 && idf == 0){
				//Do nothing
			}else{
				float multiply = tf*idf;
				tfidf.put((String) keyValue.getKey(),multiply);			
			}
		}
		return tfidf;		
	}
	
	
	public static Map<String, Float> copyHashMap(Map<String, Float> idfmap,Map<String, Integer> tfmap){
		Iterator i = tfmap.entrySet().iterator();
		while(i.hasNext()){
			Map.Entry keyValue = (Map.Entry)i.next();
			idfmap.put((String) keyValue.getKey(),0f);
		}
		return idfmap;	
	}
	
	public static Iterator valueIteratorFloat(Map<String, Float> map) {
		
	     @SuppressWarnings("unchecked")
	     Set set = new TreeSet(new Comparator<Map.Entry<String, Float>>() {
	     @Override
	     public int compare(Entry<String, Float> o1, Entry<String, Float> o2) {
	    	 return  o1.getValue().compareTo(o2.getValue()) > 0 ? -1 : 1;
	      }
	     });
	     set.addAll(map.entrySet());
	     return set.iterator();
	}
}