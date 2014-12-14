

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.tartarus.snowball.ext.PorterStemmer;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
/*
 * @author : Team 2 
 * This class computes the top occurring words. This was an intermediate stage before we did TF-IDF for feature vector
 * Produced interesting top words for every city.
*/
public class countTopWords {
	
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
		
		//Global sentiment top words
		Map<String, Integer> nmap = new TreeMap<>();
		Map<String, Integer> pmap = new TreeMap<>();		
		ArrayList<String> topGlobal= new ArrayList<String>();
		File f = new File("C:\\Users\\Vidhixa\\Desktop\\negative-words.txt");
		nmap = fileToHash(f);
		topGlobal = findGobalTopWords(pReview, pmap, 30);
		
		//Local sentiment top words
		findLocalTopWords(BussRevMap, cityBussMap, nmap);
	}
	
	public static Map fileToHash(File f) throws IOException{
		Map<String, Integer> nmap = new TreeMap<>();
		BufferedReader br = new BufferedReader(new FileReader(f));
		String line = br.readLine();
		while(line!=null){
			if(nmap.containsKey(line)){
				//do nothing
			}else{
				nmap.put(line, 0);						
			}
				
			line = br.readLine();
		}
		return nmap;
	}

	public static ArrayList<String> findGobalTopWords(DBCollection coll, Map<String, Integer> adjMap, int n) throws IOException{	
		DBCursor cursor = coll.find();
		ArrayList<String> topWords = new ArrayList<String>();
		//Map<String,Integer> global = new TreeMap<String,Integer>();
	   while(cursor.hasNext()) {
		   DBObject obj = cursor.next();
		   ArrayList<String> terms = new ArrayList<String>();
		   terms = tokenizeAndFilter((String) obj.get("text"));
		   
		   	for (String s: terms){
			  if(adjMap.containsKey(s)){
				  int count = adjMap.get(s);
				  count = count+1;
				  adjMap.put(s, count);		    
			  }else{
				  continue;
			  }  
		   }
	   }
	int cnt=1;
	@SuppressWarnings("rawtypes")
	Iterator w = valueIterator(adjMap);
    while(w.hasNext() &&cnt<n) {
    	
    	@SuppressWarnings("rawtypes")
		Map.Entry pairs = (Map.Entry)w.next();
        	
    	System.out.println(cnt +"\t"+pairs.getKey() +"\t"+pairs.getValue());
    	//((String)pairs.getKey(),(Integer)pairs.getValue());
    	cnt++;
    }
	return topWords;
}
	
	public static void findLocalTopWords(HashMap<String, String> BussRevMap,HashMap<String, ArrayList<String>> cityBussMap, Map<String, Integer> adjMap)throws IOException{	
		Iterator i1 = cityBussMap.entrySet().iterator();
		Iterator i2 = BussRevMap.entrySet().iterator();
		
		while(i1.hasNext()){
			Map.Entry keyValue = (Map.Entry)i1.next();
			System.out.println((String) keyValue.getKey());
			ArrayList<String> current_business = (ArrayList<String>) keyValue.getValue();

			for(String s: current_business){
				String reviews = BussRevMap.get(s);
				if(reviews == null){
					continue;
				}else{
					String[] words = reviews.split(" "); 
				
					for (String word : words){  
						if(adjMap.containsKey(word)){
						  int count = adjMap.get(word);
						  count = count+1;
						  adjMap.put(word, count);		    
						 }else{
							  continue;
						  }}}}
			int cnt=1;
			@SuppressWarnings("rawtypes")
			Iterator w = valueIterator(adjMap);
		    while(w.hasNext() &&cnt<30) {
		    	
		    	@SuppressWarnings("rawtypes")
				Map.Entry pairs = (Map.Entry)w.next();
		        	
		    	System.out.println(cnt +"\t"+pairs.getKey() +"\t"+pairs.getValue());
		    	//((String)pairs.getKey(),(Integer)pairs.getValue());
		    	cnt++;
		    }
		}
	int cnt=1;
	@SuppressWarnings("rawtypes")
	Iterator w = valueIterator(adjMap);
    while(w.hasNext() &&cnt<30) {
    	
    	@SuppressWarnings("rawtypes")
		Map.Entry pairs = (Map.Entry)w.next();
        	
    	//System.out.println(cnt +"\t"+pairs.getKey() +"\t"+pairs.getValue());
    	//((String)pairs.getKey(),(Integer)pairs.getValue());
    	cnt++;
    }

}
	
	//Ranking of words based on frequency
	@SuppressWarnings("unchecked")
	public static Iterator valueIterator(Map<String, Integer> map) {
		
	     @SuppressWarnings("unchecked")
	     Set set = new TreeSet(new Comparator<Map.Entry<String, Integer>>() {
	     @Override
	     public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
	    	 return  o1.getValue().compareTo(o2.getValue()) > 0 ? -1 : 1;
	      }
	     });
	     set.addAll(map.entrySet());
	     return set.iterator();
	}
	
	public static ArrayList<String> tokenizeAndFilter(String s) throws IOException{
		ArrayList<String> terms = new ArrayList<String>();		
		//Custom Stop words list
		ArrayList<String> stop_Words = new ArrayList<String>();		
		File file = new File("C:\\Users\\Vidhixa\\Desktop\\stopwords.txt");
		BufferedReader br = new BufferedReader(new FileReader(file));
		String line = br.readLine();
		while(line!=null){
			stop_Words.add(line);
			line = br.readLine();
		}
	   CharArraySet stopSet = new CharArraySet(stop_Words, true);
	   
	   //Tokenizing and filtering words
	   s.toLowerCase(); 
	   StringReader reader = new StringReader(s);  
	   TokenStream tokens = new StandardTokenizer(reader);
	   tokens = new StopFilter(tokens, StandardAnalyzer.STOP_WORDS_SET);
	   tokens = new StopFilter(tokens, stopSet);
	   StringBuilder sb = new StringBuilder();
	   OffsetAttribute offsetAttribute = tokens.addAttribute(OffsetAttribute.class);
	   CharTermAttribute charTermAttribute = tokens.addAttribute(CharTermAttribute.class);
	   tokens.reset();      
	   
	   //Listing out the terms
	   while (tokens.incrementToken()) {
	       int startOffset = offsetAttribute.startOffset();
	       int endOffset = offsetAttribute.endOffset();
	       String term = charTermAttribute.toString();
	       terms.add(term);
	   }
	   return terms;
	}
}