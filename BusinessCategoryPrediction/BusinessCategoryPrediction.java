/*
 * Final Project: Yelp Data Challenge: Task1: Predicting Business Categories
 * Team 2
 * Minal Kondawar
 * Vidhixa Joshi
 * David Ezeneber
 * *
 */





import com.mongodb.BasicDBList;
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
import java.io.PrintWriter;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.Version;



class BusinessCategoryPrediction{
	

	public static void main(String[] args)throws Exception{
		//hash map to store categories and corresponding to 100 top occcuring words 
		Map<String,Map<String,Integer>> hmap=new HashMap<String,Map<String,Integer>>();
		
		//connection string to connect to mongodb
		MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
		
		//extracting join table of Business and review fro yelpdb
		DB db = mongoClient.getDB( "yelpDB" );
		Set<String> all_database_names = db.getCollectionNames();
		DBCollection br = db.getCollection("BRTableRest");
		DBCursor cursor=br.find();
		String key;
		String value;
		List<String> categoryArray = new ArrayList<String>();
		categoryArray = br.distinct("value.categories");
		
		//noCat contains all the category which are irrelevant to restaurant categories
		Set<String> noCat = new HashSet<String>();
		
		//change path of file accordingly, file in folder
		File cat = new File("C:\\Users\\Minal\\Desktop\\noCat.txt");
		BufferedReader brcat = new BufferedReader(new FileReader(cat));
		String line = brcat.readLine();
				noCat.add(line);
		while(line!=null){
			noCat.add(line);
			line = brcat.readLine();
		}
		brcat.close();
		  
		//iterating over all the unique categories to get top 25 words using TF-IDF
		for (int i = 0; i <categoryArray.size(); i++) {
			
			Map<String,Integer> wordCount=new HashMap<String,Integer>();
			key=(String) categoryArray.get(i);
			if(!noCat.contains(key))
			{
				BasicDBObject query=new BasicDBObject("value.categories",key);
				BasicDBObject fields = new BasicDBObject();
				fields.put("value.reviewdata",1);
				DBCursor cur=br.find(query,fields);
				String reviewWholeData="";
				while(cur.hasNext()){
					BasicDBObject robj = (BasicDBObject) cur.next();
					reviewWholeData+=(String) (((DBObject)robj.get("value")).get("reviewdata"));
				}
			
				String reviewdata=reviewWholeData.toLowerCase();
				wordCount=removeStopWords(reviewdata);
				
				if(!hmap.containsKey(key)){
						hmap.put(key, wordCount);
				}
				
			System.out.println("Category"+ (i+1)+" "+key+" done with calculating TF !!");
			}
			
		}
				
		calculateTFIDF(br,hmap);
		
	}
	
	public static void calculateTFIDF(DBCollection br,Map<String,Map<String,Integer>> hmaptf) throws IOException{
		//File to write all features for feature vector
		
		//change the path accordingly
		PrintWriter writer = new PrintWriter("E:\\text.txt", "UTF-8");
		System.out.println("Calculating TF-IDF of all words of each category");
		Map<String,Map<String,Float>> hmapidf=new HashMap<String,Map<String,Float>>();
		Set<String> featurevector=new HashSet<String>();	
		Map<String,Float> temp=new TreeMap<String,Float>();
		Map<String,Float> wordCountidf=new TreeMap<String,Float>();
		int x=0;
		Iterator it=hmaptf.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry pairs=(Map.Entry)it.next();
			String categ=(String)pairs.getKey();
			temp=(Map<String, Float>) pairs.getValue();
			int num=0;
			String singleReview;
			Iterator itr=temp.entrySet().iterator();
			while(itr.hasNext()){
				
				BasicDBObject query=new BasicDBObject("value.categories",categ);
				BasicDBObject fields = new BasicDBObject();
				fields.put("value.reviewdata",1);
				Float totaldoc=(float) br.find(query,fields).count();
				DBCursor cr=br.find(query,fields);
				Map.Entry wcpairs=(Map.Entry)itr.next();
				String word=(String)wcpairs.getKey();
				Float freq=(float) 0;
				while(cr.hasNext()){
					
					BasicDBObject robj = (BasicDBObject) cr.next();
					singleReview=(String) (((DBObject)robj.get("value")).get("reviewdata"));
                    if(singleReview.toLowerCase().contains(word.toLowerCase())){
							freq++;
					}
					
				}
			
				Float idf= (float) (Math.log10( (1+(totaldoc/freq))));
				Integer tf=(Integer) wcpairs.getValue();
				Float tfidf=tf*idf;
				wordCountidf.put(word, tfidf);
				freq=(float) 0;
			}
		
			//extracting top 25 features: you can change according to requirement
			Map<String, Float> fmap = new TreeMap<>();
			int d=0;
		    Iterator w = valueIterator1(wordCountidf);
		    while(w.hasNext() && d<25 ) {
		    	
		    	@SuppressWarnings("rawtypes")
				Map.Entry p = (Map.Entry)w.next();
		        //System.out.println((String) p.getKey()); if want to see features of each category
		        featurevector.add((String) p.getKey());	
		       	d++;
		    }
		    System.out.println("Done with "+(++x)+" categories......");
				
		}
		
		for(String s:featurevector){
			writer.println(s);
			System.out.println(s);
		}
		
		System.out.println("Finished!!!!!!!!!!");
		writer.close();
	}
	
	
	/*
	 * function to remove stopwords-customize as well as built in lucene: EnglishAnalyzer
	*/
	@SuppressWarnings({ "resource", "deprecation" })
	public static Map<String, Integer> removeStopWords(String str) throws IOException{
		System.out.println("In remove stop word");
		Map<String,Integer> reviewWordCount=new HashMap<String,Integer>();
		ArrayList<String> stop_Words = new ArrayList<String>();	
		//change the path of file accordingly, file given in folder
		File file = new File("E:\\stopwords.txt");
		BufferedReader br = new BufferedReader(new FileReader(file));
		String line = br.readLine();
		while(line!=null){
			stop_Words.add(line);
			line = br.readLine();
		}
		CharArraySet stopSet = new CharArraySet(stop_Words, true);	
		CharArraySet stopWords = EnglishAnalyzer.getDefaultStopSet();
		TokenStream tokenStream = new StandardTokenizer(Version.LUCENE_48, new StringReader(str.trim()));

	    tokenStream = new StopFilter(Version.LUCENE_48, tokenStream, stopSet);	
	    tokenStream = new StopFilter(Version.LUCENE_48, tokenStream, stopWords);
	    tokenStream = new PorterStemFilter(tokenStream);
	    
	    StringBuilder sb = new StringBuilder();
	    CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);
	    tokenStream.reset();
	    while (tokenStream.incrementToken()) {
	        String term = charTermAttribute.toString();
	        term.replace("."," ");
	        sb.append(term + " ");
	    }
	    String tokenizedReview= sb.toString();
	    reviewWordCount=calculateFrequencyOfWords(tokenizedReview);
		
		return reviewWordCount;
	}

	
	//comparator function for hashmap
	@SuppressWarnings("unchecked")
	public static 	Iterator valueIterator(Map<String, Integer> map) {
		
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
	
	
	//comparator function for hashmap
	@SuppressWarnings("unchecked")
		public static 	Iterator valueIterator1(Map<String, Float> wordCountidf) {
			
		     @SuppressWarnings("unchecked")
		     Set set = new TreeSet(new Comparator<Map.Entry<String, Float>>() {
		     @Override
		     public int compare(Entry<String, Float> o1, Entry<String, Float> o2) {
		    	 return  o1.getValue().compareTo(o2.getValue()) > 0 ? -1 : 1;
		      }
		     });
		     set.addAll(wordCountidf.entrySet());
		     return set.iterator();
		}
	
	//function to calculate TF of words
	public static Map<String,Integer> calculateFrequencyOfWords(String str){
		System.out.println("calulating TF....................");
		
		Map<String, Integer> map = new TreeMap<>();
		Map<String, Integer> fmap = new TreeMap<>();
		String[] splited = str.split("\\s+");
	    for (String w : splited) {
	        Integer n = map.get(w);
	        n = (n == null) ? 1 : ++n;
	        map.put(w, n);
	    }
	    //storing only top 100 words : you can change according to your requirement
	    int d=0;
	    Iterator w = valueIterator(map);
	    while(w.hasNext() && d<100 ) {
	    	
	    	@SuppressWarnings("rawtypes")
			Map.Entry pairs = (Map.Entry)w.next();
	       	fmap.put((String)pairs.getKey(),(Integer)pairs.getValue());
	    	d++;
	    }	 
	
		
		return fmap;
	}
	

	
}