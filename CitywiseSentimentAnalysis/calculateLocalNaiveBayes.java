

	import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
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
 * This class takes all the reviews and feature vector to create a local feature vector for machine learning algorithms
 * to be run on weka
 * 
*/
	public class calculateLocalNaiveBayes {
		
		public static void main(String[] args)throws IOException{
			MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
			DB db = mongoClient.getDB( "yelpDB" );
			DBCollection indRes = db.getCollection("IndRes");
			DBCollection indRev = db.getCollection("IndRew");
			DBCollection pReview = db.getCollection("Lasp");
	  		DBCollection nReview = db.getCollection("Lasn");
			
			//Creating arraylist of Cities
			List<String> cities = new ArrayList<String>();
			cities = indRes.distinct("city");

			//Getting maps City-Buss_id and Buss_id-Review
			helperFunctions obj = new helperFunctions();
			HashMap<String, ArrayList<String>> cityBussMap = new HashMap<String, ArrayList<String>>();
			HashMap<String, String> BussRevMap = new HashMap<String, String>();
			cityBussMap = obj.getCityBusMapping(indRes, cityBussMap);
			BussRevMap = obj.getBusRevMapping(indRev, BussRevMap);
			
			HashMap<String, ArrayList<String>> cityWords = new HashMap<String, ArrayList<String>>();
			
			//Reading and creating hashmap from file
			File f = new File("C:\\Users\\Vidhixa\\Desktop\\allCSV\\topWords\\5cities.txt");
			BufferedReader br = new BufferedReader(new FileReader(f));
			
			String line = br.readLine();
			String city = null;
			//cityWords.put(line, null);
			
			while(line!=null){
				if(line.equalsIgnoreCase("NEWCITY")){
					line = br.readLine();
					cityWords.put(line, null);
					city = line;
					line= br.readLine();
				}else{
					ArrayList<String> current;
					if(cityWords.get(city)==null){
						current = new ArrayList<String>();
						current.add(line);
						cityWords.put(city, current);
						line = br.readLine();
					}else{
						current = cityWords.get(city);
						current.add(line);
						cityWords.put(city, current);							
						line=br.readLine();
				}}}
			
			Iterator iter = cityWords.entrySet().iterator();
			while(iter.hasNext()){
				Map.Entry keyValue = (Map.Entry)iter.next();
			   System.out.println((String) keyValue.getKey());
			}
			br.close();		
			
			
			createCSVnb(pReview, nReview);
			createLocalCSVnb(pReview, nReview, cityWords, "Glendale");
		}
	
		public static void createCSVnb(DBCollection p, DBCollection n) throws IOException{
			//File f = new File();
			BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Vidhixa\\Desktop\\allCSV\\topWords\\TFIDFtop.txt"));
			String line = br.readLine();
			Map<String,Integer> global = new TreeMap<String,Integer>();

			String csvAdd=line+",";
			File csv = new File("C:\\Users\\Vidhixa\\Desktop\\allCSV\\topWords\\Las.csv");
			BufferedWriter bw = new BufferedWriter(new FileWriter(csv));
			int count = 0;
			while(line != null && count<=100){
				global.put(line, 0);
				line=br.readLine();				
				csvAdd = csvAdd + line+",";
				++count;
			}
			//csvAdd = "label"+"\t";
			csvAdd = csvAdd+"label"+"\n";
			System.out.println(csvAdd);
			bw.write(csvAdd);

			//All the data write
			DBCursor cursor = p.find();			
			while(cursor.hasNext()) {
			   DBObject obj = cursor.next();
			   String intoCSV=" ";
			   String review = (String)obj.get("text");
			   Iterator i = global.entrySet().iterator();
			  
			   while(i.hasNext()){
				   Map.Entry keyValue = (Map.Entry)i.next();
				   if(review == null){
					   continue;
					} else if(review.contains((CharSequence) keyValue.getKey())){
					  intoCSV = intoCSV+"1"+",";	    
				  }else{
					  intoCSV = intoCSV+"0"+",";
				  }}
			intoCSV = intoCSV+"\n";
			bw.write(intoCSV);
			}
			
			
			DBCursor cursor1 = n.find();			
			while(cursor1.hasNext()) {
			   DBObject obj1 = cursor1.next();
			   String intoCSV=" ";
			   String review = (String)obj1.get("text");
			   Iterator i1 = global.entrySet().iterator();
			  
			   while(i1.hasNext()){
				   Map.Entry keyValue = (Map.Entry)i1.next();
				   if(review == null){
					   continue;
					}else if(review.contains((CharSequence) keyValue.getKey())){
					  intoCSV = intoCSV+"1"+",";	    
				  }else{
					  intoCSV = intoCSV+"0"+",";
				  }}
		    intoCSV = intoCSV+"\n";
			System.out.println(intoCSV);
			bw.write(intoCSV);
			}
			System.out.println("comes here");
			bw.close();
	}
		
		public static void createLocalCSVnb(DBCollection p, DBCollection n, Map<String, ArrayList<String>> cityWords, String city) throws IOException{
			Map<String,Integer> local = new TreeMap<String,Integer>();

			String csvAdd = " ";
			File csv = new File("C:\\Users\\Vidhixa\\Desktop\\allCSV\\topWords\\Gle.csv");
			BufferedWriter bw = new BufferedWriter(new FileWriter(csv));
			ArrayList<String> features = cityWords.get(city);
			for(String feature: features){
				local.put(feature, 0);
				csvAdd = csvAdd + feature+",";
			}
			csvAdd = csvAdd+"label"+"\n";
			System.out.println(csvAdd);
			bw.write(csvAdd);

			//All the data write
			DBCursor cursor = p.find();			
			while(cursor.hasNext()) {
			   DBObject obj = cursor.next();
			   String intoCSV=" ";
			   String review = (String)obj.get("text");
			   Iterator i = local.entrySet().iterator();
			  
			   while(i.hasNext()){
				   Map.Entry keyValue = (Map.Entry)i.next();
				   if(review == null){
					   continue;
					} else if(review.contains((CharSequence) keyValue.getKey())){
					  intoCSV = intoCSV+"1"+",";	    
				  }else{
					  intoCSV = intoCSV+"0"+",";
				  }}
			intoCSV = intoCSV+"\n";
			bw.write(intoCSV);
			}
			
			
			DBCursor cursor1 = n.find();			
			while(cursor1.hasNext()) {
			   DBObject obj1 = cursor1.next();
			   String intoCSV=" ";
			   String review = (String)obj1.get("text");
			   Iterator i1 = local.entrySet().iterator();
			  
			   while(i1.hasNext()){
				   Map.Entry keyValue = (Map.Entry)i1.next();
				   if(review == null){
					   continue;
					}else if(review.contains((CharSequence) keyValue.getKey())){
					  intoCSV = intoCSV+"1"+",";	    
				  }else{
					  intoCSV = intoCSV+"0"+",";
				  }}
		    intoCSV = intoCSV+"\n";
			System.out.println(intoCSV);
			bw.write(intoCSV);
			}
			bw.close();
	}
	
	}

	