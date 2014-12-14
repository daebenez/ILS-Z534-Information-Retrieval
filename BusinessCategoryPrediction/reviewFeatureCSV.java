

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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

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
import org.bson.BSONObject;
import org.tartarus.snowball.ext.PorterStemmer;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
/*
 * @author : Team 2 
 * This class takes as input the feature vector created by TF-IDF and 
 * processes all reviews to CSV for experiments on WEKa
 * 
*/
	public class reviewFeatureCSV{
		
		public static void main(String[] args)throws IOException{
			//Connectivity to MongoDB
			MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
			DB db = mongoClient.getDB( "yelpDB" );
			DBCollection coll = db.getCollection("brtable");

	  		//Check for repetitions from words file
	  		Set<String> rawFeatures = new HashSet<>() ;
	  		File f = new File("C:\\Users\\Vidhixa\\Desktop\\rawFeatures.txt");
			BufferedReader br = new BufferedReader(new FileReader(f));
			String line = br.readLine();						
			while(line!=null){
				if(rawFeatures.contains(line)){
					System.out.println(line);
					line = br.readLine();
				}
				else{
					rawFeatures.add(line);
					line = br.readLine();
				}
			}
			br.close();
			
			//Create a set noCat which ignores unnecessary categories
	  		Set<String> noCat = new HashSet<>() ;
	  		File f1 = new File("C:\\Users\\Vidhixa\\Desktop\\noCat.txt");
			BufferedReader br1 = new BufferedReader(new FileReader(f1));
			String line1 = br1.readLine();						
			while(line1!=null){
				noCat.add(line1);
				line1 = br1.readLine();
			}
			br1.close();
			
			//Adding reviews to CSV
			addReviewsToCSV(rawFeatures, noCat, coll);
			
		}
		
		//Adding review words to CSV file
		public static void addReviewsToCSV(Set<String> rawFeatures,Set<String> noCat, DBCollection coll) throws IOException{
			////Write words into CSV the first row
			File csvFile = new File("C:\\Users\\Vidhixa\\Desktop\\features.csv");
			BufferedWriter bw = new BufferedWriter(new FileWriter(csvFile));
			String printing = " ";
			for(String s: rawFeatures){
				printing = printing + s +",";
			}
			printing = printing+"label"+"\n";
			System.out.println(printing);
			bw.write(printing);
			
			DBCursor cursor = coll.find();	
			int count =0;
			while(cursor.hasNext() ) {
				++count;
			   DBObject obj = cursor.next();
			   String review=(String) (((DBObject)obj.get("value")).get("reviewdata"));
			   String modReview = removeStopWords(review);
			   @SuppressWarnings("unchecked")
			   ArrayList<String> catagArray = (ArrayList<String>) (((DBObject)obj.get("value")).get("categories"));
			   
			   for(String singleCatag: catagArray){
				   if(noCat.contains(singleCatag)){
					   System.out.println("Comes here "+noCat);
				   }
				   else{  
				   String intoCSV=" ";
				   for(String s: rawFeatures){
					   if(modReview == null ){
						   continue;
						} else {
							if(modReview.contains(s)){
								intoCSV = intoCSV+"1"+",";	    
						  }else{
							  intoCSV = intoCSV+"0"+",";
						  }
						}
				   }
				   intoCSV = intoCSV+singleCatag+"\n";
				   System.out.println(intoCSV);
				   bw.write(intoCSV);
				   }
			   }
			}
			bw.close();
		}
		
		//Preprocessing stopwords filter, stemming and tokenizing
		public static String removeStopWords(String str) throws IOException{
			ArrayList<String> stop_Words = new ArrayList<String>();		
			File file = new File("C:\\Users\\Vidhixa\\Desktop\\stopwordsAsj.txt");
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line = br.readLine();
			while(line!=null){
				stop_Words.add(line);
				line = br.readLine();
			}
			CharArraySet stopSet = new CharArraySet(stop_Words, true);	
			CharArraySet stopWords = EnglishAnalyzer.getDefaultStopSet();
			TokenStream tokenStream = new StandardTokenizer( new StringReader(str.trim()));
		    tokenStream = new StopFilter( tokenStream, stopSet);	
		    tokenStream = new StopFilter(tokenStream, stopWords);
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
			return tokenizedReview;
		}	
	}		
			
			
		