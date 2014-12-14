import java.io.BufferedReader;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.opencsv.CSVWriter;
/*
 * @author : Team 2 
 * This class takes all the reviews and feature vector to create a global feature vector for machine learning algorithms
 * to be run on weka
 * 
*/

public class calculateGlobalNaiveBayes {
	
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

		//Getting maps City-Buss_id and Buss_id-Review
		helperFunctions obj = new helperFunctions();
		HashMap<String, ArrayList<String>> cityBussMap = new HashMap<String, ArrayList<String>>();
		HashMap<String, String> BussRevMap = new HashMap<String, String>();
		cityBussMap = obj.getCityBusMapping(indRes, cityBussMap);
		BussRevMap = obj.getBusRevMapping(indRev, BussRevMap);
		
		//createGlobalCSV();
		createGlobalCSV();
	}
	
	public static void createGlobalCSV() throws IOException{
		//File f = new File();
		BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Vidhixa\\Desktop\\allCSV\\topWords\\TFIDFtop.txt"));
		String line = br.readLine();
		String csvAdd=line+",";
		File csv = new File("C:\\Users\\Vidhixa\\Desktop\\allCSV\\topWords\\global.csv");
		BufferedWriter bw = new BufferedWriter(new FileWriter(csv));
		int count = 0;
		while(line != null && count<=1000){
			line=br.readLine();
			csvAdd = csvAdd + line+",";
		}
		//csvAdd = "label"+"\t";
		csvAdd = csvAdd+"label"+"\t";
		System.out.println(csvAdd);
		bw.write(csvAdd);
		br.close();
		bw.close();
		
	}


}
