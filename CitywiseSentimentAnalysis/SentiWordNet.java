

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
import java.util.Set;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.util.Version;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
/*
 * @author : Team 2 
 * This class takes as input adjectives list and citywise reviews and calculates the sentiment value of reviews as per
 * SentiWordNet. This is unsupervised learning
 * 
*/
public class SentiWordNet {

private Map<String, Double> dictionary;

public SentiWordNet(String pathToSWN) throws IOException {
	// This is our main dictionary representation
	dictionary = new HashMap<String, Double>();

	// From String to list of doubles.
	HashMap<String, HashMap<Integer, Double>> tempDictionary = new HashMap<String, HashMap<Integer, Double>>();

	BufferedReader csv = null;
	try {
		csv = new BufferedReader (new FileReader(pathToSWN));
		int lineNumber = 0;

		String line;
		while ((line = csv.readLine()) != null) {
			lineNumber++;

			// If it's a comment, skip this line.
			if (!line.trim().startsWith("#")) {
				// We use tab separation
				String[] data = line.split("\t");
				String wordTypeMarker = data[0];

				// Example line:
				// POS ID PosS NegS SynsetTerm#sensenumber Desc
				// a 00009618 0.5 0.25 spartan#4 austere#3 ascetical#2
				// ascetic#2 practicing great self-denial;...etc

				// Is it a valid line? Otherwise, through exception.
				if (data.length != 6) {
					throw new IllegalArgumentException(
							"Incorrect tabulation format in file, line: "
									+ lineNumber);
				}

				// Calculate synset score as score = PosS - NegS
				Double synsetScore = Double.parseDouble(data[2]);
						//- Double.parseDouble(data[3]);

				// Get all Synset terms
				String[] synTermsSplit = data[4].split(" ");

				// Go through all terms of current synset.
				for (String synTermSplit : synTermsSplit) {
					// Get synterm and synterm rank
					String[] synTermAndRank = synTermSplit.split("#");
					String synTerm = synTermAndRank[0] + "#"
							+ wordTypeMarker;

					int synTermRank = Integer.parseInt(synTermAndRank[1]);
					// What we get here is a map of the type:
					// term -> {score of synset#1, score of synset#2...}

					// Add map to term if it doesn't have one
					if (!tempDictionary.containsKey(synTerm)) {
						tempDictionary.put(synTerm,
								new HashMap<Integer, Double>());
					}

					// Add synset link to synterm
					tempDictionary.get(synTerm).put(synTermRank,
							synsetScore);
				}
			}
		}

		// Go through all the terms.
		for (Map.Entry<String, HashMap<Integer, Double>> entry : tempDictionary
				.entrySet()) {
			String word = entry.getKey();
			Map<Integer, Double> synSetScoreMap = entry.getValue();

			// Calculate weighted average. Weigh the synsets according to
			// their rank.
			// Score= 1/2*first + 1/3*second + 1/4*third ..... etc.
			// Sum = 1/1 + 1/2 + 1/3 ...
			double score = 0.0;
			double sum = 0.0;
			for (Map.Entry<Integer, Double> setScore : synSetScoreMap
					.entrySet()) {
				score += setScore.getValue() / (double) setScore.getKey();
				sum += 1.0 / (double) setScore.getKey();
			}
			score /= sum;

			dictionary.put(word, score);
		}
	} catch (Exception e) {
		e.printStackTrace();
	} finally {
		if (csv != null) {
			csv.close();
		}
	}
}

public double extract(String word, String pos) {
	if(dictionary.get(word + "#" + pos)!= null){
		return dictionary.get(word + "#" + pos);
	}else{
		return 0;
	}
}

public static Set<String> createHashSet(Set<String> wordSet, BufferedReader b) throws IOException{
	
	String s= b.readLine();
	while(s!=null){
		wordSet.add(s);
		s=b.readLine();
	}
		
	return wordSet;
}

public static void main(String [] args) throws IOException {
	
	helperFunctions obj = new helperFunctions();
	HashMap<String, ArrayList<String>> cityBussMap = new HashMap<String, ArrayList<String>>();
	HashMap<String, String> BussRevMap = new HashMap<String, String>();
	
	//DB connection
	MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
	DB db = mongoClient.getDB( "yelpDB" );
	
	//Collections that I need
	DBCollection indRes = db.getCollection("IndRes");
	DBCollection indRev = db.getCollection("IndRew");
	DBCollection pReview = db.getCollection("positive");
	DBCollection nReview = db.getCollection("negative");
	
	//Getting maps City-Buss_id and Buss_id-Review
	cityBussMap = obj.getCityBusMapping(indRes, cityBussMap);
	BussRevMap = obj.getBusRevMapping(indRev, BussRevMap);
	
	//Using SentiWordNet values for computing sentiment polarity
	String pathToSWN = "C:\\Users\\Vidhixa\\Desktop\\SentiWordNet_3.0.0\\home\\swn\\www\\admin\\dump\\senti.txt";
	SentiWordNet sentiwordnet = new SentiWordNet(pathToSWN);
	
	//Reading positive and negative words
	File f = new File("C:\\Users\\Vidhixa\\Desktop\\positive-words.txt");
	BufferedReader br = new BufferedReader(new FileReader(f));
	Set<String> posSet = new HashSet<>();
	posSet = createHashSet(posSet, br);
	
	File f1 = new File("C:\\Users\\Vidhixa\\Desktop\\negative-words.txt");
	BufferedReader br1 = new BufferedReader(new FileReader(f1));
	Set<String> negSet = new HashSet<>();
	negSet = createHashSet(negSet, br1);
	
	//Computing sentiment score and printing 
	FileWriter file = new FileWriter("C:\\Users\\Vidhixa\\Desktop\\sentiValues.txt");
	BufferedWriter bw = new BufferedWriter(file);
	Iterator i1 = cityBussMap.entrySet().iterator();
	Iterator i2 = BussRevMap.entrySet().iterator();
	
	while(i1.hasNext()){
		Map.Entry pairs = (Map.Entry)i1.next();
		System.out.println((String) pairs.getKey());
		System.out.println("--------------------------");
		//bw.write((String) pairs.getKey());
		//bw.newLine();
		ArrayList<String> current_business = (ArrayList<String>) pairs.getValue();
		System.out.println("No of restros "+current_business.size());
		
		int countPos = 0;
		int countNeg = 0;
		float x = 0f ;
		float y = 0f ;
		for(String s: current_business){
			String reviews = BussRevMap.get(s);
			if(reviews == null){
				continue;
			}else{
				//System.out.println(reviews);
				String[] words = reviews.split(" "); 
				
			
			for (String word : words){ 
				if(posSet.contains(word)){
				x += sentiwordnet.extract(word, "a");
				countPos++;
				}
				
				if(negSet.contains(word)){
				y += sentiwordnet.extract(word, "a");
				countNeg++;
				}
			}
		}	
   }
		System.out.println("Total pos "+countPos);
		System.out.println("Pos score is "+x);
		System.out.println("Total neg "+countNeg);
		System.out.println("Neg score is "+y);
		System.out.println("***************************************************");
}}}

