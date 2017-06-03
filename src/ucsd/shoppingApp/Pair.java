package ucsd.shoppingApp;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import ucsd.shoppingApp.Pair;

public class Pair
{
    private String key;
    private int value;
    
    private String product1;
    private String product2;

    public Pair(String aKey, int aValue)
    {
        key   = aKey;
        value = aValue;
    }
    
    public Pair(String aProduct1, String aProduct2){
    	
    	product1= aProduct1;
    	product2 = aProduct2;
    	
    }
    
    private Pair productsPair;
    private BigDecimal cosineSimilarity;

    public Pair (Pair products, BigDecimal cosine){
    	productsPair = products;
    	cosineSimilarity = cosine;
    }
    
    
    //key is the name (customer/state), value is total sales
    public String key()   { return key; }
    public int value() { return value; } 
    
    public String getProduct1() { return product1; }
    public String getProduct2() { return product2; }
    
    public Pair getPair() { return productsPair; }
    public BigDecimal getCosine() {return cosineSimilarity; }
    
    
   /* 
    public static Pair[] bubbleSort(Pair[] arr) {
    	System.out.println("in bubble sort");
    	int n = arr.length;  
        Pair temp;  
         for(int i=0; i < n; i++){  
                 for(int j=1; j < (n-i); j++){  
                          if(arr[j-1].value > arr[j].value){ 
                                 //swap elements  
                                 temp = arr[j-1];  
                                 arr[j-1] = arr[j];  
                                 arr[j] = temp;  
                         }                 
                 }  
         }
         return arr;
    }*/
    
    
    public static ArrayList<Pair> bubbleSort(ArrayList<Pair> arr) {

    	int n = arr.size();  
        Pair temp;  
         for(int i=0; i < n; i++){  
                 for(int j=1; j < (n-i); j++){  
                          if(arr.get(j-1).value > arr.get(j).value){ 
                                 //swap elements  
                                 temp = arr.get(j-1);  
                                 arr.set(j-1, arr.get(j)); 
                                 arr.set(j, temp);  
                         }                 
                 }  
         }
         return arr;
    }   
    
    public static <K, V extends Comparable<? super V>> Map<K, V> sortMap(final Map<K, V> mapToSort) {
		List<Map.Entry<K, V>> entries = new ArrayList<Map.Entry<K, V>>(mapToSort.size());
 
		entries.addAll(mapToSort.entrySet());
 
		// Sorts the specified list according to the order induced by the specified comparator
		Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {
			@Override
			public int compare(final Map.Entry<K, V> entry1, final Map.Entry<K, V> entry2) {
				// Compares this object with the specified object for order
				return entry1.getValue().compareTo(entry2.getValue());
			}
		});
 
		Map<K, V> sortedCosinePairs = new LinkedHashMap<K, V>();
 
		// The Map.entrySet method returns a collection-view of the map
		for (Map.Entry<K, V> entry : entries) {
			sortedCosinePairs.put(entry.getKey(), entry.getValue());
		}
 
		return sortedCosinePairs;
	}
    
    
}


