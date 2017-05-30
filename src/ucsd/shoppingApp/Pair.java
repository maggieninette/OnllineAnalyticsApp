package ucsd.shoppingApp;

import ucsd.shoppingApp.Pair;

public class Pair
{
    private String key;
    private int value;

    public Pair(String aKey, int aValue)
    {
        key   = aKey;
        value = aValue;
    }
    //key is the name (customer/state), value is total sales
    public String key()   { return key; }
    public int value() { return value; }   
    
    
    
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
    }
}
