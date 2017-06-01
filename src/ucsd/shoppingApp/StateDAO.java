package ucsd.shoppingApp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StateDAO {
	private static final String STATES_SQL = "SELECT id, state_name FROM STATE ORDER BY state_name";
	
	private static final String GET_STATES_ORDERED_SQL = "select id, state_name from state order by upper(state_name)"+
														 "limit 20 offset (20*?)";

	
	private static final String BUILD_TABLE_SQL = 
			"SELECT p.id, COALESCE(SUM(pr.price * pr.quantity), 0) " +
			"FROM product p LEFT OUTER JOIN products_in_cart pr " +
			"ON p.id = pr.product_id " +
			"AND pr.cart_id IN " +
			"    (SELECT s.id " +
			"    FROM shopping_cart s, person pr, state st " +
			"    WHERE s.person_id = pr.id " +
			"		AND pr.state_id = st.id " +
			"		AND st.state_name = ? " +
			"    AND s.is_purchased = true) " +
			"GROUP BY p.id " +
			"ORDER BY p.id " +
			"LIMIT 10 " +
			"OFFSET 10 * ?";
	
	
	public static final String BUILD_TABLE_SQL2 =
			"select product_name,product.id, coalesce(sum (quant*price),0) as grandtotal "+
			"from product left outer join "+ 
										
				"(select product_id as prod_id, customer_name as cust, quantity as quant "+
				"from state s, "+
					"(select pc.product_id as product_id, p.person_name as customer_name, "+
							"p.state_id as state_id, pc.quantity as quantity "+
					"from person p, products_in_cart pc "+ 
					"where pc.cart_id in  "+
							"(select sc.id  "+
							"from shopping_cart sc  "+
							"where sc.person_id = p.id and "+ 
							"sc.is_purchased = 'true') "+
							")  as smallertable  "+
					"where state_id = s.id and s.state_name= ? ) as newtable "+ 
										
			"on product.id = newtable.prod_id "+ 
			"group by product.id "+ 
			 "order by product.id "+
			"limit 20 "+
			 "offset 20*?"
			    
			 
			;
	
	
	public static HashMap<Integer, String> getStates(Connection con) {
		HashMap<Integer, String> states = new HashMap<Integer, String>();
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = con.createStatement();
			rs = stmt.executeQuery(STATES_SQL);
			while(rs.next()) {
				states.put(rs.getInt("id"), rs.getString("state_name"));
			}
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(rs != null) {
					rs.close();
				}
				if(stmt != null) {
					stmt.close();
				}
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
		return states;
	}
	
	/*
	 * This function returns states using an offset... the function above ^^ doesn't use an offset.
	 */
	
	public static ArrayList<String> getStatesOffset(int offset) {
		
		ArrayList<String> states = new ArrayList<>();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			Connection con = ConnectionManager.getConnection();
			pstmt = con.prepareStatement(GET_STATES_ORDERED_SQL);
			pstmt.setInt(1,offset);
			
			rs = pstmt.executeQuery();

			while(rs.next()) {
				states.add(rs.getString("state_name"));
			}
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(rs != null) {
					rs.close();
				}
				if(pstmt != null) {
					pstmt.close();
				}
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
		return states;
	}
	
	
	/*
	 * This function returns a mapping of state name to a hashmap that maps (key: state name, value: total sale).
	 * this way, we can find out the amount of sales made for a given product, per State.
	 * 
	 */
	public static HashMap<String, Map <String,Integer>> getStateMapping(List<String> states, int offset) {
		
		HashMap<String, Map <String,Integer>> totalsales_per_state = new HashMap<>();
		HashMap<Integer,String> product_mapping = new HashMap<>();
		
		System.out.println("offset for state mapping: "+Integer.toString(offset));
		PreparedStatement ptst = null;
		ResultSet rs = null;
		ResultSet rc = null;
		try {

			Connection conn = ConnectionManager.getConnection();
			Statement statement = conn.createStatement();

		
			String state;
			for (int i =0; i < states.size(); i++) {
				HashMap<String, Integer> grandTotal = new HashMap<>();
				state = states.get(i);

				ptst = conn.prepareStatement(BUILD_TABLE_SQL);
				ptst.setString(1, state);
				ptst.setInt(2, offset);
					
				//rs is for getting the table for each state (how much money spent on each product)
				rs = ptst.executeQuery();
				rc = statement.executeQuery("select * from product");

				//getting mapping from product id to product name
				while (rc.next()){
					product_mapping.put(rc.getInt(1), rc.getString(3));						
				}
		
				int product_id;
				String product_name;
				while (rs.next()){

					product_id = rs.getInt(1);							
					product_name = product_mapping.get(product_id);

					//so in grandTotal, we can look up how much money was spent on each
					//product by name (each customer has a grandTotal map.
					grandTotal.put(product_name, rs.getInt(2));
					
					//System.out.println(state+" , "+product_name+" , "+Integer.toString(rs.getInt(2)));
					/*if (state.equals("Wisconsin") ){
						System.out.println("product: "+product_name+", total: "+Integer.toString(rs.getInt(2)));
						
					}*/
					

				}

				totalsales_per_state.put(state,grandTotal);
				
			}

		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			try {
				if(rs != null) {
					rs.close();
				}
				if(ptst != null) {
					ptst.close();
				}
				if(rc != null) {
					rs.close();
				}
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
		
		
		return totalsales_per_state;
	
	}
	
	//returns a mapping of state name to total purchases made across ALL products
	public static Map<String,Integer> getTotalPurchasesAllProducts(List<String> states) {
		Map<String,Integer> totalSalesPerState = new HashMap<>();

		HashMap<String, Map <String,Integer>> stateMapping = getStateMapping(states, 0);
		
		//customerMapping is a hashmap that maps state to a hashmap of product names, and total purchase for that product.
		//so, for each state's hashmap, we sum up the purchases...
		for (Map.Entry<String, Map <String,Integer>> entry : stateMapping.entrySet()) {
		    String state = entry.getKey();
		    Map <String, Integer> stateMap = entry.getValue();
		    
		    //sum up the values (purchases)
		    int total = 0;
		    int i = 0;
		    for (Map.Entry<String, Integer> entry2 : stateMap.entrySet()){
		    	total = total + entry2.getValue();
		    }
		    

		    totalSalesPerState.put(state, total);

		}
		
		return totalSalesPerState;
		

	}
	
	//returns a mapping of state name to total purchases made across a SPECIFIC category
	public static Map<String,Integer> getTotalPurchasesPerCategory(List<String> states, String category) {
		Map<String,Integer> totalSalesPerState = new HashMap<>();

		HashMap<String, Map <String,Integer>> stateMapping = getStateMapping(states, 0);
		
		//customerMapping is a hashmap that maps user to a hashmap of product names, and total purchase for that product.
		//so, for each customer's hashmap, we sum up the purchases ONLY for products belonging in
		//certain category...
		ProductDAO p = new ProductDAO(ConnectionManager.getConnection());
		
		ArrayList<String> productsFromSelectedCategory = p.getProductsFromCategory(category);
		
		for (Map.Entry<String, Map <String,Integer>> entry : stateMapping.entrySet()) {
		    String state = entry.getKey();
		    Map <String, Integer> stateMap = entry.getValue();
		    
		    //sum up the values (purchases)
		    int total = 0;
		    int i = 0;
		    for (Map.Entry<String, Integer> entry2 : stateMap.entrySet()){
		    	if(productsFromSelectedCategory.contains(entry2.getKey())){
		    	
		    		total = total + entry2.getValue();
		    	}
		    }
		    

		    totalSalesPerState.put(state, total);

		}
		
		return totalSalesPerState;
		
	}
	
	
	// This function builds a list of states sorted according to the total money they've spent.
	public static List<String> buildStatesTopKlist(String filter){
		List<String> statesTopKSorted = new ArrayList<>();
		List<String> all_states= new ArrayList<>();

		ResultSet rs = null;
		Statement stmt = null;
		Connection conn = null;
		
		try{
			conn = ConnectionManager.getConnection();
			stmt = conn.createStatement();
			
			rs = stmt.executeQuery(STATES_SQL);
			
			while (rs.next()){
				all_states.add(rs.getString("state_name"));
			}

			Map<String,Integer> totalSalesPerState;
			
			// Check if sales filter had been applied.
			if (filter.equals("all_products")) {
				totalSalesPerState = getTotalPurchasesAllProducts(all_states);
			}
			else {
				totalSalesPerState = getTotalPurchasesPerCategory(all_states, filter);
			}

			// Make pairs (customer, total money spent) and sort the list.
			Pair[] statesTotalPairs = new Pair[all_states.size()];
			int i =0;
		    for (Map.Entry<String, Integer> entry : totalSalesPerState.entrySet()) {
		    	Pair stateTotalPair = new Pair(entry.getKey(),entry.getValue());
		    	statesTotalPairs[i] = stateTotalPair;
		    	i++;
		    }
		    
		    // Sort list of pairs.
		    Pair[] sortedStateTotalPairs = Pair.bubbleSort(statesTotalPairs);
		    
		    // now put it into the statesTopK list. Start from the end of the stateTotalPairs...
		    for (int j = sortedStateTotalPairs.length - 1; j >= 0; j--) {
		    	System.out.println(sortedStateTotalPairs[j].key());
		    	
		    	statesTopKSorted.add(sortedStateTotalPairs[j].key());
		    }
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
		finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (stmt != null) {
					stmt.close();
				}
				if (conn != null) {
					conn.close();
				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		}	
		return statesTopKSorted;
	}
	
	// This function calls the buildCustomersTopKlist and gets 20 customers at a time.
	public static List<String> getStatesTopKlist(String filter,int list_offset){
		
		List<String> statesTopK = new ArrayList<>();
		List<String> all_states_topk_sorted = buildStatesTopKlist(filter);
		
		// Start getting names from the all_customers_topk_sorted list starting from list_offset * 20.
		int counter = 1;
		for (int i = list_offset*20;i < all_states_topk_sorted.size(); i++ ) {
			String state = all_states_topk_sorted.get(i);
			statesTopK.add(state);
			if (counter == 20) {
				break;
			}
			counter++;
		}
		
		return statesTopK;
	}
	
}
