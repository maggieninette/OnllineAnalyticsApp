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

	private static final String STATES_SQL =
            "SELECT id, state_name " +
            "FROM state " +
            "ORDER BY state_name";
	
	private static final String GET_STATES_ORDERED_SQL =
            "SELECT id, state_name " +
            "FROM state " +
            "ORDER BY UPPER(state_name) " +
            "LIMIT 20 " +
            "OFFSET 20 * ?";

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
	
	private static final String BUILD_TABLE_SQL_NO_OFFSET =
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
			"ORDER BY p.id ";

    /**
     * Returns states.
     * @param con
     * @return
     */
	public static HashMap<Integer, String> getStates(Connection con) {

	    HashMap<Integer, String> states = new HashMap<Integer, String>();
		Statement stmt = null;
		ResultSet rs = null;

		try {
			stmt = con.createStatement();
			rs = stmt.executeQuery(STATES_SQL);
			while (rs.next()) {
				states.put(rs.getInt("id"), rs.getString("state_name"));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (stmt != null) {
					stmt.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return states;
	}

    /**
     * Returns states using offset.
     * @param offset
     * @return
     */
	public static ArrayList<String> getStatesOffset(int offset) {
		
		ArrayList<String> states = new ArrayList<>();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		try {
			Connection con = ConnectionManager.getConnection();
			pstmt = con.prepareStatement(GET_STATES_ORDERED_SQL);
			pstmt.setInt(1, offset);
			
			rs = pstmt.executeQuery();

			while (rs.next()) {
				states.add(rs.getString("state_name"));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (pstmt != null) {
					pstmt.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return states;
	}
	
	/*
	 * This function returns a mapping of state name to a hashmap that maps (key: state name, value: total sale).
	 * this way, we can find out the amount of sales made for a given product, per State.
	 */

    /**
     * TODO: Finish header.
     * @param states
     * @param offset
     * @return
     */
	public static HashMap<String, Map <String,Integer>> getStateMapping(List<String> states, int offset) {
		
		HashMap<String, Map <String,Integer>> totalSalesPerState = new HashMap<>();
		HashMap<Integer,String> productMapping = new HashMap<>();
		
		PreparedStatement ptst = null;
		ResultSet rs = null;
		ResultSet rc = null;

		try {
			Connection conn = ConnectionManager.getConnection();
			Statement statement = conn.createStatement();

			String state;
			for (int i = 0; i < states.size(); i++) {
				HashMap<String, Integer> grandTotal = new HashMap<>();
				state = states.get(i);

				ptst = conn.prepareStatement(BUILD_TABLE_SQL);
				ptst.setString(1, state);
				ptst.setInt(2, offset);
					
				//rs is for getting the table for each state (how much money spent on each product)
				rs = ptst.executeQuery();
				rc = statement.executeQuery("SELECT * FROM product");

				//getting mapping from product id to product name
				while (rc.next()){
					productMapping.put(rc.getInt(1), rc.getString(3));
				}
		
				int productId;
				String productName;
				while (rs.next()) {

					productId = rs.getInt(1);
					productName = productMapping.get(productId);

					//so in grandTotal, we can look up how much money was spent on each
					//product by name (each customer has a grandTotal map.
					grandTotal.put(productName, rs.getInt(2));
				}

				totalSalesPerState.put(state, grandTotal);
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (ptst != null) {
					ptst.close();
				}
				if (rc != null) {
					rs.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return totalSalesPerState;
	}

    /**
     * TODO: Finish header.
     * @param states
     * @return
     */
	public static HashMap<String, Map <String,Integer>> getStateMappingAllProducts(List<String> states) {
		
		HashMap<String, Map<String,Integer>> totalSalesPerState = new HashMap<>();
		HashMap<Integer, String> productMapping = new HashMap<>();

		PreparedStatement ptst = null;
		ResultSet rs = null;
		ResultSet rc = null;

		try {
			Connection conn = ConnectionManager.getConnection();
			Statement statement = conn.createStatement();
		
			String state;
			for (int i = 0; i < states.size(); i++) {
				HashMap<String, Integer> grandTotal = new HashMap<>();
				state = states.get(i);

				ptst = conn.prepareStatement(BUILD_TABLE_SQL_NO_OFFSET);
				ptst.setString(1, state);

					
				//rs is for getting the table for each state (how much money spent on each product)
				rs = ptst.executeQuery();
				rc = statement.executeQuery("SELECT * FROM product");

				//getting mapping from product id to product name
				while (rc.next()) {
					productMapping.put(rc.getInt(1), rc.getString(3));
				}
		
				int productId;
				String productName;
				while (rs.next()) {

					productId = rs.getInt(1);
					productName = productMapping.get(productId);

					//so in grandTotal, we can look up how much money was spent on each
					//product by name (each customer has a grandTotal map.
					grandTotal.put(productName, rs.getInt(2));
				}

				totalSalesPerState.put(state, grandTotal);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (ptst != null) {
					ptst.close();
				}
				if (rc != null) {
					rs.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return totalSalesPerState;
	}

    /**
     * Returns mapping of state name to total purchases made by customers from each state for all products.
     * @param states
     * @return
     */
    public static Map<String,Integer> getTotalPurchasesAllProducts(List<String> states) {

        Map<String, Integer> totalSalesPerState = new HashMap<>();
		HashMap<String, Map<String,Integer>> stateMapping = getStateMappingAllProducts(states);
		
		//customerMapping is a hashmap that maps state to a hashmap of product names, and total purchase for that product.
		//so, for each state's hashmap, we sum up the purchases...
		for (Map.Entry<String, Map <String,Integer>> entry : stateMapping.entrySet()) {
		    String state = entry.getKey();
		    Map <String, Integer> stateMap = entry.getValue();
		    
		    //sum up the values (purchases)
		    int total = 0;
		    for (Map.Entry<String, Integer> entry2 : stateMap.entrySet()) {
		    	total = total + entry2.getValue();
		    }

		    totalSalesPerState.put(state, total);
		}
		
		return totalSalesPerState;
	}

    /**
     * Returns mapping of state name to total purchases made by customers from each state for a specific category.
     * @param states
     * @param category
     * @return
     */
	public static Map<String, Integer> getTotalPurchasesPerCategory(List<String> states, String category) {

	    Map<String, Integer> totalSalesPerState = new HashMap<>();
		HashMap<String, Map<String,Integer>> stateMapping = getStateMappingAllProducts(states);
		
		//customerMapping is a hashmap that maps user to a hashmap of product names, and total purchase for that product.
		//so, for each customer's hashmap, we sum up the purchases ONLY for products belonging in
		//certain category...
		ProductDAO p = new ProductDAO(ConnectionManager.getConnection());
		
		ArrayList<String> productsFromSelectedCategory = p.getProductsFromCategory(category);
		
		for (Map.Entry<String, Map <String,Integer>> entry : stateMapping.entrySet()) {
		    String state = entry.getKey();
		    Map<String, Integer> stateMap = entry.getValue();
		    
		    //sum up the values (purchases)
		    int total = 0;
		    for (Map.Entry<String, Integer> entry2 : stateMap.entrySet()) {
		    	if (productsFromSelectedCategory.contains(entry2.getKey())) {
		    		total = total + entry2.getValue();
		    	}
		    }

		    totalSalesPerState.put(state, total);
		}
		
		return totalSalesPerState;
	}

    /**
     * Builds a list of states sorted according to total money spent by customers belonging to each state.
     * @param filter
     * @return
     */
	public static List<String> buildStatesTopKList(String filter){
		List<String> statesTopKSorted = new ArrayList<>();
		List<String> allStates= new ArrayList<>();

		ResultSet rs = null;
		Statement stmt = null;
		Connection conn = null;
		
		try{
			conn = ConnectionManager.getConnection();
			stmt = conn.createStatement();
			
			rs = stmt.executeQuery(STATES_SQL);
			
			while (rs.next()) {
				allStates.add(rs.getString("state_name"));
			}

			Map<String,Integer> totalSalesPerState;
			
			// Check if sales filter had been applied.
			if (filter.equals("all_products")) {
				totalSalesPerState = getTotalPurchasesAllProducts(allStates);
			}
			else {
				totalSalesPerState = getTotalPurchasesPerCategory(allStates, filter);
			}

			// Make pairs (customer, total money spent) and sort the list.
			ArrayList<Pair> statesTotalPairs = new ArrayList<>();
		    for (Map.Entry<String, Integer> entry : totalSalesPerState.entrySet()) {
		    	Pair stateTotalPair = new Pair(entry.getKey(),entry.getValue());
		    	statesTotalPairs.add(stateTotalPair);
		    }
		    
		    // Sort list of pairs.
		    ArrayList<Pair> sortedStateTotalPairs = Pair.bubbleSort(statesTotalPairs);
		    
		    // now put it into the statesTopK list. Start from the end of the stateTotalPairs...
		    for (int j = sortedStateTotalPairs.size() - 1; j >= 0; j--) {
		    	statesTopKSorted.add(sortedStateTotalPairs.get(j).key());
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

    /**
     * Calls buildCustomersTopKList and gets 20 customers at a time.
     * @param filter
     * @param listOffset
     * @return
     */
	public static List<String> getStatesTopKList(String filter, int listOffset) {
		
		List<String> statesTopK = new ArrayList<>();
		List<String> allStatesTopKSorted = buildStatesTopKList(filter);
		
		// Start getting names from the all_customers_topk_sorted list starting from listOffset * 20.
		int counter = 1;
		for (int i = listOffset * 20; i < allStatesTopKSorted.size(); i++ ) {
			String state = allStatesTopKSorted.get(i);
			statesTopK.add(state);

			if (counter == 20) {
				break;
			}
			counter++;
		}
		
		return statesTopK;
	}
}
