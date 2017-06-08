package ucsd.shoppingApp;

import java.math.BigDecimal;
import java.math.BigInteger;
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
	

	

	/*private static final String BUILD_TABLE_SQL = 
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
			"LIMIT 50 " +
			"OFFSET 50 * ?";
	
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
			"ORDER BY p.id "; */

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
	
	/*
	 * This function returns a mapping of state name to a hashmap that maps (getKey: state name, getValue: total sale).
	 * this way, we can find out the amount of sales made for a given product, per State.
	 */

    /**
     * TODO: Finish header.
     * @param states
     * @param offset
     * @return
     */
	/*public static HashMap<String, Map <String,Integer>> getStateMapping(List<String> states, int offset) {
		
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
	}*/

	public static HashMap<String, Map <String,Integer>> getStateMappingFilteredTop50Products(List<String> states, List<String> products) {
		
		HashMap <String, Map <String,Integer>> totalSalesPerStateForEachProduct = new HashMap<>();
		
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Connection con = null;
		
		try{
			con = ConnectionManager.getConnection();
			
			for (int i  = 0; i <states.size(); i++) {
				pstmt = con.prepareStatement(	"SELECT total "+
					 	"FROM precomputed_state_topk_filtered "+
					 	"WHERE state_name=? "+
					 	"AND product_name=? ");
				
				String state_name = states.get(i);
				HashMap<String, Integer> grandTotal = new HashMap<>();
				System.out.println(Integer.toString(states.size()));
				
				for (int j = 0; j < products.size(); j++ ) {
					String product_name = products.get(j);
					
					pstmt.setString(1, state_name);
					pstmt.setString(2, product_name);
					
					rs = pstmt.executeQuery();
					
					rs.next();
					grandTotal.put(products.get(j), rs.getInt("total"));
					
				}
				pstmt.close();
				totalSalesPerStateForEachProduct.put(states.get(i),grandTotal);
			}

			
		}catch (SQLException e){
			e.printStackTrace();
		}finally{
			try {
				if (rs != null) {
					rs.close();
				}
				if (pstmt != null) {
					pstmt.close();
				}
				if (con != null) {
					con.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}			
		}
		
		
		return totalSalesPerStateForEachProduct;
		
	}
	
	
    /**
     * TODO: Gets a map that maps a state to a map that maps a product name to how much sales
     * 		 were made for that product. (Every state has a map (key:product, value:total sale)
     *		
     * @param states
     * @return
     */
	/*public static HashMap<String, Map <String,Integer>> getStateMappingAllProducts(List<String> states) {
		
		
		HashMap<String, Map<String,Integer>> totalSalesPerState = new HashMap<>();

		PreparedStatement ptst = null;
		ResultSet rs = null;

		try {
			Connection conn = ConnectionManager.getConnection();

			String state_name;
			for (int i = 0; i < states.size(); i++) {
				HashMap<String, Integer> grandTotal = new HashMap<>();
				state_name = states.get(i);
				
				ptst = conn.prepareStatement("SELECT * FROM precomputed_state_topk WHERE state_name = ?");
				ptst.setString(1, state_name);	
				
				rs = ptst.executeQuery();
				
				while (rs.next()) {
					grandTotal.put(rs.getString(2),rs.getInt(3));
				}
			
				totalSalesPerState.put(state_name, grandTotal);
			
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
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return totalSalesPerState;
	}*/
	
	public static HashMap<String, Map <String,Integer>> getStateMappingTop50Products(List<String> states, List<String> products) {
		
		HashMap <String, Map <String,Integer>> totalSalesPerStateForEachProduct = new HashMap<>();
		
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Connection con = null;
		
		try{
			con = ConnectionManager.getConnection();
			
			for (int i  = 0; i <states.size(); i++) {
				pstmt = con.prepareStatement(	"SELECT total "+
					 	"FROM precomputed_state_topk "+
					 	"WHERE state_name=? "+
					 	"AND product_name=? ");
				
				String state_name = states.get(i);
				HashMap<String, Integer> grandTotal = new HashMap<>();
				System.out.println(Integer.toString(states.size()));
				
				for (int j = 0; j < products.size(); j++ ) {
					String product_name = products.get(j);
					
					pstmt.setString(1, state_name);
					pstmt.setString(2, product_name);
					
					rs = pstmt.executeQuery();
					
					rs.next();
					grandTotal.put(products.get(j), rs.getInt("total"));
					
				}
				pstmt.close();
				totalSalesPerStateForEachProduct.put(states.get(i),grandTotal);
			}

			
		}catch (SQLException e){
			e.printStackTrace();
		}finally{
			try {
				if (rs != null) {
					rs.close();
				}
				if (pstmt != null) {
					pstmt.close();
				}
				if (con != null) {
					con.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}			
		}
		
		
		return totalSalesPerStateForEachProduct;
		
	}
	
	
	
	

    /**
     * Returns mapping of state name to total purchases made by each state across all products.
     * @param states
     * @return
     */
    public static Map<String,Integer> getTotalPurchasesAllProducts(List<String> states) {

        Map<String, Integer> totalSalesPerState = new HashMap<>();

		//Go to the TopStateSales table and for each state get the total purchases. 
		PreparedStatement ptst = null;
		ResultSet rs = null;
		Connection con = null;
		
		try {	
			con = ConnectionManager.getConnection();
			ptst = con.prepareStatement("SELECT * FROM top_state_sales");
		
			rs = ptst.executeQuery();
			
			while (rs.next()) {
				//totalSalesPerState.put(rs.getString("state_name"),rs.getInt("totalsale")); TEST
				totalSalesPerState.put(rs.getString("state_name"),1);
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
				if (ptst != null) {
					ptst.close();
				}
				if (con != null) {
					con.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		return totalSalesPerState;
	}

    /**
     * Returns mapping of state name to total purchases made in each state for a specific category.
     * @param states
     * @param category
     * @return
     */
	public static Map<String, Integer> getTotalPurchasesPerCategory(List<String> states, String category) {
		
        Map<String, Integer> totalSalesPerState = new HashMap<>();

		//Go to the TopStateSales table and for each state get the total purchases. 
		PreparedStatement ptst = null;
		ResultSet rs = null;
		Connection con = null;
		try {	
			
			con = ConnectionManager.getConnection();
			ptst = con.prepareStatement("SELECT * FROM top_state_sales_filtered");
		
			rs = ptst.executeQuery();
			
			while (rs.next()) {
				totalSalesPerState.put(rs.getString("state_name"),rs.getInt("totalsale")); 

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
				if (ptst != null) {
					ptst.close();
				}
				if (con != null) {
					con.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		return totalSalesPerState;
	}

    /**
     * Builds a list of states sorted according to total sales made in each state.
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

			Map<String,Integer> totalSalesPerState;
			
			// Check if sales filter had been applied.
			if (filter.equals("all_products")) {
				rs = stmt.executeQuery("SELECT * FROM top_state_sales ");
				
				while (rs.next()) {
					statesTopKSorted.add(rs.getString("state_name"));
				}	
				
			}
			else {
				rs = stmt.executeQuery(STATES_SQL);
				while (rs.next()) {
					allStates.add(rs.getString("state_name"));
				}
				totalSalesPerState = getTotalPurchasesPerCategory(allStates, filter);
			

				// Make pairs (state, total money spent) and sort the list.
				ArrayList<Pair> statesTotalPairs = new ArrayList<>();
			    for (Map.Entry<String, Integer> entry : totalSalesPerState.entrySet()) {
			    	Pair stateTotalPair = new Pair(entry.getKey(),entry.getValue());
			    	statesTotalPairs.add(stateTotalPair);
			    }
			    
			    // Sort list of pairs.
			    ArrayList<Pair> sortedStateTotalPairs = Pair.bubbleSort(statesTotalPairs);
			    
			    // now put it into the statesTopK list. Start from the end of the stateTotalPairs...
			    for (int j = sortedStateTotalPairs.size() - 1; j >= 0; j--) {
			    	statesTopKSorted.add(sortedStateTotalPairs.get(j).getKey());
			    }
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
     * Calls buildCustomersTopKList and gets all at a time.
     * @param filter
     * @return
     */
	public static List<String> getStatesTopKList(String filter) {
		
		List<String> statesTopK = new ArrayList<>();
		List<String> allStatesTopKSorted = buildStatesTopKList(filter);
		
		// Start getting names from the all_customers_topk_sorted list starting from listOffset * 20.
		for (int i = 0; i < allStatesTopKSorted.size(); i++ ) {
			String state = allStatesTopKSorted.get(i);
			statesTopK.add(state);
		}
		
		return statesTopK;
	}
}
