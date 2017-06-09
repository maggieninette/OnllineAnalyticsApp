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

	public static HashMap<String, Map <String, Double>> getStateMappingFilteredTop50Products(List<String> states, List<String> products) {
		
		HashMap <String, Map <String, Double>> totalSalesPerStateForEachProduct = new HashMap<>();
		
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Connection con = null;
		
		try{
			con = ConnectionManager.getConnection();
			
			for (int i  = 0; i <states.size(); i++) {
				pstmt = con.prepareStatement(	
												"SELECT product_name, total "+
												"FROM precomputed_state_topk_filtered "+
												"WHERE state_name=? "+
												"AND product_name IN "+
																"(SELECT product_name "+
												                 "FROM new_top_50_products "+
												                 "); ");
				
				String state_name = states.get(i);
				HashMap<String, Double> grandTotal = new HashMap<>();

				pstmt.setString(1, state_name);
				rs = pstmt.executeQuery();
				
				while (rs.next()){
					grandTotal.put(rs.getString("product_name"), rs.getDouble("total"));
				}
				
				pstmt.close();
		
				totalSalesPerStateForEachProduct.put(state_name, grandTotal);
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

	public static HashMap<String, Map <String, Double>> getStateMappingTop50Products(List<String> states, List<String> products) {
		
		HashMap <String, Map <String, Double>> totalSalesPerStateForEachProduct = new HashMap<>();
		
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Connection con = null;
		
		try{
			con = ConnectionManager.getConnection();
			
			for (int i  = 0; i <states.size(); i++) {
				pstmt = con.prepareStatement(	
												"SELECT product_name, total "+
												"FROM precomputed_state_topk "+
												"WHERE state_name=? "+
												"AND product_name IN "+
																"(SELECT product_name "+
												                 "FROM new_top_50_products "+
												                 "); ");
				
				String state_name = states.get(i);
				HashMap<String, Double> grandTotal = new HashMap<>();

				pstmt.setString(1, state_name);
				rs = pstmt.executeQuery();
				
				while (rs.next()){
					grandTotal.put(rs.getString("product_name"), rs.getDouble("total"));
				}
				
				pstmt.close();
		
				totalSalesPerStateForEachProduct.put(state_name, grandTotal);
			}

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
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
    public static Map<String, Double> getTotalPurchasesAllProducts(List<String> states) {

        Map<String, Double> totalSalesPerState = new HashMap<>();

		//Go to the TopStateSales table and for each state get the total purchases. 
		PreparedStatement ptst = null;
		ResultSet rs = null;
		Connection con = null;
		
		try {	
			con = ConnectionManager.getConnection();
			ptst = con.prepareStatement("SELECT * FROM top_state_sales");
		
			rs = ptst.executeQuery();
			
			while (rs.next()) {
				totalSalesPerState.put(rs.getString("state_name"), rs.getDouble("totalsale"));
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
	public static Map<String, Double> getTotalPurchasesPerCategory(List<String> states, String category) {
		
        Map<String, Double> totalSalesPerState = new HashMap<>();

		//Go to the TopStateSales table and for each state get the total purchases. 
		PreparedStatement ptst = null;
		ResultSet rs = null;
		Connection con = null;
		try {	
			
			con = ConnectionManager.getConnection();
			ptst = con.prepareStatement("SELECT * FROM top_state_sales_filtered");
		
			rs = ptst.executeQuery();
			
			while (rs.next()) {
				totalSalesPerState.put(rs.getString("state_name"), rs.getDouble("totalsale"));

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

		ResultSet rs = null;
		Statement stmt = null;
		Connection conn = null;
		
		try{
			conn = ConnectionManager.getConnection();
			stmt = conn.createStatement();

			Map<String,Integer> totalSalesPerState;
			
			// Check if sales filter had been applied.
			if (filter.equals("all_products")) {
				rs = stmt.executeQuery("SELECT * FROM top_state_sales ORDER BY totalsale DESC LIMIT 50 ");
				
				while (rs.next()) {
					statesTopKSorted.add(rs.getString("state_name"));
				}
			}
			else {
				rs = stmt.executeQuery(STATES_SQL);
				while (rs.next()) {
					rs = stmt.executeQuery("SELECT * FROM top_state_sales_filtered ORDER BY totalsale DESC LIMIT 50 ");
					
					while (rs.next()) {
						statesTopKSorted.add(rs.getString("state_name"));
					}
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
