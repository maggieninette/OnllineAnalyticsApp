package ucsd.shoppingApp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class PrecomputedStateTopK {

	private final static String UPDATE_CELL_VALUES = 	"UPDATE precomputed_state_topk "+
														"SET total = precomputed_state_topk.total+log.total "+
														"FROM log "+
														"WHERE log.state_name=precomputed_state_topk.state_name "+
														"AND precomputed_state_topk.product_name=log.product_name";
	
	
	private final static String DELETE_AND_INSERT_INTO_PRECOMPUTED_STATETOPK_FILTERED =
			"DELETE FROM precomputed_state_topk; "+
			
			" INSERT INTO precomputed_state_topk_filtered( "+	
			"SELECT allproductsandstates.state_name AS state_name, allproductsandstates.product_name AS product_name, COALESCE(total,0) AS total "+
			 "FROM "+
			    "(SELECT state_name AS state_name, product_name AS product_name "+
			    "FROM product, state, category "+
			     "WHERE product.category_id = category.id "+
			     "AND category.category_name=?) AS allproductsandstates "+
			    
			    
			    "LEFT OUTER JOIN "+
			      "(SELECT st.state_name AS state_name, p2.product_name AS product_name, SUM(pc.quantity * pc.price) AS total "+
			      "FROM shopping_cart sc2, products_in_cart pc, product p2, person p, state st, category c "+
			      "WHERE pc.cart_id = sc2.id "+
			      "AND pc.product_id = p2.id "+
			      "AND p2.category_id = c.id "+
			      "AND c.category_name=? "+
			      "AND sc2.person_id = p.id "+
			      "AND p.state_id = st.id "+
			      "GROUP BY (st.state_name,p2.product_name) "+
			      "ORDER BY p2.product_name "+
			    ") AS salesmade "+
			    "ON allproductsandstates.state_name = salesmade.state_name "+
			    "AND allproductsandstates.product_name = salesmade.product_name "+
			    ")";
	
	
	private final static String GET_SALE = "SELECT total "+
			 						"FROM precomputed_state_topk "+
			 						"WHERE state_name=? "+
			 						"AND product_name=?";
			 						
	private final static String UPDATE_CELL_VALUES_FILTERED = 	
			"UPDATE precomputed_state_topk_filtered "+
			"SET total = precomputed_state_topk_filtered.total+log.total "+
			"FROM log "+
			"WHERE log.state_name=precomputed_state_topk_filtered.state_name "+
			"AND precomputed_state_topk_filtered.product_name=log.product_name";
	
	
	private final static String GET_SALE_FILTERED = "SELECT total "+
													"FROM precomputed_state_topk_filtered "+
													"WHERE state_name=? "+
													"AND product_name=?";
	
	public static void buildPrecomputedStateTopKFiltered(String category_name){
		
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Connection con = null;
		
		
		
		try{
			con = ConnectionManager.getConnection();
			con.setAutoCommit(false);
			
			pstmt = con.prepareStatement(DELETE_AND_INSERT_INTO_PRECOMPUTED_STATETOPK_FILTERED);
			pstmt.setString(1, category_name);
			pstmt.setString(2, category_name);
			pstmt.executeUpdate();
			
			con.commit();
			con.setAutoCommit(true);
			
			
		}catch (SQLException e){
			e.printStackTrace();
		}finally{
		
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
				
			}
			if (con != null) {
				try {
					con.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
				
			}
		}
		return;
		
	}
	
	
	/**
	 * This function updates the Precomputed state_topk table and returns the js ids mapped to the new values.
	 * 
	 * @return
	 */
	
	public static HashMap<String, Integer> updatePrecomputedStateTopK(){
		ResultSet rs = null;
		ResultSet rc = null;
		PreparedStatement pstmt = null;
		Statement stmt = null;
		//HashMap<String, Map <String,Integer>> newCellValues = new HashMap<>();
		//ArrayList<String> states = new ArrayList<>();
		
		HashMap<String,Integer> updatedCells = new HashMap<>();
		Connection conn = null;
		
		try {
			conn = ConnectionManager.getConnection();
			conn.setAutoCommit(false);
			
			pstmt = conn.prepareStatement(UPDATE_CELL_VALUES);
			
			pstmt.executeQuery();
			
			/*rs = stmt.executeQuery("SELECT * FROM state");
			
			
			while (rs.next()) {
				states.add(rs.getString("state_name"));
			}
			
			newCellValues = StateDAO.getStateMappingAllProducts(states);*/
			
			pstmt = conn.prepareStatement(GET_SALE);
			rs = stmt.executeQuery("SELECT * FROM log");
			
			while (rs.next()) {
				String state_name = rs.getString("state_name");
			    String product_name = rs.getString("product_name");
			    
			    pstmt.setString(1, state_name);
			    pstmt.setString(2,product_name);
			    
			    rc = pstmt.executeQuery();
			    int newTotal = rc.getInt(1);
			    

			    updatedCells.put(state_name+product_name,newTotal);	    
				
			}
			
			conn.commit();
			conn.setAutoCommit(true);
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
		finally{
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (rc != null) {
				try {
					rc.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
		}
		return updatedCells;
	}
	
	/**
	 * 
	 * @return returns the js ids mapped to the new values.
	 */
	
	public static HashMap<String, Integer> updatePrecomputedStateTopKFiltered(){
		ResultSet rs = null;
		ResultSet rc = null;
		PreparedStatement pstmt = null;
		Statement stmt = null;
		//HashMap<String, Map <String,Integer>> newCellValues = new HashMap<>();
		//ArrayList<String> states = new ArrayList<>();
		
		HashMap<String,Integer> updatedCells = new HashMap<>();
		Connection conn = null;
		
		try {
			conn = ConnectionManager.getConnection();
			
			conn.setAutoCommit(false);
			pstmt = conn.prepareStatement(UPDATE_CELL_VALUES_FILTERED);
			
			pstmt.executeQuery();

			pstmt = conn.prepareStatement(GET_SALE_FILTERED);
			rs = stmt.executeQuery("SELECT * FROM log");
			
			while (rs.next()) {
				String state_name = rs.getString("state_name");
			    String product_name = rs.getString("product_name");
			    
			    pstmt.setString(1, state_name);
			    pstmt.setString(2,product_name);
			    
			    rc = pstmt.executeQuery();
			    int newTotal = rc.getInt(1);
			    
			    
			    updatedCells.put(state_name+product_name,newTotal);	    
				
			}
			
			
			conn.commit();
			conn.setAutoCommit(true);
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
		finally{
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (rc != null) {
				try {
					rc.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
		}
		return updatedCells;
	}
	
	
}
