package ucsd.shoppingApp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class PrecomputedTopStateSales {
	
	
	private final static String UPDATE_TOP_STATE_SALES = 	"UPDATE top_state_sales "+ 
															"SET totalsale = top_state_sales.totalsale+logtable.total "+
															"FROM 	( "+
																	"SELECT state_name, SUM(total) as total "+
																	"FROM log "+
																	"GROUP BY state_name "+ 
																	") AS logtable "+
					        
															"WHERE logtable.state_name=top_state_sales.state_name" ;
	
	/*private final static String CREATE_VIEW_OLD_TOP_50 = 	"CREATE OR REPLACE VIEW old_top_50_states AS "+
															"SELECT state_name as state_name, totalsale as total "+
															"FROM top_state_sales "+
															"ORDER BY total DESC" +
															"LIMIT 50; ";


	private final static String GET_STATES_OUT_OF_TOP_50 =		"CREATE OR REPLACE VIEW new_top_50_states AS "+
																"SELECT  state_name as state_name, totalsale as total "+
																"FROM top_state_sales "+
																"ORDER BY total DESC" +
																"LIMIT 50; "+


																"SELECT * "+
																"FROM old_top_50_states "+
																"WHERE state_name NOT IN ( "+
																						"SELECT state_name "+
																						"FROM new_top_50_products "+
																						"); "; */
	
			
	private static final String DELETE_AND_INSERT_INTO_PRECOMPUTED_TOP_STATE_SALES_FILTERED =		
			
			" DELETE FROM top_state_sales_filtered; "+
			
			" INSERT INTO top_state_sales_filtered( "+
			"  SELECT allstates.id AS state_id, allstates.state_name AS state_name, "+
			"allstates.c_name, "+
			"COALESCE(total,0) AS total_sale "+
  
			"FROM "+
				"(	SELECT s.id as id, s.state_name as state_name, c.category_name as c_name  "+
					"FROM state s, category c "+
					"WHERE c.category_name =? ) AS allstates "+
				"LEFT OUTER JOIN  "+
    
				"( 	SELECT st.state_name AS state_name, SUM(pc.quantity * pc.price) AS total, "+
     				"c.category_name AS c_name "+
				    "FROM shopping_cart sc2, products_in_cart pc, product p2, person p, state st, "+
					"category c "+
					"WHERE pc.cart_id = sc2.id "+
					"AND pc.product_id = p2.id "+
				    "AND p2.category_id=c.id "+
				    "AND sc2.person_id = p.id "+
				    "AND p.state_id = st.id "+
				    "AND c.category_name=? "+   
				    "GROUP BY (st.state_name,c.category_name) ) AS purchasesperstate "+
    
    
    			"ON (allstates.state_name,allstates.c_name) = (purchasesperstate.state_name,purchasesperstate.c_name) "+
        
  			"ORDER BY total_sale desc "+
  			
  			")";
	
	
	//Only add sales from the log table, the newly purchased products from the same category. 
	
	private final static String UPDATE_TOP_STATE_SALES_FILTERED = 	
			"UPDATE top_state_sales_filtered "+ 
			"SET totalsale = top_state_sales_filtered.totalsale+logtable.total "+ 
			"FROM 	(  "+
					"SELECT state_name,category_name, SUM(total) as total  "+
					"FROM log "+ 
					"GROUP BY (state_name,category_name) "+  
					") AS logtable "+ 

			"WHERE logtable.state_name=top_state_sales_filtered.state_name "+
            "AND logtable.category_name=top_state_sales_filtered.category_name";

/*private final static String CREATE_VIEW_OLD_TOP_50_FILTERED = 	
	"CREATE OR REPLACE VIEW old_top_50_states AS "+
			"SELECT state_name as state_name, totalsale as total "+
			"FROM top_state_sales_filtered "+
			"ORDER BY total DESC" +
			"LIMIT 50; ";


private final static String GET_STATES_OUT_OF_TOP_50_FILTERED =		
	"CREATE OR REPLACE VIEW new_top_50_states AS "+
				"SELECT  state_name as state_name, totalsale as total "+
				"FROM top_state_sales_filtered "+
				"ORDER BY total DESC" +
				"LIMIT 50; "+


				"SELECT * "+
				"FROM old_top_50_states "+
				"WHERE state_name NOT IN ( "+
										"SELECT state_name "+
										"FROM new_top_50_products "+
										"); ";*/

	
	
	public static void buildPrecomputedTopStateSalesFiltered(String category_name){
		
		System.out.println("Entered the method: buildPrecomputedTopStateSalesFiltered");
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		Connection con = null;

		try{
			con = ConnectionManager.getConnection();
			con.setAutoCommit(false);
			
			pstmt = con.prepareStatement(DELETE_AND_INSERT_INTO_PRECOMPUTED_TOP_STATE_SALES_FILTERED);
			pstmt.setString(1, category_name);
			pstmt.setString(2, category_name);
			pstmt.executeUpdate();
			
			System.out.println("executed update");
			pstmt.close();
			
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
		System.out.println("Returning from buildPrecomputedTopStateSalesFiltered");
		return;
		
	}
	
	
	public static void updateTopStateSalesTable() {
		
		List<String> noLongerTopK = new ArrayList<>();	
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		Statement stmt = null;
		Connection con = null;
		
		try{
			con = ConnectionManager.getConnection();
			con.setAutoCommit(false);
			pstmt = con.prepareStatement(UPDATE_TOP_STATE_SALES);
			stmt = con.createStatement();
			
		/*	//Create the view for old top 50 states.
			rs = stmt.executeQuery(CREATE_VIEW_OLD_TOP_50);	*/
			
			//Update the Precomputed TotalStateSales table.
			pstmt.executeUpdate();		

			con.commit();
			con.setAutoCommit(true);		
			
		}
		catch (SQLException e){
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
	
	
	public static void updateTopStateSalesFilteredTable() {
		
		List<String> noLongerTopK = new ArrayList<>();	
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		Statement stmt = null;
		Connection con = null;
		
		try{
			con = ConnectionManager.getConnection();
			con.setAutoCommit(false);
			pstmt = con.prepareStatement(UPDATE_TOP_STATE_SALES_FILTERED);
			stmt = con.createStatement();
			

			
			//Update the Precomputed TotalStateSalesFiltered table.
			pstmt.executeUpdate();		

			con.commit();
			con.setAutoCommit(true);		
			
		}
		catch (SQLException e){
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

}


