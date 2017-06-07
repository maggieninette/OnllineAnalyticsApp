package ucsd.shoppingApp;

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
	
	private final static String CREATE_VIEW_OLD_TOP_50 = 	"CREATE OR REPLACE VIEW old_top_50_states AS "+
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
																						"); ";
	
	
	private static final String DROP_AND_BUILD_PRECOMPUTED_TOPSTATESALES_FILTERED =
			"DROP TABLE IF EXISTS top_state_sales_filtered; "+

			"CREATE TABLE top_state_sales_filtered( "+
			  "state_id INTEGER, "+
			  "state_name TEXT, "+
			  "category_name TEXT, "+
			  "totalsale INTEGER "+
			"); "+
			
			
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
	
	private final static String UPDATE_TOP_STATE_SALES_FILTERED = 	
			"UPDATE top_state_sales_filtered "+ 
			"SET totalsale = top_state_sales_filtered.totalsale+logtable.total "+
			"FROM 	( "+
					"SELECT state_name, SUM(total) as total "+
					"FROM log "+
					"GROUP BY state_name "+ 
					") AS logtable "+

			"WHERE logtable.state_name=top_state_sales_filtered.state_name" ;

private final static String CREATE_VIEW_OLD_TOP_50_FILTERED = 	
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
										"); ";

	
	
	public static void buildPrecomputedTopStateSalesFiltered(String category_name){
		
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		try{
			pstmt = ConnectionManager.getConnection().prepareStatement(DROP_AND_BUILD_PRECOMPUTED_TOPSTATESALES_FILTERED);
			pstmt.setString(1, category_name);
			pstmt.setString(2, category_name);
			int tuples = pstmt.executeUpdate();

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
		}
		System.out.println("Returning from buildPrecomputedTopStateSalesFiltered");
		return;
		
	}
	
	
	public static List<String> updateTopStateSalesTable() {
		
		List<String> noLongerTopK = new ArrayList<>();	
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		Statement stmt = null;
		
		try{
			pstmt = ConnectionManager.getConnection().prepareStatement(UPDATE_TOP_STATE_SALES);
			stmt = ConnectionManager.getConnection().createStatement();
			
			//Create the view for old top 50 states.
			rs = stmt.executeQuery(CREATE_VIEW_OLD_TOP_50);	
			
			//Update the Precomputed TotalStateSales table.
			pstmt.executeUpdate();		
			
			//Get the products that no longer belong in the top 50.
			rs = stmt.executeQuery(GET_STATES_OUT_OF_TOP_50);
			
			//Put the products in a list. 
			while (rs.next()) {
				noLongerTopK.add(rs.getString("state_name"));
			}
					
			
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
			
		}
	
		return noLongerTopK;
		
		
	}
	
	public static void updateTopStateSalesFilteredTable() {
		
		List<String> noLongerTopK = new ArrayList<>();	
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		Statement stmt = null;
		
		try{
			stmt = ConnectionManager.getConnection().createStatement();
			rs = stmt.executeQuery(UPDATE_TOP_STATE_SALES_FILTERED);
		
			
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
			
		}
	
		return;
		
		
	}
	
	
	

}






