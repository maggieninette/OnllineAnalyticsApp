package ucsd.shoppingApp;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class PrecomputedTopStateSales {
	
	
	private final static String UPDATE_TOP_STATE_SALES = 	"UPDATE TopStateSales "+ 
															"SET totalsale = TopStateSales.totalsale+logtable.total "+
															"FROM 	( "+
																	"SELECT state_name, SUM(total) as total "+
																	"FROM log "+
																	"GROUP BY state_name "+ 
																	") AS logtable "+
					        
															"WHERE logtable.state_name=TopStateSales.state_name" ;
	
	private final static String CREATE_VIEW_OLD_TOP_50 = 	"CREATE OR REPLACE VIEW old_top_50_states AS "+
															"SELECT state_name as state_name "+
															"FROM TopStateSales "+ 
															"LIMIT 50; ";


	private final static String GET_PRODUCTS_OUT_OF_TOP_50 =	"CREATE OR REPLACE VIEW new_top_50_products AS "+
																"SELECT  state_name as state_name "+
																"FROM TopStateSales "+ 
																"LIMIT 50; "+


																"SELECT * "+
																"FROM old_top_50_products "+
																"WHERE product_id NOT IN ( "+
																						"SELECT state_name "+
																						"FROM new_top_50_products "+
																						"); ";
	
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
			rs = stmt.executeQuery(GET_PRODUCTS_OUT_OF_TOP_50);
			
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
	
	
	

}






