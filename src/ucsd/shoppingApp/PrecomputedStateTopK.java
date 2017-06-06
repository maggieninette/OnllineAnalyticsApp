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
	
	
	public static HashMap<String, Map <String,Integer>> updatePrecomputedStateTopK(){
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		Statement stmt = null;
		HashMap<String, Map <String,Integer>> newCellValues = new HashMap<>();
		ArrayList<String> states = new ArrayList<>();
		
		try {
			Connection conn = ConnectionManager.getConnection();
			pstmt = conn.prepareStatement(UPDATE_CELL_VALUES);
			
			pstmt.executeQuery();
			
			rs = stmt.executeQuery("SELECT * FROM state");
			while (rs.next()) {
				states.add(rs.getString("state_name"));
			}
			
			newCellValues = StateDAO.getStateMappingAllProducts(states);
	
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
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
		}
		return newCellValues;
	}
	
	
}