package ucsd.shoppingApp;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class PrecomputedTopProductSales {

	private final static String UPDATE_TOP_PRODUCT_SALES =
            "UPDATE top_product_sales " +
            "SET totalsale = top_product_sales.totalsale + logtable.total " +
            "FROM " +
                "(SELECT product_id, SUM(total) AS total " +
                "FROM log " +
                "GROUP BY product_id " +
                ") AS logtable " +
                "WHERE logtable.product_id = top_product_sales.product_id " +
                "AND top_product_sales.product_id = logtable.product_id;";
	
	private final static String CREATE_VIEW_OLD_TOP_50 =
            "CREATE OR REPLACE VIEW old_top_50_products AS " +
            "SELECT product_id AS product_id, product_name AS product_name, totalsale AS total " +
            "FROM top_product_sales " +
            "ORDER BY total DESC " +
            "LIMIT 50;";
	

	private final static String GET_PRODUCTS_OUT_OF_TOP_50 =
            "CREATE OR REPLACE VIEW new_top_50_products AS " +
            "SELECT  product_id AS product_id, product_name AS product_name, totalsale AS total " +
            "FROM top_product_sales " +
            "ORDER BY total DESC " +
            "LIMIT 50; " +
		
            "SELECT * " +
            "FROM old_top_50_products " +
            "WHERE product_id NOT IN " +
                "(SELECT product_id " +
                "FROM new_top_50_products "+
                ");";
	
	private final static String CLEAR_LOG_TABLE = "DELETE FROM log";
	
	public static void clearLogTable() {
		PreparedStatement pstmt = null;
		try {
			pstmt = ConnectionManager.getConnection().prepareStatement(CLEAR_LOG_TABLE);
			pstmt.executeQuery();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		finally{
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}		
		}	
	}

	/*
	 * This function updates the precomputed TopProductSales table and returns the list 
	 * of products that are no longer make it to the top 50.
	 */
	public static List<String> updateTopProductSalesTable() {
		
		List<String> noLongerTopK = new ArrayList<>();	
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		Statement stmt = null;
		
		try {
			pstmt = ConnectionManager.getConnection().prepareStatement(UPDATE_TOP_PRODUCT_SALES);
			stmt = ConnectionManager.getConnection().createStatement();
			
			//Create the view for old top 50 products.
			rs = stmt.executeQuery(CREATE_VIEW_OLD_TOP_50);	
			
			//Update the Precomputed TotalProductSales table.
			pstmt.executeUpdate();		
			
			//Get the products that no longer belong in the top 50.
			rs = stmt.executeQuery(GET_PRODUCTS_OUT_OF_TOP_50);
			
			//Put the products in a list. 
			while (rs.next()) {
				noLongerTopK.add(rs.getString("product_name"));
			}
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
		finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
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
