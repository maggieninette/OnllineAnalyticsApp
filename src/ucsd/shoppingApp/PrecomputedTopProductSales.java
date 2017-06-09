package ucsd.shoppingApp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class PrecomputedTopProductSales {



	private final static String DELETE_OLD_TOP_50 = "DELETE FROM old_top_50_products";
	
	private final static String DELETE_NEW_TOP_50 = "DELETE FROM new_top_50_products";
	
	private final static String INSERT_INTO_OLD_TOP_50 = 		
					"INSERT INTO old_top_50_products (product_id, product_name, totalsale) "+ 
					"SELECT product_id AS product_id, product_name AS product_name, totalsale AS total "+ 
				    "FROM top_product_sales "+ 
				    "ORDER BY total DESC "+ 
				    "LIMIT 50 ";
	
	private final static String UPDATE_TOP_50_PRODUCTS =

		    "UPDATE top_product_sales "+ 
		    "SET totalsale = top_product_sales.totalsale + logtable.total  "+
		    "FROM  "+
		        "(SELECT product_id, SUM(total) AS total "+ 
		        "FROM log  "+
		        "GROUP BY product_id  "+
		        ") AS logtable  "+
		        "WHERE logtable.product_id = top_product_sales.product_id  "+
		        "AND top_product_sales.product_id = logtable.product_id ";
	
	private final static String CLEAR_LOG = "DELETE FROM log";
	
	private final static String INSERT_INTO_NEW_TOP_50 =	
		    "INSERT INTO new_top_50_products (product_id, product_name, totalsale) "+ 
		    "SELECT product_id AS product_id, product_name AS product_name, totalsale AS total  "+
		    "FROM top_product_sales "+
		    "ORDER BY total DESC "+ 
		    "LIMIT 50 ";	
	
	private final static String GET_PRODUCTS_OUT_OF_TOP_50 =	
		    "SELECT * "+
		    "FROM old_top_50_products "+
		    "WHERE product_id NOT IN "+
		        "( SELECT product_id "+ 
		         "FROM new_top_50_products) "; 
			
	

//XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
	
	private static final String DELETE_AND_INSERT_INTO_PRECOMPUTED_TOP_PRODUCT_SALES_FILTERED =	
			"DELETE FROM top_product_sales_filtered; "+
	 
			"INSERT INTO top_product_sales_filtered( "+
			"SELECT t2.pr_id as product_id, t2.pr_name as product_name, "+ 
	    			"t2.c_name as category_name, "+
	 				"totalsale as total "+
			"FROM "+ 
				"( SELECT p.id as p_id, p.product_name AS product_name, "+ 
	       		   			"c.category_name as category_name "+
	  				"FROM product p, category c "+  
	       			"WHERE p.category_id=c.id "+
	      				"AND c.category_name=?) AS t1 "+
	  	
	    			"LEFT OUTER JOIN "+
	        
				"( SELECT pr_id as pr_id, pr_name as pr_name, c_name as c_name, SUM(totalsale) AS totalsale "+
	     			"FROM "+
	       		  		"( SELECT prod.product_name AS pr_name, p.person_name AS ps_name, prod.id AS pr_id, "+
	        		  			"(prod.price * pc.quantity) AS totalsale, c.category_name AS c_name "+
	        				"FROM person p, product prod, shopping_cart sc, products_in_cart pc, category c "+
	        				"WHERE p.id = sc.person_id "+
	               				"AND sc.id = pc.cart_id "+
	                    		"AND prod.category_id=c.id "+
	               	    		"AND category_name=? "+
	                    		"AND sc.is_purchased = 'true' "+
	                    		"AND pc.product_id = prod.id ) AS customerpurchases "+
	     
	     			"GROUP BY (pr_id,pr_name,c_name) ) AS t2 "+
	        
	      			"ON (t1.p_id,t1.product_name,t1.category_name) = (t2.pr_id,t2.pr_name,t2.c_name) "+
	     			");";
	

	private final static String UPDATE_TOP_50_PRODUCTS_FILTERED =

		    "UPDATE top_product_sales_filtered "+ 
		    "SET totalsale = top_product_sales_filtered.totalsale + logtable.total  "+
		    "FROM  "+
		        "(SELECT product_id, SUM(total) AS total "+ 
		        "FROM log  "+
		        "GROUP BY product_id  "+
		        ") AS logtable  "+
		        "WHERE logtable.product_id = top_product_sales_filtered.product_id  "+
		        "AND top_product_sales_filtered.product_id = logtable.product_id ";

	private final static String INSERT_INTO_OLD_TOP_50_FILTERED = 		
			"INSERT INTO old_top_50_products (product_id, product_name, totalsale) "+ 
			"SELECT product_id AS product_id, product_name AS product_name, totalsale AS total "+ 
		    "FROM top_product_sales_filtered "+ 
		    "ORDER BY total DESC "+ 
		    "LIMIT 50 ";
	
	private final static String INSERT_INTO_NEW_TOP_50_FILTERED =	
		    "INSERT INTO new_top_50_products (product_id, product_name, totalsale) "+ 
		    "SELECT product_id AS product_id, product_name AS product_name, totalsale AS total  "+
		    "FROM top_product_sales_filtered "+
		    "ORDER BY total DESC "+ 
		    "LIMIT 50 ";	
	

	/*
	 * This function updates the precomputed TopProductSales table and returns the list 
	 * of products that are no longer make it to the top 50.
	 */
	public static List<String> updateTopProductSalesTable() {
		System.out.println("entered update method for top_product_sales");
		
		
		List<List> result = new ArrayList<>();
		
		List<String> noLongerTopK = new ArrayList<>();	
		List<String> newTopK = new ArrayList<>();
		ResultSet rs = null;
		ResultSet rc = null;
		PreparedStatement pstmt = null;
		PreparedStatement ps = null;
		Statement stmt = null;

		Connection con = null;
		
		try {
			con = ConnectionManager.getConnection();
			con.setAutoCommit(false);
			
			stmt = con.createStatement();
			
			pstmt = con.prepareStatement(DELETE_OLD_TOP_50);			
			pstmt.executeUpdate();
			pstmt.close();
			
			
			pstmt = con.prepareStatement(DELETE_NEW_TOP_50);			
			pstmt.executeUpdate();
			pstmt.close();
			
			
			pstmt = con.prepareStatement(INSERT_INTO_OLD_TOP_50);			
			pstmt.executeUpdate();
			pstmt.close();
			
			pstmt = con.prepareStatement(UPDATE_TOP_50_PRODUCTS);			
			pstmt.executeUpdate();
			pstmt.close();
			
//			pstmt = con.prepareStatement(CLEAR_LOG);
//			pstmt.executeUpdate();
//			pstmt.close();
			
			pstmt = con.prepareStatement(INSERT_INTO_NEW_TOP_50);			
			pstmt.executeUpdate();
			pstmt.close();
			
			//Get the products that no longer belong in the top 50.
			rs = stmt.executeQuery(GET_PRODUCTS_OUT_OF_TOP_50);


			//Put the products in a list. 
			while (rs.next()) {
				
				String product_name = rs.getString("product_name");
				System.out.println("these products don't belong in top 50 : "+product_name);
				noLongerTopK.add(product_name);
			}

			
			//System.out.println("adding the 2 lists");
			//result.add(newTopK);
			//result.add(noLongerTopK);
			
		
			
			con.commit();
			con.setAutoCommit(true);

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
			if (rc != null) {
				try {
					rc.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (ps != null) {
				try {
					ps.close();
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
			if (stmt != null) {
				try {
					stmt.close();
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
		

		return noLongerTopK;
	}

    public static void clearLog() {
        Statement stmt = null;
        ResultSet rs = null;
        Connection con = null;

        try{
            con = ConnectionManager.getConnection();
            con.setAutoCommit(false);
            stmt = con.createStatement();
            stmt.executeUpdate(CLEAR_LOG);
            System.out.println("test");
            con.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }

            }
        }
    }


	/**
	 * 
	 * @return The map of new products that made it into the top 50 and their total sales. 
	 */
	
	public static HashMap <String, Double> getNewTop50Products(){
		HashMap <String, Double> newTop50 = new HashMap<>();
		
		Statement stmt = null;
		ResultSet rs = null;
		
		Connection con = null;
		
		try{
			con = ConnectionManager.getConnection();
			stmt = con.createStatement();
			rs = stmt.executeQuery(	"SELECT * "+
									"FROM new_top_50_products "+
									"WHERE product_id "+
									"NOT IN ("+
									"		SELECT product_id "+
									"		FROM old_top_50_products )" );
			
			while (rs.next()) {
				newTop50.put(rs.getString("product_name"), rs.getDouble("totalsale"));
			}
			
			
		}catch (SQLException e) {
			
		}finally{
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (con != null) {
				try {
					con.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			
			}
		}
		return newTop50;
	}
	
	
	public static List<String> updateTopProductSalesFilteredTable() {

		List<List> result = new ArrayList<>();
		
		List<String> noLongerTopK = new ArrayList<>();	
		List<String> newTopK = new ArrayList<>();
		ResultSet rs = null;
		ResultSet rc = null;
		PreparedStatement pstmt = null;
		PreparedStatement ps = null;
		Statement stmt = null;

		Connection con = null;
		
		try {
			con = ConnectionManager.getConnection();
			con.setAutoCommit(false);
			
			stmt = con.createStatement();
			
			pstmt = con.prepareStatement(DELETE_OLD_TOP_50);			
			pstmt.executeUpdate();
			pstmt.close();
			
			
			pstmt = con.prepareStatement(DELETE_NEW_TOP_50);			
			pstmt.executeUpdate();
			pstmt.close();
			
			
			pstmt = con.prepareStatement(INSERT_INTO_OLD_TOP_50_FILTERED);			
			pstmt.executeUpdate();
			pstmt.close();
			
			pstmt = con.prepareStatement(UPDATE_TOP_50_PRODUCTS_FILTERED);			
			pstmt.executeUpdate();
			pstmt.close();
			
			pstmt = con.prepareStatement(CLEAR_LOG);			
			pstmt.executeUpdate();
			pstmt.close();
			
			pstmt = con.prepareStatement(INSERT_INTO_NEW_TOP_50_FILTERED);			
			pstmt.executeUpdate();
			pstmt.close();
			
			//Get the products that no longer belong in the top 50.
			rs = stmt.executeQuery(GET_PRODUCTS_OUT_OF_TOP_50);


			//Put the products in a list. 
			while (rs.next()) {
				
				String product_name = rs.getString("product_name");
				System.out.println("these products don't belong in top 50 : "+product_name);
				noLongerTopK.add(product_name);
			}

			
			//System.out.println("adding the 2 lists");
			//result.add(newTopK);
			//result.add(noLongerTopK);
			
		
			
			con.commit();
			con.setAutoCommit(true);

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
			if (rc != null) {
				try {
					rc.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (ps != null) {
				try {
					ps.close();
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
			if (stmt != null) {
				try {
					stmt.close();
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
		

		return noLongerTopK;
	}
	
	
	public static void buildPrecomputedTopProductSalesFiltered(String category_name){
		
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Connection con = null;
		
		
		try{
			
			con = ConnectionManager.getConnection();
			con.setAutoCommit(false);
			
			
			pstmt = con.prepareStatement(DELETE_AND_INSERT_INTO_PRECOMPUTED_TOP_PRODUCT_SALES_FILTERED);
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
	

	
	
}
