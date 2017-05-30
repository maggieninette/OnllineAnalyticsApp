package ucsd.shoppingApp.controllers;

import java.io.IOException;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;


import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import com.google.gson.Gson;

import ucsd.shoppingApp.ConnectionManager;
import ucsd.shoppingApp.ProductDAO;

/**
 * Servlet implementation class SalesAnalyticsController
 */
@WebServlet("/SalesAnalyticsController")
public class SalesAnalyticsController extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
	

	private Connection con = null;

	public SalesAnalyticsController() {
		con = ConnectionManager.getConnection();
	}

	public class MyPair
	{
	    private final String key;
	    private final int value;

	    public MyPair(String aKey, int aValue)
	    {
	        key   = aKey;
	        value = aValue;
	    }
	    //key is the name (customer/state), value is total sales
	    public String key()   { return key; }
	    public int value() { return value; }
	    

	    
	}

    
    public void bubbleSort(MyPair[] arr) {
    	int n = arr.length;  
        MyPair temp;  
         for(int i=0; i < n; i++){  
                 for(int j=1; j < (n-i); j++){  
                          if(arr[j-1].value > arr[j].value){  
                                 //swap elements  
                                 temp = arr[j-1];  
                                 arr[j-1] = arr[j];  
                                 arr[j] = temp;  
                         }                 
                 }  
         }
    }
	
	
	public void destroy() {
		if (con != null) {
			try {
				con.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void increase_offset(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		HttpSession session = request.getSession();
		int counter = 0;
		if (session.getAttribute("counter") != null){
			counter = (Integer) session.getAttribute("counter");
		}

		counter = counter+1;
		session.setAttribute("counter", counter);		
	}
	
	public void reset_offset(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		HttpSession session = request.getSession();
		session.setAttribute("counter", 0);
	
	}
	

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		response.getWriter().append("Served at: ").append(request.getContextPath());
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 * 
	 */

	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
		String action = request.getParameter("action");
		
		HttpSession session = request.getSession();
		
		if (action.equals("Run Query")) {
			
			// Set filter session variables.
			session.setAttribute("row", request.getParameter("row"));
			session.setAttribute("order", request.getParameter("order"));
			session.setAttribute("filter", request.getParameter("filter"));
		    session.setAttribute("firsttime", false);
		}

		// Get filter session variables.
		String rowOption = (String) session.getAttribute("row");
		String orderOption = (String) session.getAttribute("order");
		String salesFilterOption = (String) session.getAttribute("filter");

		ResultSet rs = null;
		ResultSet rc = null;
		ResultSet rt = null;
	    PreparedStatement pstmt = null;
	    PreparedStatement pstmt2 = null;
	    
	    Connection conn = ConnectionManager.getConnection();

		try{
			Statement statement = conn.createStatement();
			Statement statement1 = conn.createStatement();
			Statement statement2 = conn.createStatement();
			
			
			String getGrandTotalTable = ("select product.id, coalesce(sum (quant*price),0) as grandtotal "+
										"from product left outer join "+ 
										
											"(select product_id as prod_id, customer_name as cust, quantity as quant "+
											"from "+ 
													"(select pc.product_id as product_id, p.person_name as customer_name, "+ 
										 				"pc.quantity as quantity "+
										            "from person p, products_in_cart pc "+
										            "where pc.cart_id in "+
										                "(select sc.id "+ 
										                 "from shopping_cart sc "+
										                 "where sc.person_id = p.id and "+
										                 "sc.is_purchased = 'true') "+
										             ")  as smallertable "+
										     "where customer_name= ? ) as newtable "+
										
										"on product.id = newtable.prod_id "+
										"group by product.id "+
										"; ");
			
		String getGrandTotalbyState = ("select product.id, coalesce(sum (quant*price),0) as grandtotal "+
										"from product left outer join "+
							
											"(select product_id as prod_id, customer_name as cust, quantity as quant "+
											"from state s, "+
													"(select pc.product_id as product_id, p.person_name as customer_name, "+
										             	"p.state_id as state_id, "+
										 				"pc.quantity as quantity "+
										            "from person p, products_in_cart pc "+
										            "where pc.cart_id in "+
										                "(select sc.id "+
										                 "from shopping_cart sc "+
										                 "where sc.person_id = p.id and "+
										                 "sc.is_purchased = 'true') "+
										             ")  as smallertable "+
										     "where state_id = s.id and s.state_name= ? ) as newtable "+
							
										"on product.id = newtable.prod_id "+
										"group by product.id ");
		
		pstmt = conn.prepareStatement("select * from person p, role r where p.role_id = r.id "+
													"and r.role_name = 'Customer' order by p.person_name "+
					  								"limit 20 offset (20*?) ");
		pstmt2 = conn.prepareStatement("select * from state limit 20 offset (20*?)");
		
		//setting up the data structures
		List<String> col_vals = new ArrayList<String>();
		List<String> row_vals = new ArrayList<String>();
		HashMap<Integer, String> product_mapping = new HashMap<>();
		Map <String, Integer> totalsales = new HashMap <>(); //maps user/state to total purchases
		HashMap <String, Integer> grandTotal = new HashMap<>();		
		//maps customer_name to hashmap
		HashMap< String, Map <String,Integer>> totalsales_per_rowval = new HashMap<>();

		// Reset button clicked so resetting session variables.
		if (action.equalsIgnoreCase("Reset")) {
			session.removeAttribute("firsttime");
			session.removeAttribute("row");
			session.removeAttribute("order");
			session.removeAttribute("filter");
		}
	    
		else if (action.equalsIgnoreCase("Run Query") || action.equalsIgnoreCase("Next 20 Rows") ) {
			try {
				
				if (rowOption.equalsIgnoreCase("customer") || rowOption.equalsIgnoreCase("state")) {
					if (orderOption.equalsIgnoreCase("alphabetical") || orderOption.equalsIgnoreCase("top-k")) {
					  	
						if (action.equalsIgnoreCase("Run Query") && rowOption.equalsIgnoreCase("customer")) {
							//reset the counter
							reset_offset(request, response);
							pstmt.setInt(1, 0);
							rs = pstmt.executeQuery();
						}
						else if (action.equalsIgnoreCase("Next 20 Rows") && rowOption.equalsIgnoreCase("customer")) {
							System.out.println("inside");
							//increase the counter
							increase_offset(request, response);
							pstmt.setInt(1, (int) session.getAttribute("counter"));
							rs = pstmt.executeQuery();
						}
						else if (action.equalsIgnoreCase("Run Query") && rowOption.equalsIgnoreCase("state")) {
							//reset the counter
							reset_offset(request, response);
							pstmt2.setInt(1, 0);
							rs = pstmt2.executeQuery();	
						}
						else{
							//increase the counter
							increase_offset(request, response);
							pstmt2.setInt(1, (int) session.getAttribute("counter"));
							rs = pstmt2.executeQuery();	
						}

						//populate row_vals with values for the rows
						String tmp;
						while (rs.next()){
							tmp = rs.getString(2);						
							row_vals.add(tmp);
									 					 	
						}

						//get Map<product id, total sale> for every customer/state and put it in the list 
						//1. loop over the row_vals since those are the customer/state, and 
						//2.for each customer/state:
						//	run the getGrandTotalTable query, 
						//3. get the values into hash map and insert into totalsales_per_rowval
						
						String row_val;
						for (int i = 0; i < row_vals.size(); i++) {
							row_val = row_vals.get(i);
							
							if (rowOption.equalsIgnoreCase("customer")) {
								pstmt = conn.prepareStatement(getGrandTotalTable);
							}
							else{
								pstmt = conn.prepareStatement(getGrandTotalbyState);
							}
							pstmt.setString(1, row_val);
							rs = pstmt.executeQuery();

							
							//getting mapping from product id to product name
							rc = statement2.executeQuery("select * from product");
							while (rc.next()){
								product_mapping.put(rc.getInt(1), rc.getString(3));
								
							}
					
							int prod_id;
							String prod_name;
							while (rs.next()){
	
								prod_id = rs.getInt(1);							
								prod_name = product_mapping.get(prod_id);

								//so in grandTotal, we can look up how much money was spent on each
								//product (each customer/state has a grandTotal map.
								grandTotal.put(prod_name, rs.getInt(2));

							}
							
							//can sum up the values in the grandTotal hashmap to get the total amount
							//of purchases ($) made
							Integer total = 0;
							Integer temp = 0;
							for (Object value : grandTotal.values()) {
							    temp = (Integer) value;
							    
							    total = total+temp;
							}
							//now insert into totalsales hashmap
							totalsales.put(row_val, total);

							totalsales_per_rowval.put(row_val,grandTotal);
							
						}
				
						
						//now get the product names. if all_producuts was chosen, get
						//all products. else, get only the category chosen...
						
						if (salesFilterOption.equalsIgnoreCase("all_products")) {
					
							rt = statement1.executeQuery("select * "
														+ "from product "
														+ "order by product_name "
														+ "limit 10 ");
							
							//System.out.println("obtained result set for products");
							String prod;
							while (rt.next()) {
								prod = rt.getString(3);
				
								col_vals.add(prod);							
							}
						}
						else { //apply the sales filter

							System.out.println("applying sales filter");
							
							pstmt = conn.prepareStatement("select p.product_name from product p, category c "+
														 "where p.category_id = c.id and "+
														 "c.category_name= ? "+
														 "order by product_name "+
														 "limit 10 ");

							pstmt.setString(1, salesFilterOption);
							
							rt = pstmt.executeQuery();
							
							String prod;

							while (rt.next()){
								//System.out.println("filling in col_vals (sales filter applied)");								
								prod = rt.getString("product_name");
			
								col_vals.add(prod);							
							}	
							
						}
						
						//if the TOP-K ordering was chosen, need to order the rows according to
						//how much money was spent

						if (orderOption.equalsIgnoreCase("top-k")) {
							
							//need to sort the total sales
							MyPair[] list = new MyPair[row_vals.size()];
							
							row_vals.clear();
							int i =0;
							for(Map.Entry<String,Integer> entry : totalsales.entrySet()) {
								
								//System.out.println("top k chosen: "+Integer.toString(entry.getValue()));
								MyPair temp = new MyPair(entry.getKey(),entry.getValue());
								
								i++;
						    }
							System.out.println("just filled in the list");
							bubbleSort(list);
							System.out.println("just sorted");
							
							for (int a =0; a<list.length;a++){
								System.out.println(Integer.toString(list[a].value));
							}
						}
					}
				}		
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		// Set table session variables.
		request.setAttribute("row_values",row_vals);
		request.setAttribute("col_values",col_vals);
		request.setAttribute("cell_values", totalsales_per_rowval);
		request.setAttribute("totalSales",totalsales);
		
		request.getRequestDispatcher("/salesanalytics.jsp").forward(request, response);

		//statement.close();
		}
		catch(SQLException e) {
			e.printStackTrace();
		}
		finally {
			if (rs != null && rc != null && rt != null && pstmt != null && conn != null) {
				try {
					rs.close();
					rc.close();
					rt.close();
					pstmt.close();
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
				rs = null;
				rt = null;
				rc = null;
				pstmt = null;
				conn = null;
			}
		}
	}
}