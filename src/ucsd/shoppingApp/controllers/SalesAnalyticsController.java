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
    class Sort implements Comparator
    {


		@Override
		public int compare(Object o1, Object o2) {
			// TODO Auto-generated method stub
			MyPair x = (MyPair) o1;
			MyPair y = (MyPair) o2;
			
	        int a = x.value;
	        int b = y.value;

	        return a-b;
		}

    }
    
    public void bubbleSort(MyPair[] arr) {
    	System.out.println("entering bubbleeeee sort");
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
        System.out.println("exiting bubble sort");
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
		// TODO Auto-generated method stub
		//doGet(request, response);
		
		HttpSession session = request.getSession();
		
		response.setContentType("text/html");
		
		String row_option = request.getParameter("row");
		String order_option = request.getParameter("order");
		String sales_filter_option = request.getParameter("filter");
		
		Map <String, Integer> totalsales = new HashMap <>(); //maps user/state to total purchases

		
		
		String action = request.getParameter("action");

		ResultSet rs = null;
		ResultSet rc = null;
		ResultSet rt = null;
	    PreparedStatement pstmt = null;
	    PreparedStatement pstmt2 = null;
	    
	    Connection conn = ConnectionManager.getConnection();
	    session.setAttribute("firsttime", false);

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

		if (action.equalsIgnoreCase("reset")){
			session.setAttribute("firsttime", true);
		}
	    		
		else if (action.equalsIgnoreCase("analyze") ) {
			
			//TO_DO: need to get total sales for each customer
			
			PrintWriter out = response.getWriter();
			List<String> col_vals = new ArrayList<String>();
			
			reset_offset(request, response);
			
			
			
			try{
				System.out.println("analyze button was clicked");


				
				if (row_option.equals("customer")) {
					if (order_option.equals("alphabetical")|| (order_option.equalsIgnoreCase("top-k"))){
					  	rs = statement.executeQuery("select * from person p, role r where p.role_id = r.id "+
													"and r.role_name = 'Customer' order by p.person_name "+
					  								"limit 20 ");
						//System.out.println("ran query");
						
						List<String> row_vals = new ArrayList<String>();
						HashMap<Integer, String> product_mapping = new HashMap<>();
						
						//maps customer_name to hashmap
						HashMap< String, Map <String,Integer>> totalsales_per_customer = new HashMap<>();
						
						//populate list with customer names
						String user;
						while (rs.next()){
							user = rs.getString(2);
							//System.out.println("filling in row_vals: "+user);							
							row_vals.add(user);
									 					 	
						}

						//get Map<product id, total sale> for every customer and put it in the list (TO-DO!)
						//1. loop over the row_vals since those are the users, and 
						//2.for each user:
						//	run the getGrandTotalTable query, 
						//3. get the values into hash map and insert into totalsales_per_customer
						
						for (int i =0; i < row_vals.size(); i++) {
							user = row_vals.get(i);
							
							
							pstmt = conn.prepareStatement(getGrandTotalTable);
							pstmt.setString(1, user);
							rs = pstmt.executeQuery();
			
							if (!rs.next()){
								//System.out.println("ResultSet is empty");
								
								}
							
							//getting mapping from product id to product name
							rc = statement2.executeQuery("select * from product");
							while (rc.next()){
								product_mapping.put(rc.getInt(1), rc.getString(3));
								
							}
		
							HashMap <String, Integer> grandTotal = new HashMap<>();							
							int prod_id;
							String prod_name;
							while (rs.next()){
								//System.out.print("in while loop");		
								prod_id = rs.getInt(1);
								
								prod_name = product_mapping.get(prod_id);
								//get product name;
		
								grandTotal.put(prod_name, rs.getInt(2));
								/*if (user.equals("CUST_0")){
									System.out.println("CUST_0 spent " +Integer.toString(rs.getInt(2))+" on "+prod_name);
								}*/

							}
							
							//can sum up the values in the grandTotal hashmap to get the total amount
							//of purchases ($) the user made
							Integer total = 0;
							Integer temp = 0;
							for (Object value : grandTotal.values()) {
							    temp = (Integer) value;
							    
							    total = total+temp;
							}
							//now insert into totalsales hashmap
							totalsales.put(user, total);

							
							totalsales_per_customer.put(user,grandTotal);
							
						}
				
						
						//now get the product names. if all_producuts was chosen, get
						//all products. else, get only the category chosen...
						
						if (sales_filter_option.equals("all_products")){
					
							rt = statement1.executeQuery("select * "
														+ "from product "
														+ "order by product_name "
														+ "limit 10 ");					

							
							//System.out.println("obtained result set for products");
							String prod;
							while (rt.next()){
								prod = rt.getString(3);
				
								col_vals.add(prod);							
							}
						}
						else { //apply the sales filter

							System.out.println("applying sales filter");
							
							pstmt = conn.prepareStatement("select p.product_name from product p, category c "+
														 "where p.category_id = c.id and "+
														 "c.category_name= ? "
														 + "order by product_name "+
														 "limit 10 ");
							
							System.out.println("user chose: "+sales_filter_option);
							pstmt.setString(1, sales_filter_option);
							
							rt = pstmt.executeQuery();

							
							System.out.println("after rt executes query");
							
							if (!rt.next()){
								System.out.println("empty!");
							}

							String prod;

							while (rt.next()){
								//System.out.println("filling in col_vals (sales filter applied)");								
								prod = rt.getString("product_name");
			
								col_vals.add(prod);							
							}	
							
						}
						
						//if the TOP-K ordering was chosen, need to order the rows according to
						//how much money was spent

						if (order_option.equalsIgnoreCase("top-k")){
							
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
						

						request.setAttribute("row_values",row_vals);
						request.setAttribute("col_values",col_vals);
						request.setAttribute("cell_values", totalsales_per_customer);
						request.setAttribute("totalSales", totalsales);

					}
									
					
				}
				else{  //QUERY BY STATE

					if (order_option.equals("alphabetical")){
						
						//get the states
					  	rs = statement.executeQuery("select * from state limit 20");
						
						List<String> row_vals = new ArrayList<String>();
						HashMap<Integer, String> product_mapping = new HashMap<>();
						
						//maps each state to hashmap
						HashMap< String, Map <String,Integer>> totalsales_per_state = new HashMap<>();
						
						//populate list with state names
						String state;
						while (rs.next()){
							//System.out.println("filling in row_vals");
							state = rs.getString(2);
							row_vals.add(state);
									 					 	
						}

						//get Map<product id, total sale> for every state and put it in the list (TO-DO!)
						//1. loop over the row_vals since those are the state, and 
						//2.for each state:
						//	run the getGrandTotalbyState query, 
						//3. get the values into hash map and insert into totalsales_per_state
						
						for (int i =0; i < row_vals.size(); i++) {
							state = row_vals.get(i);
							
							
							pstmt = conn.prepareStatement(getGrandTotalbyState);
							pstmt.setString(1, state);
							rs = pstmt.executeQuery();
							
							if (!rs.next()){
								//System.out.println("ResultSet is empty");
								
								}
							
							//getting mapping from product id to product name
							rc = statement2.executeQuery("select * from product");
							while (rc.next()){
								product_mapping.put(rc.getInt(1), rc.getString(3));
								
							}
		
							HashMap <String, Integer> grandTotal = new HashMap<>();							
							int prod_id;
							String prod_name;
							while (rs.next()){
								//System.out.print("in while loop");		
								prod_id = rs.getInt(1);
								
								prod_name = product_mapping.get(prod_id);
								//get product name;
		
								grandTotal.put(prod_name, rs.getInt(2));
								if (state.equals("California")){
									System.out.println("The state CA has total sale: " +Integer.toString(rs.getInt(2))+" on "+prod_name);
								}

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
							totalsales.put(state, total);							
							
							
							
							//System.out.println(user);								
							totalsales_per_state.put(state, grandTotal);
							
						}
				
						
						//now get the product names
						if (sales_filter_option.equals("all_products")){
							rt = statement1.executeQuery("select * from product order by product_name limit 10");					

							
							//System.out.println("obtained result set for products");
							String prod;
							while (rt.next()){
								prod = rt.getString(3);
								//System.out.println("filling in col_vals");				
								col_vals.add(prod);							
							}
						}
						else { //apply the sales filter


							
							pstmt = conn.prepareStatement("select * from product p, category c "+
														 "where p.category_id = c.id and "+
														 "c.category_name = ? "
														 + "order by product_name "+
														 "limit 10 ");	
							pstmt.setString(1, sales_filter_option);
							rt = pstmt.executeQuery();
							

							
							//System.out.println("obtained result set for products");
							String prod;
							while (rt.next()){
								prod = rt.getString(3);
								//System.out.println("filling in col_vals");				
								col_vals.add(prod);							
							}							
							
						}

						String prod;
						while (rt.next()){
							prod = rt.getString(3);
							//System.out.println("filling in col_vals");				
							col_vals.add(prod);							
						}
						
						//System.out.println("setting attribute");
						request.setAttribute("row_values",row_vals);
						request.setAttribute("col_values",col_vals);
						request.setAttribute("cell_values", totalsales_per_state);
						request.setAttribute("totalSales",totalsales);

					}
									
					
					
				}
			}catch (Exception e) {
				// throw new ServletException("Could not update product.
				// Retry.");
				//request.setAttribute("error", true);
				//request.setAttribute("errorMsg", "Could not update product. " + e.getMessage());
				//e.printStackTrace();
			}
		}
		
		
		//SUPER BAD CODE FIX LATER
		
		
		
		else if (action.equalsIgnoreCase("next 20")) {	
			List<String> col_vals = new ArrayList<String>();
			increase_offset(request,response);
			
			try{
				System.out.println("next 20 was clicked");
				if (row_option.equals("customer")) {
					if (order_option.equals("alphabetical")){
						
						
						System.out.println("made it in!!!");
						
						pstmt2 = conn.prepareStatement("select * from person p, role r where p.role_id = r.id "+
													"and r.role_name = 'Customer' order by p.person_name "+
					  								"limit 20 offset (20*?) ");
						
					  	/* rs = statement.executeQuery("select * from person p, role r where p.role_id = r.id "+
													"and r.role_name = 'Customer' order by p.person_name "+
					  								"limit 20 offset 20 "); */
						
						
						//System.out.println("ran query");
						
						pstmt2.setInt(1, (Integer) session.getAttribute("counter"));
						
						rs = pstmt2.executeQuery();
						
						
						//if there are no 20 tuples to return, offset by however many tuples
						//we've returned
						if (!rs.next()){
							pstmt2 = conn.prepareStatement("select * from person p, role r where p.role_id = r.id "+
													"and r.role_name = 'Customer' order by p.person_name "+
					  								"offset (20*?) ");
							pstmt2.setInt(1, (Integer) session.getAttribute("counter"));
							rs = pstmt2.executeQuery();
						}
						
						List<String> row_vals = new ArrayList<String>();
						HashMap<Integer, String> product_mapping = new HashMap<>();
						
						//maps customer_name to hashmap
						HashMap< String, Map <String,Integer>> totalsales_per_customer = new HashMap<>();
						
						//populate list with customer names
						String user;
						while (rs.next()){
							//System.out.println("filling in row_vals");
							user = rs.getString(2);
							row_vals.add(user);
									 					 	
						}

						//get Map<product id, total sale> for every customer and put it in the list (TO-DO!)
						//1. loop over the row_vals since those are the users, and 
						//2.for each user:
						//	run the getGrandTotalTable query, 
						//3. get the values into hash map and insert into totalsales_per_customer
						
						for (int i =0; i < row_vals.size(); i++) {
							user = row_vals.get(i);
							
							
							pstmt = conn.prepareStatement(getGrandTotalTable);
							pstmt.setString(1, user);
							rs = pstmt.executeQuery();
							
							if (!rs.next()){
								//System.out.println("ResultSet is empty");
								
								}
							
							//getting mapping from product id to product name
							rc = statement2.executeQuery("select * from product");
							while (rc.next()){
								product_mapping.put(rc.getInt(1), rc.getString(3));
								
							}
		
							HashMap <String, Integer> grandTotal = new HashMap<>();							
							int prod_id;
							String prod_name;
							while (rs.next()){
								//System.out.print("in while loop");		
								prod_id = rs.getInt(1);
								
								prod_name = product_mapping.get(prod_id);
								//get product name;
		
								grandTotal.put(prod_name, rs.getInt(2));
								/*if (user.equals("CUST_0")){
									System.out.println("CUST_0 spent " +Integer.toString(rs.getInt(2))+" on "+prod_name);
								}*/

							}
							//can sum up the values in the grandTotal hashmap to get the total amount
							//of purchases ($) the user made
							Integer total = 0;
							Integer temp = 0;
							for (Object value : grandTotal.values()) {
							    temp = (Integer) value;
							    
							    total = total+temp;
							}
							//now insert into totalsales hashmap
							totalsales.put(user, total);
							
							
							
							//System.out.println(user);								
							totalsales_per_customer.put(user,grandTotal);
							
						}
				
						
						//now get the product names. if all_producuts was chosen, get
						//all products. else, get only the category chosen...
						
						if (sales_filter_option.equals("all_products")){
							System.out.println("getting all products");							
							rt = statement1.executeQuery("select * "
														+ "from product "
														+ "order by product_name "
														+ "limit 10 offset 10 ");					

							
							//System.out.println("obtained result set for products");
							String prod;
							while (rt.next()){
								prod = rt.getString(3);
								//System.out.println("filling in col_vals");				
								col_vals.add(prod);							
							}
						}
						else { //apply the sales filter

							System.out.println("applying sales filter");
							
							pstmt = conn.prepareStatement("select p.product_name from product p, category c "+
														 "where p.category_id = c.id and "+
														 "c.category_name= ? "
														 + "order by product_name "+
														 "limit 10 offset 10 ");
							
							System.out.println("user chose: "+sales_filter_option);
							pstmt.setString(1, sales_filter_option);
							
							rt = pstmt.executeQuery();

							
							System.out.println("after rt executes query");
							
							if (!rt.next()){
								System.out.println("empty!");
							}

							String prod;
							
							System.out.println("before while loop");
							while (rt.next()){
								System.out.println("filling in col_vals (sales filter applied)");								
								prod = rt.getString("product_name");
			
								col_vals.add(prod);							
							}	
							
						}
						
						
						//System.out.println("setting attribute");
						request.setAttribute("row_values",row_vals);
						request.setAttribute("col_values",col_vals);
						request.setAttribute("cell_values", totalsales_per_customer);
						request.setAttribute("totalSales", totalsales);

					}
									
					
				}
				else{  //QUERY BY STATE

					if (order_option.equals("alphabetical")){
						
						//get the states
					  	//rs = statement.executeQuery("select * from state limit 20 offset 20 ");
						pstmt2 = conn.prepareStatement("select * from state limit 20 offset (20*?)");

						pstmt2.setInt(1, (Integer) session.getAttribute("counter"));
						
						rs = pstmt2.executeQuery();
						
						//if we don't have 20 tuples to return, return the leftovers (offset by
						//however many tuples we've returned)
						if (!rs.next()){
							pstmt2 = conn.prepareStatement("select * from state offset (20*?)");
							pstmt2.setInt(1, (Integer) session.getAttribute("counter"));
							rs = pstmt2.executeQuery();
						}

					  	
					  	
						List<String> row_vals = new ArrayList<String>();
						HashMap<Integer, String> product_mapping = new HashMap<>();
						
						//maps each state to hashmap
						HashMap< String, Map <String,Integer>> totalsales_per_state = new HashMap<>();
						
						//populate list with state names
						String state;
						while (rs.next()){
							//System.out.println("filling in row_vals");
							state = rs.getString(2);
							row_vals.add(state);
									 					 	
						}

						//get Map<product id, total sale> for every state and put it in the list (TO-DO!)
						//1. loop over the row_vals since those are the state, and 
						//2.for each state:
						//	run the getGrandTotalbyState query, 
						//3. get the values into hash map and insert into totalsales_per_state
						
						for (int i =0; i < row_vals.size(); i++) {
							state = row_vals.get(i);
							
							
							pstmt = conn.prepareStatement(getGrandTotalbyState);
							pstmt.setString(1, state);
							rs = pstmt.executeQuery();
							
							if (!rs.next()){
								//System.out.println("ResultSet is empty");
								
								}
							
							//getting mapping from product id to product name
							rc = statement2.executeQuery("select * from product");
							while (rc.next()){
								product_mapping.put(rc.getInt(1), rc.getString(3));
								
							}
		
							HashMap <String, Integer> grandTotal = new HashMap<>();							
							int prod_id;
							String prod_name;
							while (rs.next()){
								//System.out.print("in while loop");		
								prod_id = rs.getInt(1);
								
								prod_name = product_mapping.get(prod_id);
								//get product name;
		
								grandTotal.put(prod_name, rs.getInt(2));
								if (state.equals("California")){
									System.out.println("The state CA has total sale: " +Integer.toString(rs.getInt(2))+" on "+prod_name);
								}

							}
							
							//can sum up the values in the grandTotal hashmap to get the total amount
							//of purchases ($) the user made
							Integer total = 0;
							Integer temp = 0;
							for (Object value : grandTotal.values()) {
							    temp = (Integer) value;
							    
							    total = total+temp;
							}
							//now insert into totalsales hashmap
							totalsales.put(state, total);
							
							
							//System.out.println(user);								
							totalsales_per_state.put(state, grandTotal);
							
						}
				
						
						//now get the product names
						if (sales_filter_option.equals("all_products")){
							rt = statement1.executeQuery("select * from product order by product_name limit 10 offset 10");					

							
							//System.out.println("obtained result set for products");
							String prod;
							while (rt.next()){
								prod = rt.getString(3);
								//System.out.println("filling in col_vals");				
								col_vals.add(prod);							
							}
						}
						else { //apply the sales filter


							
							pstmt = conn.prepareStatement("select * from product p, category c "+
														 "where p.category_id = c.id and "+
														 "c.category_name = ? "
														 + "order by product_name "+
														 "limit 10 offset 10 ");	
							pstmt.setString(1, sales_filter_option);
							rt = pstmt.executeQuery();
							

							
							//System.out.println("obtained result set for products");
							String prod;
							while (rt.next()){
								prod = rt.getString(3);
								//System.out.println("filling in col_vals");				
								col_vals.add(prod);							
							}							
							
						}

						String prod;
						while (rt.next()){
							prod = rt.getString(3);
							//System.out.println("filling in col_vals");				
							col_vals.add(prod);							
						}
						
						//System.out.println("setting attribute");
						request.setAttribute("row_values",row_vals);
						request.setAttribute("col_values",col_vals);
						request.setAttribute("cell_values", totalsales_per_state);
						request.setAttribute("totalSales", totalsales);

					}
		
				}
			}catch (Exception e) {
				// throw new ServletException("Could not update product.
				// Retry.");
				//request.setAttribute("error", true);
				//request.setAttribute("errorMsg", "Could not update product. " + e.getMessage());
				//e.printStackTrace();
			}
		}		
		
		
		
			request.getRequestDispatcher("/salesanalytics.jsp").forward(request, response);

		//statement.close();
		}
		catch(SQLException e){
			
		}
		
		
		finally {
		    // Release resources in a finally block in reverse-order of
		    // their creation
			    if (rs != null) {
			        try {
			            rs.close();
			        } catch (SQLException e) { } // Ignore
			        rs = null;
			    }
  
			    if (rc != null) {
			        try {
			            rc.close();
			        } catch (SQLException e) { } // Ignore
			        rc = null;
			    } 	    
			    if (rt != null) {
			        try {
			            rt.close();
			        } catch (SQLException e) { } // Ignore
			        rt = null;
			    } 			    
			    if (pstmt != null) {
			        try {
			            pstmt.close();
			        } catch (SQLException e) { } // Ignore
			        pstmt = null;
			    }			    
			    if (conn != null) {
			        try {
			            conn.close();
			        } catch (SQLException e) { } // Ignore
			        conn = null;
			    }
			}		
	}
	
}
