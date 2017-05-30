package ucsd.shoppingApp;

import java.sql.Statement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ucsd.shoppingApp.models.CategoryModel;
import ucsd.shoppingApp.models.ShoppingCartModel;

public class PersonDAO {
	
	private static final String PERSON_EXISTS_SQL = "SELECT ID FROM PERSON WHERE PERSON_NAME = ?";
	private static final String INSERT_PERSON_SQL = "INSERT INTO PERSON(person_name, age, role_id, state_id) "
			+ " VALUES(?, ?, ?, ?) ";
	private static final String GET_PERSON_ROLE = "SELECT role_name FROM ROLE R, PERSON P WHERE P.person_name = ? AND P.role_id = R.id";
	
	public static final String BUILD_TABLE_SQL = ("select product.id, coalesce(sum (quant*price),0) as grandtotal "+
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
	
	public static final String GET_PERSONS_ORDERED_SQL = "select * from person p, role r where p.role_id = r.id "+
										"and r.role_name = 'Customer' order by upper(p.person_name) "+
											"limit 20 offset (20*?) ";
	
	public static final String GET_ALL_CUSTOMERS = "select * from person p, role r where p.role_id = r.id "+
													"and r.role_name = 'Customer' ";
	
	private Connection con = null;
	
	public PersonDAO(Connection con) {
		this.con = con;
	}
	
	
	public boolean personExists(String username) {
		boolean isExists = false;
		PreparedStatement ptst = null;
		ResultSet rs = null;
		try {
			ptst = con.prepareStatement(PERSON_EXISTS_SQL);
			ptst.setString(1, username);
			rs = ptst.executeQuery();
			if(rs.next()) {
				isExists = true;
			} else {
				isExists = false;
			}
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(rs != null) {
					rs.close();
				}
				if(ptst != null) {
					ptst.close();
				}
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
		return isExists;
	}
	
	public int insertPerson(String username, int age, int role_id, int state_id) throws Exception {
		int rows = 0;
		PreparedStatement ptst = null;
		try {
			ptst = con.prepareStatement(INSERT_PERSON_SQL);
			ptst.setString(1, username);
			ptst.setInt(2, age);
			ptst.setInt(3, role_id);
			ptst.setInt(4, state_id);
			rows = ptst.executeUpdate();
			con.commit();
		} catch(Exception e) {
			con.rollback();
			throw e;
		} finally {
			try {
				if(ptst != null) {
					ptst.close();
				}
			} catch(Exception e) {
				throw e;
			}
		}
		return rows;
	}
	
	public String getPersonRole(String username) {
		String role = null;
		PreparedStatement ptst = null;
		ResultSet rs = null;
		try {
			ptst = con.prepareStatement(GET_PERSON_ROLE);
			ptst.setString(1, username);
			rs = ptst.executeQuery();
			if(rs.next()) {
				role = rs.getString(1);
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		finally {
			try {
				if(rs != null) {
					rs.close();
				}
				if(ptst != null) {
					ptst.close();
				}
			} 
			catch(Exception e) {
				e.printStackTrace();
			}
		}
		return role;
	}
	
	public int getIdfromName(String username) {
		int id = -1;
		PreparedStatement ptst = null;
		ResultSet rs = null;
		try {
			ptst = con.prepareStatement(PERSON_EXISTS_SQL);
			ptst.setString(1, username);
			rs = ptst.executeQuery();
			if(rs.next()) {
				id = rs.getInt(1);
			} else {
				id = -1;
			}
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(rs != null) {
					rs.close();
				}
				if(ptst != null) {
					ptst.close();
				}
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
		return id;
	}
	

	
	//this function returns the map that maps a user to a mapping of (key: product, value: total purchase),
	//so we can find out how much money the user spent in total for each product.
	
	public HashMap<String, Map <String,Integer>> getCustomerMapping(List<String> customers){
		
		HashMap<String, Map <String,Integer>> totalsales_per_customer = new HashMap<>();
		HashMap<Integer,String> product_mapping = new HashMap<>();
		
		
		PreparedStatement ptst = null;
		ResultSet rs = null;
		ResultSet rc = null;
		try {

			Statement statement = ConnectionManager.getConnection().createStatement();

		
			String customer;
			for (int i =0; i < customers.size(); i++) {
				HashMap<String, Integer> grandTotal = new HashMap<>();
				customer = customers.get(i);
				ptst = con.prepareStatement(BUILD_TABLE_SQL);
				ptst.setString(1, customer);
					
				//rs is for getting the table for each customer (how much money spent on each product)
				rs = ptst.executeQuery();
				rc = statement.executeQuery("select * from product");

				//getting mapping from product id to product name
				while (rc.next()){
					product_mapping.put(rc.getInt(1), rc.getString(3));						
				}
		
				int product_id;
				String product_name;
				while (rs.next()){

					product_id = rs.getInt(1);							
					product_name = product_mapping.get(product_id);

					//so in grandTotal, we can look up how much money was spent on each
					//product by name (each customer has a grandTotal map.
					grandTotal.put(product_name, rs.getInt(2));

				}

				totalsales_per_customer.put(customer,grandTotal);
				
			}

		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			try {
				if(rs != null) {
					rs.close();
				}
				if(ptst != null) {
					ptst.close();
				}
				if(rc != null) {
					rs.close();
				}
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
		
		
		return totalsales_per_customer;
	
	}
	
	/*
	 * This function returns names of customers using an offset.
	 */
	
	public List<String> getNames(int offset){
		List<String> row_vals = new ArrayList<>();
		ResultSet rs = null;
		
		PreparedStatement ptst = null;
		PreparedStatement ptst2 = null;
		try {
			ptst = con.prepareStatement(GET_PERSONS_ORDERED_SQL);
			ptst.setInt(1, offset);
		
			rs = ptst.executeQuery();

			while (rs.next()){
				row_vals.add(rs.getString("person_name"));
			}	
			
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(rs != null) {
					rs.close();
				}
				if(ptst != null) {
					ptst.close();
				}
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
		return row_vals;
	}
	
	
	
	//returns a mapping of customer to total purchases made across all products
	
	public Map<String,Integer> getTotalPurchasesAllProducts(List<String> customers){
		Map<String,Integer> totalSalesPerCustomer = new HashMap<>();

		HashMap<String, Map <String,Integer>> customerMapping = getCustomerMapping(customers);
		
		//customerMapping is a hashmap that maps user to a hashmap of product names, and total purchase for that product.
		//so, for each customer's hashmap, we sum up the purchases...
		for (Map.Entry<String, Map <String,Integer>> entry : customerMapping.entrySet()) {
		    String customer = entry.getKey();
		    Map <String, Integer> customerMap = entry.getValue();
		    
		    //sum up the values (purchases)
		    int total = 0;
		    int i = 0;
		    for (Map.Entry<String, Integer> entry2 : customerMap.entrySet()){
		    	total = total + entry2.getValue();
		    }
		    

		    totalSalesPerCustomer.put(customer, total);

		}
		
		return totalSalesPerCustomer;
		

	}
	
	
	//returns a mapping of customer to total purchases made across a SPECIFIC category
	
	public Map<String,Integer> getTotalPurchasesPerCategory(List<String> customers, String category){
		Map<String,Integer> totalSalesPerCustomer = new HashMap<>();

		HashMap<String, Map <String,Integer>> customerMapping = getCustomerMapping(customers);
		
		//customerMapping is a hashmap that maps user to a hashmap of product names, and total purchase for that product.
		//so, for each customer's hashmap, we sum up the purchases ONLY for products belonging in
		//certain category...
		ProductDAO p = new ProductDAO(ConnectionManager.getConnection());
		
		ArrayList<String> productsFromSelectedCategory = p.getProductsFromCategory(category);
		
		for (Map.Entry<String, Map <String,Integer>> entry : customerMapping.entrySet()) {
		    String customer = entry.getKey();
		    Map <String, Integer> customerMap = entry.getValue();
		    
		    //sum up the values (purchases)
		    int total = 0;
		    int i = 0;
		    for (Map.Entry<String, Integer> entry2 : customerMap.entrySet()){
		    	if(productsFromSelectedCategory.contains(entry2.getKey())){
		    	
		    		total = total + entry2.getValue();
		    	}
		    }
		    

		    totalSalesPerCustomer.put(customer, total);

		}
		
		return totalSalesPerCustomer;
		
	}
	
	
	//this function builds a list of customers sorted according to the total money they've spent.
	//this list includes ALL customers in the database.
	
	public List<String> buildCustomersTopKlist(String filter){
		List<String> customersTopKSorted = new ArrayList<>();
		List<String> all_customers = new ArrayList<>();

		
		/*for every single customer in the database, find out how much money they spent
		 */
		ResultSet rs = null;
		Statement stmt = null;
		Connection conn = null;
		
		try{
			conn = ConnectionManager.getConnection();
			stmt = conn.createStatement();
			rs = stmt.executeQuery(GET_ALL_CUSTOMERS);
			
			while (rs.next()){
				//System.out.println("adding to all_customers: "+rs.getString("person_name") );
				all_customers.add(rs.getString("person_name"));
				
			}
			
			Map<String,Integer> totalSalesPerCustomer;
			
			
			//we check if sales filter had been applied.
			if (filter.equals("all_products")){	
				totalSalesPerCustomer = getTotalPurchasesAllProducts(all_customers);
			}
			else{
				totalSalesPerCustomer = getTotalPurchasesPerCategory(all_customers,filter);
			}

			//now we make pairs (customer, total money spent) and sort the list.
			Pair[] customerTotalPairs = new Pair[all_customers.size()];
			int i =0;
		    for (Map.Entry<String, Integer> entry : totalSalesPerCustomer.entrySet()){
		    	Pair customerTotalPair = new Pair(entry.getKey(),entry.getValue());
		    	customerTotalPairs[i] = customerTotalPair;
		    	i++;
		    }
		    
		    //sort the list of pairs.
		    Pair[] sortedCustomerTotalPairs = Pair.bubbleSort(customerTotalPairs);
		    
		    //now put it into the customersTopK list. Start from the end of the customerTotalPairs...
		    for (int j = (sortedCustomerTotalPairs.length)-1; j>=0; j--){
		    	customersTopKSorted.add(sortedCustomerTotalPairs[j].key());
		    }
		    
			
			
		}
		catch (SQLException e){
			e.printStackTrace();
		}
	 finally {
		try {
			if(rs != null) {
				rs.close();
			}
			if(stmt != null) {
				stmt.close();
			}
			if(conn != null) {
				conn.close();
			}
			
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
			
		return customersTopKSorted;
	}
	
	
	//this function calls the buildCustomersTopKlist and gets 20 customers at a time.
	
	public List<String> getCustomersTopKlist(String filter,int list_offset){
		
		List<String> customersTopK = new ArrayList<>();
		
		List<String> all_customers_topk_sorted = buildCustomersTopKlist(filter);
		
		//start getting names from the all_customers_topk_sorted list starting from list_offset*20
		int counter = 1;
		for(int i = list_offset*20;i < all_customers_topk_sorted.size(); i++ ){
			
			String customer = all_customers_topk_sorted.get(i);
			customersTopK.add(customer);
			if (counter ==20){
				break;
			}
			counter++;
		}
		
		
		return customersTopK;
	}

	
}






