package ucsd.shoppingApp;

import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ucsd.shoppingApp.models.ProductModel;
import ucsd.shoppingApp.models.ProductModelExtended;

public class ProductDAO {
	private static final String SELECT_ALL_PRODUCT_SQL = "SELECT * FROM PRODUCT";

	private static final String ADD_PRODUCT_SQL = "INSERT INTO PRODUCT "
			+ "(sku_id, product_name, price, category_id, created_by) " + "VALUES (?, ?, ?, ?, ?)";

	private static final String SELECT_PRODUCT_BY_ID = "SELECT product.*, category.category_name FROM "
			+ "product, category WHERE " + "product.category_id = category.id AND " + "product.id = ?";

	private static final String UPDATE_PRODUCT_BY_ID = "UPDATE product "
			+ "SET sku_id=?, product_name=?, price=?, category_id=?, " + "modified_by=?" + "WHERE id = ?";

	private static final String FILTER_PRODUCT = "SELECT product.*, category.category_name FROM "
			+ "product, category WHERE " + "product.category_id = category.id";

	private static final String FILTER_PRODUCT_ADMIN = "SELECT product.*, category.category_name, count(products_in_cart.product_id)"
			+ " FROM product INNER JOIN category ON(product.category_id = category.id)"
			+ " LEFT JOIN products_in_cart on (product.id = products_in_cart.product_id)";
	
	private static final String FILTER_PRODUCT_BY_CATEGORY = ("select * from product p, category c "+
															 "where p.category_id = c.id and "+
															 "c.category_name= ? "
															 + "order by product_name "+
															 "limit 10 offset (10*?)");
	
	private static final String SELECT_ALL_PRODUCT_OFFSET = ("select * from product limit 10 offset (10*?)");
	

	private static final String DELETE_PRODUCT_BY_ID = "DELETE FROM product WHERE id=?";
	
	private static final String GET_TOTAL_SALES_FOR_EACH_PRODUCT = "select p.product_name, coalesce(totalsale,0) "+
																	"from product p left outer join "+ 
																	"(select product_id, sum(totalsale) as totalsale "+
																	"from( "+
																        "select prod.product_name as product_name, p.person_name as person_name,  "+
																        					"prod.id as product_id, (prod.price*pc.quantity) as totalsale "+
																        "from person p, product prod, shopping_cart sc, products_in_cart pc "+
																        "where p.id = sc.person_id and sc.id = pc.cart_id and sc.is_purchased='true' "+
																        "and pc.product_id=prod.id) as customerpurchases "+
																    "group by product_id) as salesmade "+
																    
																    "on p.id = product_id";
															
	
	private Connection con;

	public ProductDAO(Connection con) {
		this.con = con;
	}

	public int addProduct(String sku_id, String product_name, Double price, Integer category_id, String created_by)
			throws SQLException {

		PreparedStatement pstmt = null;
		ResultSet rs = null;
		int product_id = -1;
		try {
			pstmt = con.prepareStatement(ADD_PRODUCT_SQL, Statement.RETURN_GENERATED_KEYS);
			pstmt.setString(1, sku_id);
			pstmt.setString(2, product_name);
			pstmt.setDouble(3, price);
			pstmt.setInt(4, category_id);
			pstmt.setString(5, created_by);
			int done = pstmt.executeUpdate();
			con.commit();
			rs = pstmt.getGeneratedKeys();
			while (rs.next()) {
				product_id = rs.getInt(1);
			}
			return product_id;

		} catch (SQLException e) {
			e.printStackTrace();
			con.rollback();
			throw e;

		} finally {
			if (rs != null) {
				rs.close();
			}
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	public int updateProductById(Integer prod_id, String sku_id, String prod_name, Double price, Integer category_id,
			String modified_by) throws SQLException {
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		try {
			pstmt = con.prepareStatement(UPDATE_PRODUCT_BY_ID);
			pstmt.setString(1, sku_id);
			pstmt.setString(2, prod_name);
			pstmt.setDouble(3, price);
			pstmt.setInt(4, category_id);
			pstmt.setString(5, modified_by);
			pstmt.setInt(6, prod_id);
			int done = pstmt.executeUpdate();
			if (done == 1) {
				con.commit();
				return prod_id;
			} else {
				con.rollback();
				return 0;
			}
		} catch (SQLException e) {
			e.printStackTrace();
			con.rollback();
			throw e;

		} finally {
			if (rs != null) {
				rs.close();
			}
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	public ArrayList<ProductModel> getProductById(Integer id) throws SQLException {

		PreparedStatement pstmt = null;
		ResultSet rs = null;
		ArrayList<ProductModel> result = new ArrayList<ProductModel>();
		try {
			pstmt = con.prepareStatement(SELECT_PRODUCT_BY_ID);
			pstmt.setInt(1, id);
			rs = pstmt.executeQuery();
			// collect the result in class and send to controller.
			while (rs.next()) {
				result.add(new ProductModel(rs));
			}
			return result;
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	public boolean deleteProductById(Integer id) throws SQLException {
		PreparedStatement pstmt = null;
		try {
			pstmt = con.prepareStatement(DELETE_PRODUCT_BY_ID);
			pstmt.setInt(1, id);
			int done = pstmt.executeUpdate();
			if (done == 1) {
				con.commit();
				return true;
			} else {
				con.rollback();
				return false;
			}
		} catch (SQLException e) {
			e.printStackTrace();
			con.rollback();
			throw e;
		} finally {
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	public ArrayList<ProductModel> filterProduct(String product_name, Integer category_id) throws SQLException {
		StringBuilder sb = new StringBuilder(FILTER_PRODUCT);
		String prod_name_filter = " AND product.product_name LIKE ?";
		String cat_id_filter = " AND product.category_id = ?";
		sb = sb.append(prod_name_filter);
		sb = sb.append(cat_id_filter);
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		ArrayList<ProductModel> result = new ArrayList<ProductModel>();
		try {
			pstmt = con.prepareStatement(sb.toString());
			pstmt.setString(1, "%" + product_name + "%");
			pstmt.setInt(2, category_id);
			rs = pstmt.executeQuery();
			// collect the result in class and send to controller.
			while (rs.next()) {
				result.add(new ProductModel(rs));
			}
			return result;
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	public ArrayList<ProductModelExtended> filterProductAdmin(String product_name, Integer category_id)
			throws SQLException {
		StringBuilder sb = new StringBuilder(FILTER_PRODUCT_ADMIN);
		String prod_name_filter = " WHERE product.product_name LIKE ?";
		String cat_id_filter = " AND product.category_id = ?";
		String group_and_order = " GROUP BY product.id, category.id ORDER BY product.id";
		sb = sb.append(prod_name_filter);
		sb = sb.append(cat_id_filter);
		sb = sb.append(group_and_order);

		PreparedStatement pstmt = null;
		ResultSet rs = null;

		ArrayList<ProductModelExtended> result = new ArrayList<ProductModelExtended>();
		try {
			pstmt = con.prepareStatement(sb.toString());
			pstmt.setString(1, "%" + product_name + "%");
			pstmt.setInt(2, category_id);
			rs = pstmt.executeQuery();
			// collect the result in class and send to controller.
			while (rs.next()) {
				result.add(new ProductModelExtended(rs));
			}
			return result;
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	public ArrayList<ProductModel> filterProduct(String product_name) throws SQLException {
		StringBuilder sb = new StringBuilder(FILTER_PRODUCT);
		String prod_name_filter = " AND product.product_name LIKE ?";
		sb = sb.append(prod_name_filter);

		PreparedStatement pstmt = null;
		ResultSet rs = null;
		ArrayList<ProductModel> result = new ArrayList<ProductModel>();
		try {
			Connection con = ConnectionManager.getConnection();
			pstmt = con.prepareStatement(sb.toString());
			pstmt.setString(1, "%" + product_name + "%");
			rs = pstmt.executeQuery();
			// collect the result in class and send to controller.
			while (rs.next()) {
				result.add(new ProductModel(rs));
			}
			return result;
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	public ArrayList<ProductModelExtended> filterProductAdmin(String product_name) throws SQLException {
		StringBuilder sb = new StringBuilder(FILTER_PRODUCT_ADMIN);
		String prod_name_filter = " WHERE product.product_name LIKE ?";
		String group_and_order = " GROUP BY product.id, category.id ORDER BY product.id";
		sb = sb.append(prod_name_filter);
		sb = sb.append(group_and_order);

		PreparedStatement pstmt = null;
		ResultSet rs = null;
		ArrayList<ProductModelExtended> result = new ArrayList<ProductModelExtended>();
		try {
			pstmt = con.prepareStatement(sb.toString());
			pstmt.setString(1, "%" + product_name + "%");
			rs = pstmt.executeQuery();
			// collect the result in class and send to controller.
			while (rs.next()) {
				result.add(new ProductModelExtended(rs));
			}
			return result;
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	public ArrayList<ProductModel> filterProduct(Integer category_id) throws SQLException {
		StringBuilder sb = new StringBuilder(FILTER_PRODUCT);
		String cat_id_filter = " AND product.category_id = ?";
		sb = sb.append(cat_id_filter);

		PreparedStatement pstmt = null;
		ResultSet rs = null;
		ArrayList<ProductModel> result = new ArrayList<ProductModel>();
		try {
			pstmt = con.prepareStatement(sb.toString());
			pstmt.setInt(1, category_id);
			rs = pstmt.executeQuery();
			// collect the result in class and send to controller.
			while (rs.next()) {
				result.add(new ProductModel(rs));
			}
			return result;
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	public ArrayList<ProductModelExtended> filterProductAdmin(Integer category_id) throws SQLException {
		StringBuilder sb = new StringBuilder(FILTER_PRODUCT_ADMIN);
		String cat_id_filter = " WHERE product.category_id = ?";
		String group_and_order = " GROUP BY product.id, category.id ORDER BY product.id";
		sb = sb.append(cat_id_filter);
		sb = sb.append(group_and_order);

		PreparedStatement pstmt = null;
		ResultSet rs = null;
		ArrayList<ProductModelExtended> result = new ArrayList<ProductModelExtended>();
		try {
			pstmt = con.prepareStatement(sb.toString());
			pstmt.setInt(1, category_id);
			rs = pstmt.executeQuery();
			// collect the result in class and send to controller.
			while (rs.next()) {
				result.add(new ProductModelExtended(rs));
			}
			return result;
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (rs != null) {
				rs.close();
			}
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
	 * This function returns a list of products, and depending on the filter, it either
	 * gets all products (20 at a time) or gets it from a specific category (20 at a time).
	 * 
	 */
	public ArrayList<String> filterProductbyCategory(String filter, int offset){
		ArrayList<String> products = new ArrayList<>();
		ResultSet rs = null;
		PreparedStatement ptst = null;
		
		
		try{

			if(filter.equals("all_products")){
			 
				ptst = con.prepareStatement(SELECT_ALL_PRODUCT_OFFSET);
				ptst.setInt(1, offset);
			}
			else{
				ptst = con.prepareStatement(FILTER_PRODUCT_BY_CATEGORY);

				ptst.setString(1, filter);
				ptst.setInt(2, offset);
			}
			rs = ptst.executeQuery();
			String product;
			while (rs.next()){
				product = rs.getString(3);
				//System.out.println("adding: "+product);
				products.add(product);							
			}
			
			
		}
		catch (SQLException e) {
			// TODO Auto-generated catch block
			 e.printStackTrace();
		}
			
		return products;	
	
	}
	
	/*
	 * This function returns a list of ALL products from a given category (no offset).
	 * 
	 */
	
	public ArrayList<String> getProductsFromCategory(String category){
		ArrayList<String> products = new ArrayList<>();
		ResultSet rs = null;
		PreparedStatement ps = null;
		try{

			ps = ConnectionManager.getConnection().prepareStatement("select * from product p, category c "+
																	"where p.category_id = c.id "+
																	"and c.category_name = ?");
			ps.setString(1, category);
			
			rs = ps.executeQuery();
			
			while(rs.next()){
				products.add(rs.getString("product_name"));
			}
			
			
		}catch (SQLException e){
			e.printStackTrace();
		}finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		
		return products;
	}
	
	
	//This function returns a map of products and the total sales made for that product. 
	public HashMap<String,Integer> getTotalSales(){
		HashMap<String, Integer> totalSalesPerProduct = new HashMap<>();
		
		ResultSet rs = null;
		Statement stmt = null;
		try{
			stmt = ConnectionManager.getConnection().createStatement();
			rs = stmt.executeQuery(GET_TOTAL_SALES_FOR_EACH_PRODUCT);
			
			while(rs.next()){
				System.out.println("product: "+rs.getString("product_name")+", total sale: "+Integer.toString(rs.getInt(2)));
				totalSalesPerProduct.put(rs.getString("product_name"),rs.getInt(2));
			}
			
		}catch (SQLException e){
			e.printStackTrace();
		}
		finally{
			try {
				if (rs != null) {
					rs.close();
				}
				if (stmt != null) {
					stmt.close();
				}
				
			}
			catch(Exception e){
				e.printStackTrace();
			}
		}
		
		return totalSalesPerProduct;
		
	}
	
	
	/*
	 * This function returns  map of products and their hashmap(vector) (key: customer, value: total sale)
	 */
	public Map<String, Map<String,Integer>> getVector(HashMap<String, Map <String,Integer>> customerMapping){
		Map<String, Map<String, Integer>> productVectors = new HashMap<>();
		

		ResultSet rs = null;
		Statement stmt = null;
		try{
			stmt = ConnectionManager.getConnection().createStatement();
			rs = stmt.executeQuery(SELECT_ALL_PRODUCT_SQL);
			
			while(rs.next()){
				
				String productName = rs.getString("product_name");
				
				//Need to build the hashmap for each product
				Map<String,Integer> productVector = new HashMap<>();
				
				for (Map.Entry<String, Map <String,Integer>> entry : customerMapping.entrySet()) {
				    String customerName = entry.getKey();
				    
				    //The hashmap (key:product, value: totalpurchase) for that specific customer.
				    Map<String,Integer> customerPurchaseMap = entry.getValue();


				    int totalPurchaseMadeByCustomer = customerPurchaseMap.get(productName);
				    
				    //System.out.println("customer: "+customerName+" spent "+Integer.toString(totalPurchaseMadeByCustomer)+" on "+productName);
				    productVector.put(customerName,totalPurchaseMadeByCustomer);
				}

				productVectors.put(productName, productVector);
			}
			
		}catch (SQLException e){
			e.printStackTrace();
		}
		finally{
			try {
				if (rs != null) {
					rs.close();
				}
				if (stmt != null) {
					stmt.close();
				}
				
			}
			catch(Exception e){
				e.printStackTrace();
			}
		}
		
		return productVectors;
	}
	
	
	public List<String> getAllProducts(){
		List<String> products = new ArrayList<>();
		
		ResultSet rs = null;
		Statement stmt = null;
		try{
			stmt = ConnectionManager.getConnection().createStatement();
			rs = stmt.executeQuery(SELECT_ALL_PRODUCT_SQL);
			
			while(rs.next()){
				
				String productName = rs.getString("product_name");

				products.add(productName);
			}
			
		}catch (SQLException e){
			e.printStackTrace();
		}
		finally{
			try {
				if (rs != null) {
					rs.close();
				}
				if (stmt != null) {
					stmt.close();
				}
				
			}
			catch(Exception e){
				e.printStackTrace();
			}
		}
		
		
		return products;
		
	}
	
	//Returns a hashmap for a given product, mapping their cosine similarity with every product in
	//the table.
	public Map<Pair,BigDecimal> getCosineSimilarityMapAllProducts(Map<String, Map<String,Integer>> productVectors,
														HashMap<String,Integer> totalSalesPerProduct,
														List<String> allProducts){
		
		
		HashMap<String,BigDecimal> cosineSimilarityMap = new HashMap<>();
		 MathContext mc = new MathContext(4);
		

		Map<String,Integer> otherProductVector;
		String otherProduct;
		Map<Pair, BigDecimal> cosinePairs = new HashMap<>();
		
		//Loop through the allProducts list and get their cosine similarity with givenProduct.
		for (int j = 0; j <allProducts.size();j++){
			String product1 = allProducts.get(j);
			Map<String,Integer> givenProductVector = productVectors.get(product1);
			
			for (int i = 0; i < allProducts.size(); i++){
				if (!allProducts.get(i).equals(product1)){
					BigDecimal accumulator = new BigDecimal(0);
					otherProduct = allProducts.get(i);
					
					//Get the other product's vector.
					otherProductVector = productVectors.get(otherProduct);
					
					
					//Go through both vectors, multiply and add...
				    for (String customer : otherProductVector.keySet()) {
				    	
				    	//System.out.println(Integer.toString(otherProductVector.get(customer)));
				    	//System.out.println(Integer.toString(givenProductVector.get(customer)));
				    	
				    	BigDecimal otherProductTotal = new BigDecimal (otherProductVector.get(customer));
				    	BigDecimal givenProductTotal = new BigDecimal (givenProductVector.get(customer));
				    	
				    	BigDecimal tmp = otherProductTotal.multiply(givenProductTotal,mc);
				    	accumulator = accumulator.add(tmp,mc);   
				    	
				    }
				  //Now divide by (totalSales of givenProduct * totalSales of otherProduct)
				    BigDecimal totalSaleGivenProduct = new BigDecimal(totalSalesPerProduct.get(product1));
				    BigDecimal totalSaleOtherProduct = new BigDecimal(totalSalesPerProduct.get(otherProduct));
				    BigDecimal cosineSimilarity = new BigDecimal(0);
				    
				    //If it equals to 0 then, don't divide.
				    
				    //System.out.println("Accuumulator: "+accumulator+" , "+totalSaleGivenProduct+" , "+totalSaleOtherProduct);
				    
				    BigDecimal divideBy = totalSaleGivenProduct.multiply(totalSaleOtherProduct,mc);
				    if (divideBy.compareTo(new BigDecimal(0))==1){
				    	
				    	cosineSimilarity = accumulator.divide(divideBy,mc);
				    }
				    
	
				    //System.out.println("cosine similarity between "+product1+ " and "+otherProduct+ " is "+cosineSimilarity.toString());
				    cosineSimilarityMap.put(otherProduct, cosineSimilarity);
				    
				    
				    Pair cosinePair = new Pair(product1, otherProduct);
				    cosinePairs.put(cosinePair, cosineSimilarity);
				    
			
				}
			
			}
		
		}

		//return cosineSimilarityMap;
		Map<Pair,BigDecimal> sortedPairs = Pair.sortMap(cosinePairs);
		return sortedPairs;
	}
	
	//Calls the getCosineSimilarityPerProduct on every product. So, in the end, we would have the mapping
	//of product to hashmap of (product, cosine similarity)
	public Map<Pair, BigDecimal> getCosineSimilarity(Map<String, Map<String,Integer>> productVectors,
														HashMap<String,Integer> totalSalesPerProduct,
														List<String> allProducts){
		

		Map<Pair,BigDecimal> sortedPairs = getCosineSimilarityMapAllProducts(productVectors,
														totalSalesPerProduct, allProducts);
		
		
		//Get all entries into a list, then starting from the end of the list, (get 100) and put it back into the map
		Map<Pair,BigDecimal> sortedPairsDescending = new HashMap<>();
		Pair[] arr = new Pair[100];
		
		int i = 0;
		int lastIndex = 0;
		int counter = 1;
		for (Map.Entry<Pair, BigDecimal> entry : sortedPairs.entrySet() ) {
	
			Pair productPair = entry.getKey();
			BigDecimal cosine = entry.getValue();
			

			Pair wrapped = new Pair (productPair,cosine);
			
			arr[i] = wrapped;
			System.out.println("(sorted) " + cosine);
			
			lastIndex = i;
			i++;
			if (counter ==100){
				break;
			}
			
			counter++;
		}
		
		for (int j = lastIndex; j >=0; j--){

			Pair wrapper = arr[j];

			System.out.println("(descending) : "+wrapper.getCosine());
			sortedPairsDescending.put(wrapper.getPair(), wrapper.getCosine());
		
		}
		
			
		return sortedPairsDescending;
		
		
		
		
	}
	
	
}





