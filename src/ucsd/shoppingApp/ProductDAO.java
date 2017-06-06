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
import java.util.TreeMap;

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
	
	private static final String FILTER_PRODUCT_BY_CATEGORY =
            "SELECT * " +
            "FROM product p, category c " +
            "WHERE p.category_id = c.id " +
            "AND c.category_name = ? " +
            "ORDER BY product_name " +
            "LIMIT 50 " +
            "OFFSET 50 * ?";
	
	private static final String SELECT_ALL_PRODUCT_OFFSET =
            "SELECT * " +
            "FROM product " +
            "LIMIT 50 " +
            "OFFSET 50 * ?";

	private static final String DELETE_PRODUCT_BY_ID = "DELETE FROM product WHERE id = ?";
	
	private static final String GET_TOTAL_SALES_FOR_EACH_PRODUCT =
            "SELECT p.product_name, COALESCE(totalsale, 0) " +
            "FROM product p LEFT OUTER JOIN " +
                    "(SELECT product_id, SUM(totalsale) AS totalsale " +
                    "FROM (" +
                        "SELECT prod.product_name AS product_name, p.person_name AS person_name, prod.id AS product_id, (prod.price * pc.quantity) AS totalsale " +
                        "FROM person p, product prod, shopping_cart sc, products_in_cart pc " +
                        "WHERE p.id = sc.person_id " +
                        "AND sc.id = pc.cart_id " +
                        "AND sc.is_purchased = 'true' " +
                        "AND pc.product_id = prod.id)" +
                    "AS customerpurchases " +
                    "GROUP BY product_id) " +
                    "AS salesmade " +
            "ON p.id = product_id";

	private static final String GET_ALL_PRODUCTS_FROM_CATEGORY_NO_OFFSET =
            "SELECT * " +
            "FROM product p, category c " +
            "WHERE p.category_id = c.id " +
            "AND c.category_name = ?";

	private Connection con;

	public ProductDAO(Connection con) {
		this.con = con;
	}

	public int addProduct(String sku_id, String product_name, Double price, Integer category_id, String created_by)
			throws SQLException {

		PreparedStatement pstmt = null;
		ResultSet rs = null;
		int productId = -1;

		try {
			pstmt = con.prepareStatement(ADD_PRODUCT_SQL, Statement.RETURN_GENERATED_KEYS);
			pstmt.setString(1, sku_id);
			pstmt.setString(2, product_name);
			pstmt.setDouble(3, price);
			pstmt.setInt(4, category_id);
			pstmt.setString(5, created_by);
			con.commit();
			rs = pstmt.getGeneratedKeys();
			while (rs.next()) {

				productId = rs.getInt(1);
			}
			return productId;
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

	public int updateProductById(Integer prodId, String skuId, String prodName, Double price, Integer categoryId, String modifiedBy)
            throws SQLException {

	    PreparedStatement pstmt = null;
		ResultSet rs = null;

		try {
			pstmt = con.prepareStatement(UPDATE_PRODUCT_BY_ID);
			pstmt.setString(1, skuId);
			pstmt.setString(2, prodName);
			pstmt.setDouble(3, price);
			pstmt.setInt(4, categoryId);
			pstmt.setString(5, modifiedBy);
			pstmt.setInt(6, prodId);
			int done = pstmt.executeUpdate();
			if (done == 1) {
				con.commit();
				return prodId;
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

			// Collect the result in class and send to controller.
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
			}
			else {
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

	public ArrayList<ProductModel> filterProduct(String productName, Integer categoryId) throws SQLException {

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
			pstmt.setString(1, "%" + productName + "%");
			pstmt.setInt(2, categoryId);
			rs = pstmt.executeQuery();

			// Collect the result in class and send to controller.
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

	public ArrayList<ProductModelExtended> filterProductAdmin(String productName, Integer categoryId)
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
			pstmt.setString(1, "%" + productName + "%");
			pstmt.setInt(2, categoryId);
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

	public ArrayList<ProductModel> filterProduct(String productName) throws SQLException {

	    StringBuilder sb = new StringBuilder(FILTER_PRODUCT);
		String prod_name_filter = " AND product.product_name LIKE ?";
		sb = sb.append(prod_name_filter);

		PreparedStatement pstmt = null;
		ResultSet rs = null;
		ArrayList<ProductModel> result = new ArrayList<ProductModel>();

		try {
			Connection con = ConnectionManager.getConnection();
			pstmt = con.prepareStatement(sb.toString());
			pstmt.setString(1, "%" + productName + "%");
			rs = pstmt.executeQuery();

			// Collect the result in class and send to controller.
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

	public ArrayList<ProductModelExtended> filterProductAdmin(String productName) throws SQLException {

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
			pstmt.setString(1, "%" + productName + "%");
			rs = pstmt.executeQuery();

			// Collect the result in class and send to controller.
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

	public ArrayList<ProductModel> filterProduct(Integer categoryId) throws SQLException {

	    StringBuilder sb = new StringBuilder(FILTER_PRODUCT);
		String cat_id_filter = " AND product.category_id = ?";
		sb = sb.append(cat_id_filter);

		PreparedStatement pstmt = null;
		ResultSet rs = null;
		ArrayList<ProductModel> result = new ArrayList<ProductModel>();

		try {
			pstmt = con.prepareStatement(sb.toString());
			pstmt.setInt(1, categoryId);
			rs = pstmt.executeQuery();

			// Collect the result in class and send to controller.
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

	public ArrayList<ProductModelExtended> filterProductAdmin(Integer categoryId) throws SQLException {

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
			pstmt.setInt(1, categoryId);
			rs = pstmt.executeQuery();

			// Collect the result in class and send to controller.
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

    /**
     * Returns a list of products. Depending on the given filter, either gets all products or those from a specific
     * category.
     * @param filter
     * @param offset
     * @return
     */
	public ArrayList<String> filterProductbyCategory(String filter, int offset) {

	    ArrayList<String> products = new ArrayList<>();
		ResultSet rs = null;
		PreparedStatement ptst = null;
		
		try {

			if (filter.equals("all_products")) {
				ptst = con.prepareStatement(SELECT_ALL_PRODUCT_OFFSET);
				ptst.setInt(1, offset);
			}
			else {
				ptst = con.prepareStatement(FILTER_PRODUCT_BY_CATEGORY);
				ptst.setString(1, filter);
				ptst.setInt(2, offset);
			}

			rs = ptst.executeQuery();
			String product;
			while (rs.next()) {
				product = rs.getString(3);
				products.add(product);							
			}
		}
		catch (SQLException e) {
		    e.printStackTrace();
		}
			
		return products;
	}

    /**
     * Returns a list of all products from a given category with no offset.
     * @param category
     * @return
     */
	public ArrayList<String> getProductsFromCategory(String category) {
		System.out.println("getting products from that category");
		
	    ArrayList<String> products = new ArrayList<>();
		ResultSet rs = null;
		PreparedStatement ps = null;

		try {
			ps = ConnectionManager.getConnection().prepareStatement(GET_ALL_PRODUCTS_FROM_CATEGORY_NO_OFFSET);
			ps.setString(1, category);
			rs = ps.executeQuery();
			
			while (rs.next()) {
				
				String productName = rs.getString("product_name");
				products.add(productName);
				System.out.println("product: "+productName+" belongs in category: "+category);
			}
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

    /**
     * Returns map of products and their total sales from all customers.
     * @return
     */
	public HashMap<String,Integer> getTotalSales(String filter) {

	    HashMap<String, Integer> totalSalesPerProduct = new HashMap<>();
		
		ResultSet rs = null;
		ResultSet rc= null;
		Statement stmt = null;
		PreparedStatement pt = null;
		ArrayList<String> productsByCategory = new ArrayList<>();

		try {
			stmt = ConnectionManager.getConnection().createStatement();
			rs = stmt.executeQuery("SELECT * FROM top_product_sales ");
			
			if (filter == null) {
				while (rs.next()) {
					totalSalesPerProduct.put(rs.getString("product_name"), rs.getInt("totalsale"));
				}
			}
			else {
				
				pt = ConnectionManager.getConnection().prepareStatement(GET_ALL_PRODUCTS_FROM_CATEGORY_NO_OFFSET);
				pt.setString(1, filter);
				
				//Get the products in that category.
				productsByCategory = getProductsFromCategory(filter);
				
				while (rs.next()) {
					//Only add product if the product belongs to the given category.
					String productName = rs.getString("product_name");
					if (productsByCategory.contains(productName)) {
						totalSalesPerProduct.put(productName, rs.getInt("totalsale"));
					}	
				}		
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (stmt != null) {
					stmt.close();
				}
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		return totalSalesPerProduct;
	}

    /**
     * Returns map of products and their purchase vector.
     * @param customerMapping
     * @return
     */
	public Map<String, Map<String, Integer>> getVector(HashMap<String, Map<String, Integer>> customerMapping) {

	    Map<String, Map<String, Integer>> productVectors = new HashMap<>();

		ResultSet rs = null;
		Statement stmt = null;

		try{
			stmt = ConnectionManager.getConnection().createStatement();
			rs = stmt.executeQuery(SELECT_ALL_PRODUCT_SQL);
			
			while(rs.next()) {
				
				String productName = rs.getString("product_name");
				
				//Need to build the hashmap for each product
				Map<String,Integer> productVector = new HashMap<>();
				
				for (Map.Entry<String, Map <String,Integer>> entry : customerMapping.entrySet()) {
				    String customerName = entry.getKey();
				    
				    //The hashmap (getKey:product, getValue: totalpurchase) for that specific customer.
				    Map<String,Integer> customerPurchaseMap = entry.getValue();
				    int totalPurchaseMadeByCustomer = customerPurchaseMap.get(productName);
                    productVector.put(customerName,totalPurchaseMadeByCustomer);
				}

				productVectors.put(productName, productVector);
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (stmt != null) {
					stmt.close();
				}
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		return productVectors;
	}

    /**
     * Returns list of all products no offset.
     * @return
     */
	public List<String> getAllProducts() {
		List<String> products = new ArrayList<>();
		
		ResultSet rs = null;
		Statement stmt = null;
		try{
			stmt = ConnectionManager.getConnection().createStatement();
			rs = stmt.executeQuery(SELECT_ALL_PRODUCT_SQL);
			
			while (rs.next()) {
				products.add(rs.getString("product_name"));
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (stmt != null) {
					stmt.close();
				}
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}

		return products;
	}

    /**
     * Returns map of given product to their cosine similarity with every other product.
     * @param productVectors
     * @param totalSalesPerProduct
     * @param allProducts
     * @return
     */
	public Map<Pair, BigDecimal> getCosineSimilarityMapAllProducts(
	        Map<String, Map<String, Integer>> productVectors, HashMap<String, Integer> totalSalesPerProduct,
            List<String> allProducts) {

		HashMap<String, BigDecimal> cosineSimilarityMap = new HashMap<>();
        MathContext mc = new MathContext(10);

		Map<String, Integer> otherProductVector;
		String otherProduct;
		Map<Pair, BigDecimal> cosinePairs = new HashMap<>();
		
		//Loop through the allProducts list and get their cosine similarity with givenProduct.
		for (int j = 0; j < allProducts.size(); j++) {

		    String product1 = allProducts.get(j);
			Map<String, Integer> givenProductVector = productVectors.get(product1);
			
			for (int i = 0; i < allProducts.size(); i++) {
				if (!allProducts.get(i).equals(product1)) {
					BigDecimal accumulator = new BigDecimal(0);
					otherProduct = allProducts.get(i);
					
					// Get the other product's vector.
					otherProductVector = productVectors.get(otherProduct);
					
					// Go through both vectors, multiply and add.
				    for (String customer : otherProductVector.keySet()) {
				    	BigDecimal otherProductTotal = new BigDecimal (otherProductVector.get(customer));
				    	BigDecimal givenProductTotal = new BigDecimal (givenProductVector.get(customer));
				    	
				    	BigDecimal tmp = otherProductTotal.multiply(givenProductTotal, mc);
				    	accumulator = accumulator.add(tmp, mc);
				    }

				    // Divide by (totalSales of givenProduct * totalSales of otherProduct).
				    BigDecimal totalSaleGivenProduct = new BigDecimal(totalSalesPerProduct.get(product1));
				    BigDecimal totalSaleOtherProduct = new BigDecimal(totalSalesPerProduct.get(otherProduct));
				    BigDecimal cosineSimilarity = new BigDecimal(0);

				    BigDecimal divideBy = totalSaleGivenProduct.multiply(totalSaleOtherProduct, mc);
				    if (divideBy.compareTo(new BigDecimal(0)) == 1) {
				    	cosineSimilarity = accumulator.divide(divideBy,mc);
				    }

				    cosineSimilarityMap.put(otherProduct, cosineSimilarity);

				    Pair cosinePair = new Pair(product1, otherProduct);
				    cosinePairs.put(cosinePair, cosineSimilarity);
				}
			}
		}

		// Return cosineSimilarityMap.
		Map<Pair, BigDecimal> sortedPairs = Pair.sortMap(cosinePairs);
		return sortedPairs;
	}

    /**
     * Calls getCosineSimilarityPerProduct on every product. Returns mapping of product to map of product to cosine
     * similarity.
     * @param productVectors
     * @param totalSalesPerProduct
     * @param allProducts
     * @return
     */
	public Map<Pair, BigDecimal> getCosineSimilarity(
	        Map<String, Map<String, Integer>> productVectors, HashMap<String, Integer> totalSalesPerProduct,
            List<String> allProducts) {
		
		Map<Pair, BigDecimal> sortedPairs = new TreeMap<>();
		sortedPairs = getCosineSimilarityMapAllProducts(productVectors, totalSalesPerProduct, allProducts);
		
		//Get all entries into a list, then starting from the end of the list, (get 100) and put it back into the map
		Map<Pair, BigDecimal> sortedPairsDescending = new HashMap<>();
		Pair[] arr = new Pair[100];

		int counter = 1;
		ArrayList<Pair> keys = new ArrayList<Pair>(sortedPairs.keySet());

		for (int j = keys.size() - 1; j >= 0; j--) {
			Pair productPair = keys.get(j);
			BigDecimal cosine = sortedPairs.get(productPair);

			sortedPairsDescending.put(productPair, cosine);
			
			if (counter == 100) {
				break;
			}
		
			counter++;
		}

		return sortedPairsDescending;
	}
	
	public List<String> getTopKOrderedProducts(HashMap<String, Integer> totalSalesPerProduct, int offset) {

		List<String> topKOrderedProducts = new ArrayList<>();
		
		// Make pairs (product, total money spent) and sort the list.
		ArrayList<Pair> productTotalPairs = new ArrayList<>();
		int i =0;
	    for (HashMap.Entry<String, Integer> entry : totalSalesPerProduct.entrySet()) {

	    	Pair productTotalPair = new Pair(entry.getKey(),entry.getValue());
	    	productTotalPairs.add(productTotalPair);
	    	i++;
	    }
	    
	    // Sort list of pairs.
	    ArrayList<Pair> sortedProductTotalPairs = Pair.bubbleSort(productTotalPairs);

	    // now put it into the topKOrderedProducts list. Start from the end of the sortedProductTotalPairs...
	    int counter =1;
	    for (int j = sortedProductTotalPairs.size() - (offset * 50) -1; j >= 0; j--) {
	    	
	    	topKOrderedProducts.add(sortedProductTotalPairs.get(j).getKey());
	    	
	    	if (counter == 50) {
	    		break;
	    	}
	    	counter++;
	    }
		
		return topKOrderedProducts;
	}
}
