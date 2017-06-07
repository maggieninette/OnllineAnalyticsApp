package ucsd.shoppingApp.controllers;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import ucsd.shoppingApp.ConnectionManager;
import ucsd.shoppingApp.Pair;
import ucsd.shoppingApp.PersonDAO;
import ucsd.shoppingApp.ProductDAO;

@WebServlet("/SimilarProductsController")
public class SimilarProductsController extends HttpServlet {
    
    public void displayTop100Similar(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    	
    	Connection conn = null;
    	conn = ConnectionManager.getConnection();
    	PersonDAO person = new PersonDAO(conn);
    	ProductDAO product = new ProductDAO(conn);
    	
    	List<String> allCustomers = person.getAllCustomers();
    	
    	//maps customer to a map (getKey:product name, getValue: total purchase)
    	HashMap<String, Map <String, Integer>> customerMapping = person.getCustomerMappingAllProducts(allCustomers);
    	
    	//Get the product vectors
//    	Map<String, Map<String, Integer>> productVectors = product.getVector(customerMapping);
    	
    	//Get totalSales per product
    	HashMap<String,Integer> totalSalesPerProduct = product.getTotalSales(null);
    	
    	//Get list of products.
    	List<String> allProducts = product.getAllProducts();

    	//Mapping of product to a hashmap of their cosine similarity with every other product.
//    	Map<Pair, BigDecimal> sortedCosineMap = product.getCosineSimilarity(
//    	        productVectors, totalSalesPerProduct, allProducts);

//    	request.setAttribute("cosineMap", sortedCosineMap);
    }

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

		displayTop100Similar(request, response);
		response.getWriter().append("Served at: ").append(request.getContextPath());
		request.getRequestDispatcher("./similarProducts.jsp").forward(request, response);
	}

	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request, response);
	}

}
