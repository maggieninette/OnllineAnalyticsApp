package ucsd.shoppingApp.controllers;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import ucsd.shoppingApp.ConnectionManager;
import ucsd.shoppingApp.Pair;
import ucsd.shoppingApp.PersonDAO;
import ucsd.shoppingApp.ProductDAO;

/**
 * Servlet implementation class SimilarProductsController
 */
@WebServlet("/SimilarProductsController")
public class SimilarProductsController extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public SimilarProductsController() {
        super();
        // TODO Auto-generated constructor stub
    }

    
    public void displayTop100Similar(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    	
    	Connection conn = null;
    	conn = ConnectionManager.getConnection();
    	PersonDAO person = new PersonDAO(conn);
    	ProductDAO product = new ProductDAO(conn);
    	
    	List<String> allCustomers = person.getAllCustomers();
    	
    	//maps customer to a map (key:product name, value: total purchase)
    	HashMap<String, Map <String,Integer>> customerMapping = person.getCustomerMapping(allCustomers,0);
    	
    	//Get the product vectors
    	Map<String, Map<String,Integer>> productVectors = product.getVector(customerMapping);
    	
    	//Get totalSales per product
    	HashMap<String,Integer> totalSalesPerProduct = product.getTotalSales();
    	
    	
    	//Get list of products.
    	List<String> allProducts = product.getAllProducts();
    	
    	
    	//Mapping of product to a hashmap of their cosine similarity with every other product.
    	Map<Pair, BigDecimal> sortedCosineMap = product.getCosineSimilarity(productVectors,
																			totalSalesPerProduct,
																			allProducts);
  
    	
    	//Call the cosineSimilarity method on every product.
    	//TO-DOOOOOOOOOOOOO
    	
    	request.setAttribute("cosineMap",sortedCosineMap);
    	
    	
    	
    }
    
	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		String forward = "./similarProducts.jsp";
		
		displayTop100Similar(request, response);
		
		response.getWriter().append("Served at: ").append(request.getContextPath());
		
		RequestDispatcher view = request.getRequestDispatcher(forward);
		view.forward(request, response);
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}
