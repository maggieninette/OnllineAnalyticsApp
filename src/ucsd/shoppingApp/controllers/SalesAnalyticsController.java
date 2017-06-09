package ucsd.shoppingApp.controllers;

import java.io.IOException;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import ucsd.shoppingApp.ConnectionManager;
import ucsd.shoppingApp.PrecomputedStateTopK;
import ucsd.shoppingApp.PrecomputedTopProductSales;
import ucsd.shoppingApp.PrecomputedTopStateSales;
import ucsd.shoppingApp.ProductDAO;
import ucsd.shoppingApp.StateDAO;

public class SalesAnalyticsController extends HttpServlet {

	private Connection con = null;

	public SalesAnalyticsController() {
	    con = ConnectionManager.getConnection();
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

    /**
     * Increments column offset by one and resets it as a session variable.
     * @param request
     * @param response
     * @throws ServletException
     * @throws IOException
     */
	public void increaseColumnOffset(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

	    HttpSession session = request.getSession();
		int counter = 0;

		if (session.getAttribute("column_counter") != null) {
			counter = (Integer) session.getAttribute("column_counter");
		}

		session.setAttribute("column_counter", ++counter);
	}

    /**
     * Resets session variable for column offset to 0.
     * @param request
     * @param response
     * @throws ServletException
     * @throws IOException
     */
	public void resetOffset(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

	    HttpSession session = request.getSession();
		session.setAttribute("column_counter", 0);
	}
	
	public void filterbyState(HttpServletRequest request, HttpServletResponse response) {
		
		HttpSession session = request.getSession();
		String categoryFilter = (String) session.getAttribute("filter");

		List<String> products = new ArrayList<String>();
		List<String> states = new ArrayList<String>();
		Map <String, Double> totalSales = new HashMap<>();
        HashMap<String, Map<String, Double>> totalSalesPerState = new HashMap<>();
        HashMap<String, Double> totalSalesPerProduct = new HashMap<>();

        states = StateDAO.getStatesTopKList(categoryFilter);
		ProductDAO product = new ProductDAO(ConnectionManager.getConnection());
		
		/** 1. Get mapping of state to total money they spent in purchases and   
		 *  2. Get product names
		 *  ...depending on whether or not a filter was used.
		 */
		if (categoryFilter.equals("all_products")) {
	        
			totalSales = StateDAO.getTotalPurchasesAllProducts(states);  //OPTIMIZED
			
			products = product.getTopKOrderedProducts(null); //Only gets 50.
			// Get Map<product id, total sale> for every state and put it in the list.
			System.out.println("got the top 50 products");
			totalSalesPerState = StateDAO.getStateMappingTop50Products(states, products); //OPTIMIZED
			System.out.println("got the state mappings");
			
		}
		else {

			// A filter was applied so we need to build the precomputed tables for that category
			System.out.println("About to build the precomputedtopstatesales table with filter");
			PrecomputedTopStateSales.buildPrecomputedTopStateSalesFiltered(categoryFilter);
			
			System.out.println("About to build the precomputedtopproductsales table with filter");
			PrecomputedTopProductSales.buildPrecomputedTopProductSalesFiltered(categoryFilter);
			
			
			PrecomputedStateTopK.buildPrecomputedStateTopKFiltered(categoryFilter);
			
			totalSales = StateDAO.getTotalPurchasesPerCategory(states, categoryFilter);
			
			products = product.getTopKOrderedProducts(categoryFilter); //Only gets 50.
			/** Get Map<product id, total sale> for every state and put it in the list.
			 * 
			 */
			totalSalesPerState = StateDAO.getStateMappingFilteredTop50Products(states, products); //OPTIMIZED
		}
		
		/**Get a mapping of the top 50 products to total sales made for that product (with/without filter).
		 * 
		 */
		totalSalesPerProduct = product.getTotalSales(products); //OPTIMIZED

		request.setAttribute("row_values", states); //OK
		request.setAttribute("col_values", products); //FIXED
		request.setAttribute("cell_values", totalSalesPerState); //FIXED
		request.setAttribute("totalSales", totalSales); //OK
		request.setAttribute("totalSalesPerProduct", totalSalesPerProduct);  //FIXED
	}

    /**
     * Services AJAX request from salesanalytics.jsp refresh button and responds with a list of products no longer in
     * the top 50 as JSON.
     * @param request
     * @param response
     * @throws ServletException
     * @throws IOException
     */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        // Get products no longer in top 50.
        List<String> noLongerTopKProductsList = PrecomputedTopProductSales.updateTopProductSalesTable();
        Map<String, Double> newTopKProductsMap = PrecomputedTopProductSales.getNewTop50Products();
        Map<String, Double> updatedTotalSalesMap = PrecomputedStateTopK.updatePrecomputedStateTopK();

        // Convert lists & map to JSON string.
        Gson gson = new Gson();
        String noLongerTopKProductsListAsString = gson.toJson(noLongerTopKProductsList);
        String newTopKProductsMapAsString = gson.toJson(newTopKProductsMap);
        String updatedTotalSalesMapAsString = gson.toJson(updatedTotalSalesMap);

        // Create JSON object and fill with contents.
        JsonArray jsonResponse = new JsonArray();
        jsonResponse.add(noLongerTopKProductsListAsString);
        jsonResponse.add(newTopKProductsMapAsString);
        jsonResponse.add(updatedTotalSalesMapAsString);

        // Get filled JSON object as string.
        String jsonResponseAsString = gson.toJson(jsonResponse);

        // Send response as JSON.
        response.setContentType("application/json");
        PrintWriter out = response.getWriter();
        out.write(jsonResponseAsString);
        out.close();
	}

	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

		String action = request.getParameter("action");
		
		HttpSession session = request.getSession();

        // Set filter session variables when run query button clicked.
		if (action.equals("Run Query")) {
            session.setAttribute("firstTime", false);
            session.setAttribute("filter", request.getParameter("category_filter"));
		}

		// Reset button clicked. Reset session variables.
		if (action.equalsIgnoreCase("Reset")) {
			session.removeAttribute("firstTime");
			session.removeAttribute("filter");
			session.removeAttribute("hideNextColsBtn");
			resetOffset(request, response);
		}

		else {

			if (action.equalsIgnoreCase("Next 50 Columns")) {
				increaseColumnOffset(request, response);
			}

            filterbyState(request, response);
		}
		
		request.getRequestDispatcher("/salesanalytics.jsp").forward(request, response);
	}
}