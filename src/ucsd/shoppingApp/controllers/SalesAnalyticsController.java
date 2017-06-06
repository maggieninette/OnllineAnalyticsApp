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
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import com.google.gson.Gson;
import ucsd.shoppingApp.ConnectionManager;
import ucsd.shoppingApp.PrecomputedStateTopK;
import ucsd.shoppingApp.PrecomputedTopProductSales;
import ucsd.shoppingApp.PrecomputedTopStateSales;
import ucsd.shoppingApp.ProductDAO;
import ucsd.shoppingApp.StateDAO;

@WebServlet("/SalesAnalyticsController")
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
		Map <String, Integer> totalSales = new HashMap <>();
        HashMap<String, Map<String, Integer>> totalSalesPerState = new HashMap<>();
        HashMap<String,Integer> totalSalesPerProduct = new HashMap<>();
        HashMap<String,Integer> totalSalesPerProductByCategory = new HashMap<>();
        String filter;

        states = StateDAO.getStatesTopKList(categoryFilter);
		ProductDAO product = new ProductDAO(ConnectionManager.getConnection());

		// Get Map<product id, total sale> for every customer/state and put it in the list.
		totalSalesPerState = StateDAO.getStateMappingAllProducts(states); //OPTIMIZED
				
		//Get a mapping of products to total sales made for that product (with/without filter).
		totalSalesPerProduct = product.getTotalSales(null); //OPTIMIZED
		totalSalesPerProductByCategory = product.getTotalSales(categoryFilter); //OPTIMIZED

		if (categoryFilter.equals("all_products")) {
	        // Get mapping of state to total money they spent in purchases.
			totalSales = StateDAO.getTotalPurchasesAllProducts(states);  //OPTIMIZED
			
			products = product.getTopKOrderedProducts(totalSalesPerProduct,(int) session.getAttribute("column_counter"));
		}
		else {
	        // Get mapping of state to total money they spent in purchases, filtered by category.
			totalSales = StateDAO.getTotalPurchasesPerCategory(states, categoryFilter);
			products = product.getTopKOrderedProducts(totalSalesPerProductByCategory,(int) session.getAttribute("column_counter"));
		}
		
		// Check if next columns button should be displayed.
		if (product.filterProductbyCategory(categoryFilter, ((int) session.getAttribute("column_counter") + 1)).isEmpty())
			session.setAttribute("hideNextColsBtn", true);
		
		request.setAttribute("row_values", states);
		request.setAttribute("col_values", products);
		request.setAttribute("cell_values", totalSalesPerState);
		request.setAttribute("totalSales", totalSales);
		request.setAttribute("totalSalesPerProduct", totalSalesPerProduct);
	}
	
	public void updatePrecomputedTables(HttpServletRequest request, HttpServletResponse response) {

	    // TODO: Move into doGet.
		List<String> noLongerTopKProducts = new ArrayList<>();
		List<String> noLongerTopKStates = new ArrayList<>();
		HashMap<String, Map<String, Integer>> newCellValues = new HashMap<>();
		
		noLongerTopKProducts = PrecomputedTopProductSales.updateTopProductSalesTable();
		PrecomputedTopStateSales.updateTopStateSalesTable();
		PrecomputedStateTopK.updatePrecomputedStateTopK();
		
		PrecomputedTopProductSales.clearLogTable(); //Clears the log table.
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

        System.out.println("Server received AJAX request.");

        // Get products no longer in top 50.
        List<String> noLongerTopKProductsSample = new ArrayList<>();
        noLongerTopKProductsSample.add("PROD_4527");
        noLongerTopKProductsSample.add("PROD_2896");

        // Get updated total sale prices.
        Map<String, Double> updatedTotalSalePrices = new HashMap<>();
        updatedTotalSalePrices.put("IndianaPROD_4527", 13123987213.12);
        updatedTotalSalePrices.put("VermontPROD_4527", 98722313.12);
        updatedTotalSalePrices.put("KansasPROD_1351", 13123987213.12);

        // Package list as JSON.
        Gson gson = new Gson();
        String noLongerTopKProductsJson = gson.toJson(noLongerTopKProductsSample);
        String updatedTotalSalePricesJson = gson.toJson(updatedTotalSalePrices);


        String jsonResponse = null;

        // Send response as JSON.
        response.setContentType("application/json");
        PrintWriter out = response.getWriter();
        out.print(jsonResponse);
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