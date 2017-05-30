package ucsd.shoppingApp.controllers;

import java.io.IOException;

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

import ucsd.shoppingApp.ConnectionManager;
import ucsd.shoppingApp.PersonDAO;
import ucsd.shoppingApp.ProductDAO;
import ucsd.shoppingApp.StateDAO;

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


	public void destroy() {
		if (con != null) {
			try {
				con.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void increase_row_offset(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		HttpSession session = request.getSession();
		int counter = 0;
		if (session.getAttribute("row_counter") != null){
			counter = (Integer) session.getAttribute("row_counter");
		}

		counter = counter+1;
		session.setAttribute("row_counter", counter);		
	}
	
	public void increase_column_offset(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		HttpSession session = request.getSession();
		int counter = 0;
		if (session.getAttribute("column_counter") != null){
			counter = (Integer) session.getAttribute("column_counter");
		}

		counter = counter+1;
		session.setAttribute("column_counter", counter);		
	}
	
	public void reset_offset(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		HttpSession session = request.getSession();
		session.setAttribute("row_counter", 0);
		session.setAttribute("column_counter", 0);
	
	}
	
	public void filterbyCustomer(HttpServletRequest request, HttpServletResponse response){
		
		// Get session variables.
		HttpSession session = request.getSession();
		String sales_filter_option = (String) session.getAttribute("filter");	
		String order_option = (String) session.getAttribute("order");
		
		//setting up the data structures
		List<String> products = new ArrayList<String>();
		List<String> customers = new ArrayList<String>();
		Map<String, Integer> totalSales = new HashMap<>();

		//maps customer_name to hashmap of (key:product name, value: purchases)
		HashMap< String, Map <String,Integer>> totalsales_per_customer = new HashMap<>();
			
		PersonDAO person = new PersonDAO(ConnectionManager.getConnection());
		
		//filling in row values list with customer names.
		
		//first, check if it's ordered by alphabetical or top-k.
		if (order_option.equals("alphabetical")) {
			customers = person.getNames((int)session.getAttribute("row_counter"));
			
			// Check if next rows button should be displayed.
			if (person.getNames((int) session.getAttribute("row_counter") + 1).isEmpty())
				session.setAttribute("hideNextRowsBtn", true);
		}
		else { //do the top-k ordering.
			
			//we need to see if a sales filter has been applied, but we will do that in the PersonDAO class
			//when we build the list of customers sorted according to total purchases made...
			
			customers = person.getCustomersTopKlist(sales_filter_option,(int)session.getAttribute("row_counter"));	
			
			// Check if next rows button should be displayed.
			if (person.getCustomersTopKlist(sales_filter_option, ((int) session.getAttribute("row_counter") + 1)).isEmpty())
				session.setAttribute("hideNextRowsBtn", true);
		}
		
		//getting the mapping of user to the total money they spent in purchases.
		if (sales_filter_option.equals("all_products")) {
			totalSales = person.getTotalPurchasesAllProducts(customers);
		}
		else {
			totalSales = person.getTotalPurchasesPerCategory(customers, sales_filter_option);
		}

		//get Map<product id, total sale> for every customer/state and put it in the list. 
		totalsales_per_customer = person.getCustomerMapping(customers);

		//get column values (product names) depending on the filter selected.
		ProductDAO product = new ProductDAO(ConnectionManager.getConnection());
		products = product.filterProductbyCategory(sales_filter_option,(int)session.getAttribute("column_counter"));
		
		// Check if next columns button should be displayed.
		if (product.filterProductbyCategory(sales_filter_option, ((int) session.getAttribute("column_counter") + 1)).isEmpty())
			session.setAttribute("hideNextColsBtn", true);
				
		// Set table session variables.
		request.setAttribute("row_values",customers);
		request.setAttribute("col_values",products);
		request.setAttribute("cell_values", totalsales_per_customer);
		request.setAttribute("totalSales", totalSales);

	}
	
	public void filterbyState(HttpServletRequest request, HttpServletResponse response){
		
		HttpSession session = request.getSession();
		String sales_filter_option = (String) session.getAttribute("filter");
		String order_option = (String) session.getAttribute("order");
		
		//setting up the data structures
		List<String> products = new ArrayList<String>();
		List<String> states = new ArrayList<String>();

		Map <String, Integer> totalSales = new HashMap <>(); //maps customer to total purchases

		//maps state_name to hashmap
		HashMap< String, Map <String,Integer>> totalsales_per_state = new HashMap<>();

		//filling in row values list with state names.
		
		//first, check if it's ordered by alphabetical or top-k.
		if (order_option.equals("alphabetical")) {
			states = StateDAO.getStatesOffset((int) session.getAttribute("row_counter"));
			
			// Check if next rows button should be displayed.
			if (StateDAO.getStatesOffset(((int) session.getAttribute("row_counter")) + 1).isEmpty())
				session.setAttribute("hideNextRowsBtn", true);
		}
		else { //do the top-k ordering.
			//we need to see if a sales filter has been applied.
			if (sales_filter_option.equals("all_products")){
				
				//states =
			}
			else{
				//states =
			}
		
		}
		//getting the mapping of state to the total money they spent in purchases.
		if (sales_filter_option.equals("all_products")){
			totalSales = StateDAO.getTotalPurchasesAllProducts(states);
		}
		else{
			totalSales = StateDAO.getTotalPurchasesPerCategory(states, sales_filter_option);
		}

		//get Map<product id, total sale> for every customer/state and put it in the list. 
		totalsales_per_state = StateDAO.getStateMapping(states);

		//get column values (product names) depending on the filter selected.
		ProductDAO product = new ProductDAO(ConnectionManager.getConnection());
		products = product.filterProductbyCategory(sales_filter_option,(int)session.getAttribute("column_counter"));
		
		// Check if next columns button should be displayed.
		if (product.filterProductbyCategory(sales_filter_option, ((int) session.getAttribute("column_counter") + 1)).isEmpty())
			session.setAttribute("hideNextColsBtn", true);
		
		request.setAttribute("row_values",states);
		request.setAttribute("col_values",products);
		request.setAttribute("cell_values", totalsales_per_state);
		request.setAttribute("totalSales", totalSales);
	}
	
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		response.getWriter().append("Served at: ").append(request.getContextPath());
	}

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

		// Get row filter session variable.
		String rowOption = (String) session.getAttribute("row");

		// Reset button clicked so resetting session variables.
		if (action.equalsIgnoreCase("Reset")) {
			session.removeAttribute("firsttime");
			session.removeAttribute("row");
			session.removeAttribute("order");
			session.removeAttribute("filter");
			session.removeAttribute("hideNextRowsBtn");
			session.removeAttribute("hideNextColsBtn");
			reset_offset(request, response);
		}
		
		else {
			// Increase either row or column offset.
			if (action.equalsIgnoreCase("Next 20 Rows")) {
				increase_row_offset(request, response);
			}

			else if (action.equalsIgnoreCase("Next 10 Columns")) {
				increase_column_offset(request, response);
			}

			// Filter by customer or state.
			if (rowOption.equalsIgnoreCase("customer"))
				filterbyCustomer(request, response);
			else if (rowOption.equalsIgnoreCase("state"))
				filterbyState(request, response);
		}
		
		request.getRequestDispatcher("/salesanalytics.jsp").forward(request, response);
	}
}