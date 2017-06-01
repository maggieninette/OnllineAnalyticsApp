<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<%@ page import="java.sql.Connection, ucsd.shoppingApp.ConnectionManager, ucsd.shoppingApp.*"%>
<%@ page import="ucsd.shoppingApp.models.* , java.util.*" %>    
<%@ taglib prefix = "c" uri = "http://java.sun.com/jsp/jstl/core" %>

<%
	List<String> row_vals = new ArrayList<>();
	List<String> col_vals = new ArrayList<>();
	HashMap<String, Map <String,Integer>> cell_vals = new HashMap<>();	
	Map<String, Integer> totalSales = new HashMap<>();

	if (request.getAttribute("row_values") != null ||
		request.getAttribute("col_values") != null ||
		request.getAttribute("cell_values") != null ||
		request.getAttribute("totalSales") != null) {

		// Get request attributes to build table.
		row_vals = (List<String>) request.getAttribute("row_values"); 
		col_vals = (List<String>) request.getAttribute("col_values");
		cell_vals = (HashMap<String, Map <String,Integer>>) request.getAttribute("cell_values");
		totalSales = (Map<String, Integer>) request.getAttribute("totalSales");
	}
		
	Connection con = ConnectionManager.getConnection();
	CategoryDAO categoryDao = new CategoryDAO(con);
	List<CategoryModel> categoryList = categoryDao.getCategories();
	con.close();
%>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
	<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
	<title>Sales Analytics</title>
</head>
<body>
<a href="./home.jsp">Back to Home</a>
<c:choose>
	<c:when test="${sessionScope.firsttime eq null}">
		<form action="SalesAnalyticsController" method="post" >
			<table>
				<tr>
					<td>Row option:</td>
					<td>
						<select name="row">
							<option value="customer">Customer</option>
							<option value="state">State</option>
						</select>
					</td>
				</tr>
				<tr>
					<td>Order option:</td>
					<td>
						<select name="order">
							<option value="alphabetical">Alphabetical</option>
							<option value="top-k">Top-K</option>
						</select>
					</td>
				</tr>
				<tr>
					<td>Sales filtering option:</td>
					<td>
						<select name="filter">
							<option value="all_products">All Products</option>
							<c:forEach var="category" items="<%= categoryList %>">
								<option value="${category.categoryName}">${category.categoryName}</option>
							</c:forEach>
						</select>
					</td>
				</tr>
			</table>
			<input type="submit" value="Run Query" name="action" />
		</form>
	</c:when>
	<c:otherwise>
		<form action="SalesAnalyticsController" method="post">
			<c:if test="${sessionScope.hideNextRowsBtn eq null}">
				<input type="submit" value="Next 20 Rows" name="action" />
			</c:if>
			<c:if test="${sessionScope.hideNextColsBtn eq null}">
				<input type="submit" value="Next 10 Columns" name="action" />
			</c:if>
			<input type="submit" value="Reset" name="action" />
		</form>
		<table border="1">
		<tr>
			<td></td>
			<%
				//setting up the column headers
				for (int i = 0; i < col_vals.size(); i++) {
			%>
			<td><b><%= col_vals.get(i) %></b></td>
			<%
				}
			%>
		</tr>
		<tr>
			<%
				//looping through the rows
				Map <String, Integer> userSales;
				int sale = 0;
	
				for (int i = 0; i < row_vals.size(); i++) {

				String user;
				user = row_vals.get(i);
		
				userSales = cell_vals.get(user);
				//display the user name/state (row value) and total purchases made
			%>
			<td><b><%= user + " (" + Integer.toString(totalSales.get(user)) + ")" %></b></td>
			<% 
				//loop through col vals to output cell values in correct order
				for (int j = 0; j < col_vals.size(); j++) {
					String temp;
					temp = col_vals.get(j);
				
					//System.out.println(user);
					if (userSales.get(temp) != null) {
						
			
						sale = userSales.get(temp);
						
						if(user.equals("Wisconsin")){
							
							System.out.println("(in jsp). product: "+temp+", total sale: "+Integer.toString(sale));
						}
					}
			%>
			<td><%= sale %></td>
			<% 
				}
			%>
		</tr>
		<% 
			}
		%>
		</table>
	</c:otherwise>
</c:choose>
</body>
</html>