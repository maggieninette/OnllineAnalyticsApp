<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<%@ page import="java.sql.Connection, ucsd.shoppingApp.ConnectionManager, ucsd.shoppingApp.*"%>
<%@ page import="ucsd.shoppingApp.models.* , java.util.*" %>    
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>

<%
	List<String> rowVals = new ArrayList<>();
	List<String> colVals = new ArrayList<>();
	HashMap<String, Map<String,Integer>> cellVals = new HashMap<>();
	Map<String, Integer> totalSales = new HashMap<>();
	HashMap<String,Integer> totalSalesPerProduct = new HashMap<>();

	if (request.getAttribute("row_values") != null || request.getAttribute("col_values") != null ||
		request.getAttribute("cell_values") != null || request.getAttribute("totalSales") != null) {

		// Get request attributes to build table.
		rowVals = (List<String>) request.getAttribute("row_values");
		colVals = (List<String>) request.getAttribute("col_values");
		cellVals = (HashMap<String, Map <String,Integer>>) request.getAttribute("cell_values");
		totalSales = (Map<String, Integer>) request.getAttribute("totalSales");
		totalSalesPerProduct = (HashMap<String, Integer>) request.getAttribute("totalSalesPerProduct");
	}

	Connection con = ConnectionManager.getConnection();
	CategoryDAO categoryDao = new CategoryDAO(con);
	List<CategoryModel> categoryList = categoryDao.getCategoriesAlphabetical();
	con.close();
%>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
	<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
	<title>Sales Analytics</title>
    <link rel="stylesheet" href="css/salesanalytics.css" />
</head>
<body>
<a id="floating-refresh-link" onclick="refreshTable()">Refresh</a>
<a id="back-home-link" href="./home.jsp">Back to Home</a>
<c:choose>
	<c:when test="${sessionScope.firstTime eq null}">
		<form action="SalesAnalyticsController" method="post" >
            <h3>Category Filter:</h3>
            <select name="category_filter">
                <option value="all_products" selected>All Products</option>
                <c:forEach var="category" items="<%= categoryList %>">
                    <option value="${category.categoryName}">${category.categoryName}</option>
                </c:forEach>
            </select>
            <input type="submit" value="Run Query" name="action" />
		</form>
	</c:when>
	<c:otherwise>
		<form action="SalesAnalyticsController" method="post">
	 		<c:if test="${sessionScope.hideNextColsBtn eq null}"> 
				<input type="submit" value="Next 50 Columns" name="action" />
	   		</c:if> 
			<input type="submit" value="Reset" name="action" />
		</form>
		<table border="1">
		<tr>
			<td></td>
			<%
				// Setting up column headers.
				for (int i = 0; i < colVals.size(); i++) {
					String productName = colVals.get(i);
					int totalSalePerProduct = totalSalesPerProduct.get(productName);
			%>
			<td><b><%= productName+ " ("+Integer.toString(totalSalePerProduct)+ ")" %></b></td>
			<%
				}
			%>
		</tr>
		<tr>
			<%
				// Loop through rows.
				Map <String, Integer> userSales;
				int sale = 0;
	
				for (int i = 0; i < rowVals.size(); i++) {
				    String user;
				    user = rowVals.get(i);
				    userSales = cellVals.get(user);
			%>
			<td><b><%= user + " (" + Integer.toString(totalSales.get(user)) + ")" %></b></td>
			<% 
				// Loop through columns.
                for (int j = 0; j < colVals.size(); j++) {
					String temp;
					temp = colVals.get(j);

					if (userSales.get(temp) != null) {
						sale = userSales.get(temp);
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
<script src="js/jquery-3.2.1.min.js"></script>
<script src="js/salesanalytics.js"></script>
</body>
</html>