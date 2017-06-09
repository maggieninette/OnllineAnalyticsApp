<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<%@ page import="java.sql.Connection, ucsd.shoppingApp.ConnectionManager, ucsd.shoppingApp.*"%>
<%@ page import="ucsd.shoppingApp.models.* , java.util.*, java.text.DecimalFormat" %>    
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>

<%
DecimalFormat decimalFormat = new DecimalFormat("#,##0.00");


	List<String> rowVals = new ArrayList<>();
	List<String> colVals = new ArrayList<>();
	HashMap<String, Map<String, Double>> cellVals = new HashMap<>();
	Map<String, Double> totalSales = new HashMap<>();
	HashMap<String, Double> totalSalesPerProduct = new HashMap<>();

	if (request.getAttribute("row_values") != null || request.getAttribute("col_values") != null ||
		request.getAttribute("cell_values") != null || request.getAttribute("totalSales") != null) {

		// Get request attributes to build table.
		rowVals = (List<String>) request.getAttribute("row_values");
		colVals = (List<String>) request.getAttribute("col_values");
		cellVals = (HashMap<String, Map <String,Double>>) request.getAttribute("cell_values");
		totalSales = (Map<String, Double>) request.getAttribute("totalSales");
		totalSalesPerProduct = (HashMap<String, Double>) request.getAttribute("totalSalesPerProduct");
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
    <script src="js/jquery-3.2.1.min.js"></script>
    <script src="js/salesanalytics.js"></script>
</head>
<body>
<c:if test="${sessionScope.firstTime ne null}">
    <a id="floating-refresh-link" onclick="refreshTable()">Refresh</a>
</c:if>
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
			<input type="submit" id="reset-btn" value="Reset" name="action" />
		</form>
        <div align="center" id="new-topk-products-outer">
            <h3>No new products are included in the top 50.</h3>
            <div align="center" id="new-topk-products-inner"></div>
        </div>
		<table border="1">
		<tr>
			<td></td>
            <%
				// Setting up column headers.
				for (int i = 0; i < colVals.size(); i++) {
					String productName = colVals.get(i);
					pageContext.setAttribute("productName", productName);
					String totalSalePerProduct = decimalFormat.format(totalSalesPerProduct.get(productName));
			%>
			<td class="${productName}">
                <b><%= productName + " (" + totalSalePerProduct + ")" %></b>
            </td>
			<%
				}
			%>
		</tr>
		<tr>
			<%
                // Loop through rows.
				Map <String, Double> userSales;
				double sale = 0;
	
				for (int i = 0; i < rowVals.size(); i++) {
				    String user;
				    user = rowVals.get(i);
				    userSales = cellVals.get(user);
				    
				    
				    String total = decimalFormat.format(totalSales.get(user));
			%>
			<td><b><%= user + " (" + total + ")" %></b></td>
            <%
				    // Loop through columns.
                    for (int j = 0; j < colVals.size(); j++) {
					    String temp;
					    temp = colVals.get(j);
                        pageContext.setAttribute("productName", temp);
                        user = user.replaceAll("\\.", "");
                        user = user.replaceAll("\\s", "");
                        pageContext.setAttribute("cellId", user + temp);

                        System.out.println(pageContext.getAttribute("cellId"));
					    if (userSales.get(temp) != null) {
						    sale = userSales.get(temp);
					    }
					    String sale2 = decimalFormat.format(sale);
            %>
			<td id=${cellId} class="${productName}"><%= sale2 %></td>
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