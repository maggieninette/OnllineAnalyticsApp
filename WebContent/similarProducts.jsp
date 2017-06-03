<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<%@ page import="ucsd.shoppingApp.*"%>
<%@ page import="java.util.*, java.math.BigDecimal" %>

<%
    Map<Pair,BigDecimal> sortedPairs = new HashMap<>();

    if (request.getAttribute("cosineMap") != null) {
        sortedPairs = (Map<Pair,BigDecimal>) request.getAttribute("cosineMap");
    }
%>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
    <title>Insert title here</title>
</head>
<body>
<form action="SimilarProductsController" method="post">
	<input type="submit" value="Get similar products" name="action" />
</form>
<table border="1">
    <tr>
        <td>Product 1</td>
        <td>Product 2</td>
        <td>Cosine similarity</td>
    </tr>
    <%
        int counter = 0;
        for (Map.Entry<Pair,BigDecimal> entry : sortedPairs.entrySet()) {
    %>
	<tr>
    <%
	    if (counter < 100 || counter < sortedPairs.size()) {
		    Pair tmp = entry.getKey();
		    BigDecimal cosine = entry.getValue();
		
		    String product1 = tmp.getProduct1();
		    String product2 = tmp.getProduct2();
    %>
		<td><%= product1 %></td>
		<td><%= product2 %></td>
		<td><%= cosine %></td>
    <%
        counter++;
	    }
    %>
    </tr>
    <%
        }
    %>
</table>
</body>
</html>