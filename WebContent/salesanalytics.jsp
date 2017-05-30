<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<%@page import="java.sql.Connection, ucsd.shoppingApp.ConnectionManager, ucsd.shoppingApp.*"%>
<%@ page import="ucsd.shoppingApp.models.* , java.util.*" %>    
<%@ taglib prefix = "c" uri = "http://java.sun.com/jsp/jstl/core" %>
    
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>Sales Analytics</title>

<script src="javascript/jquery-3.2.1.min.js"></script>
<script src="javascript/basics.js"></script>

	<%
	

	boolean b;
	boolean firsttime = (boolean) session.getAttribute("firsttime");
	//session.setAttribute("show20",false);
	
	System.out.println("counter: "+Integer.toString((Integer)session.getAttribute("counter")));
	
	
	List<String> row_vals = new ArrayList<>();
	List<String> col_vals = new ArrayList<>();
	HashMap<String, Map <String,Integer>> cell_vals = new HashMap<>();	
	Map<String, Integer> totalSales = new HashMap<>();
	
	if(request.getAttribute("row_values") != null &&
		request.getAttribute("col_values") != null &&
		request.getAttribute("cell_values") != null &&
		request.getAttribute("totalSales") != null){
		
		System.out.println("setting attributes");
		row_vals = (List<String>) request.getAttribute("row_values"); 
		col_vals = (List<String>) request.getAttribute("col_values");
		cell_vals = (HashMap<String, Map <String,Integer>>) request.getAttribute("cell_values");
		totalSales = (Map<String, Integer>) request.getAttribute("totalSales");

		

		//pageContext.setAttribute("sales_per_customer",cell_vals);
		
		
		//start setting up the table. get the column values
	}
	
		%>

</head>
<body>

<a href="./home.jsp">Go back home</a>

<%

Connection con = ConnectionManager.getConnection();	

CategoryDAO categoryDao = new CategoryDAO(con);
List<CategoryModel> category_list = categoryDao.getCategories();
con.close();

%>


<form name="analyzeSales" method="POST" action="SalesAnalyticsController">

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
	<select name= "filter">	
	<option value="all_products">all products</option>
	<%
		for (CategoryModel cat : category_list) {
	%>
		<option value="<%=cat.getCategoryName()%>"> "<%=cat.getCategoryName() %>"</option>
	<%
		}
	%>
	</select>	
	</td>
	</tr>

	</table>
	<input type="submit" value="analyze" name="action" />

	
	<input type="submit" value="next 20" name="action" />
	<input type="submit" value="reset" name="action" />
	<input type="submit" value="next 10 products" name="action" />
</form>



<form action="SalesAnalyticsController" method="post">


	
<br>


<table border="1">

 <%
 /* DON'T DELETE
	<tr>
		<td>XXX</td>
	<c:forEach items="${products}" var="current">
		<td> <c:out value="${current}"/> </td>
	</c:forEach>	
	<tr>
	<c:forEach items="${customers}" var="current">
		<tr> 
		<td> <c:out value="${current}"/> </td>		
		</tr>
	</c:forEach>
	*/
	%>
	
	<tr>
	<td> </td>
	
<%
	//setting up the column headers
	
	for (int i = 0; i < col_vals.size(); i++){
		%>
		<td><b><%=col_vals.get(i) %></td>
		<%	
		
	}

%>	
	</tr>
	<%
	//looping through the rows
	Map <String,Integer> userSales;

	int sale = 0;
	
	for (int i = 0; i < row_vals.size(); i++){

		%>
		<tr>
		<% 
		
		String user;
		user = row_vals.get(i);
		
		
		userSales = cell_vals.get(user);
			//display the user name/state (row value) and total purchases made

		
			
		%>
		<td><b> <%=user+" ("+Integer.toString(totalSales.get(user))+")" %></b></td>
		<% 

		
		//loop through col vals to output cell values in correct order
		for (int j = 0; j < col_vals.size(); j++){
			String temp;
			temp = col_vals.get(j);
			if (userSales.get(temp) != null){
			sale = userSales.get(temp);
			}
			%>
			<td><%=sale %></td>
			<% 
		}
	
		%>
		</tr>
		<% 
	}
	
	
	%>
	
	

</table>

</form>
	
	
	

</body>
</html>


