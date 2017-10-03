<%@ page language="java" pageEncoding="UTF-8" contentType="text/html;charset=UTF-8" language="java" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>
<%@ taglib uri="http://www.springframework.org/tags" prefix="spring" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/functions" prefix="fn" %>


<!DOCTYPE html>
<html>
<head>
	 <link href="<c:url value="/css/getItResultStyle.css" />" rel="stylesheet">
	 <link href="<c:url value="/css/getItStyle.css" />" rel="stylesheet">
</head>
<body>
	<h4 align="center">Welcome Sushma</h4> <br/> <br/>
        <c:forEach items="${result}" var="deal" >
         <input type="text" id="textFields2" readOnly="readOnly" value="${deal.categoryName}"> 
         <input type="text" id="textFields3" readOnly="readOnly" value="${deal.categoryLimit}"> 
		 <br/>
        </c:forEach>
        &nbsp;
        &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;
        <form action="newCategory" method="GET"> 
       	 <input type="submit" id="button" value="Add Category">
        </form>
</body>
</html>