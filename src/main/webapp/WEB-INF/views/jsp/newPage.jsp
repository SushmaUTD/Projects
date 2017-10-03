<%@ page language="java" pageEncoding="UTF-8" contentType="text/html;charset=UTF-8" language="java" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>
<%@ taglib uri="http://www.springframework.org/tags" prefix="spring" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/functions" prefix="fn" %>
<!DOCTYPE html>
<html>
	<head>
		<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css">
    	<script data-require="bootstrap@*" data-semver="3.3.7" src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/js/bootstrap.min.js"></script>
    	<link href="<c:url value="/css/style.css" />" rel="stylesheet">
    	<meta charset="utf-8">
  		<meta name="viewport" content="width=device-width, initial-scale=1">
  		<script src="https://ajax.googleapis.com/ajax/libs/jquery/1.12.4/jquery.min.js"></script>
	</head>
	<body>
		<nav class="navbar navbar-inverse" data-spy="affix" data-offset-top="0">
			<ul class="nav navbar-nav">
				<li><a href="#" style="width:40px; height:40px;"><img src="<c:url value="/images/dollar.png" />" style="width:35px; height:35px;margin-top:-3px;" href="#"></a></li>
				<li><a href="#" style="width:40px; height:40px;">
				<form action="newCategory" method="GET"> 
       					<input type="image" src="<c:url value="/images/add.png" />" alt="submit" style="width:35px; height:35px;margin-top:-3px;margin-left:1340px;">
        			</form></a>
        		</li>
			</ul>
		</nav>
		<div>
			<c:forEach items="${result}" var="deal" >
					<div style="width:100%;" class="row">
						<div id="nameBox" align="center">
						<input value="${deal.categoryName}" readOnly="readOnly" style="color:#5C5B5B;margin-top:35px; margin-left:4px;background-color:#DBDBDB; border-width:0px;"> 	
						</div>
        				 <div id="limitBox" align="center">
        	 			<input readOnly="readOnly" value="${deal.categoryLimit}" style="color:#5C5B5B;margin-top:35px; margin-left:4px;background-color:#DBDBDB; border-width:0px; width:30px;"> 
        				</div>
					</div>
				</div>
        	<br>
       		</c:forEach>
		</div>
	</body>
</html>