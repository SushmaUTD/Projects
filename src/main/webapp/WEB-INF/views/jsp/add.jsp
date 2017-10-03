<%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<!DOCTYPE html>
<html>
	<head>
			 <link href="<c:url value="/css/style.css" />" rel="stylesheet">
	</head>
	<body>
		<div id="loginForm">
			<form action="addCategory?categoryName='categoryName'&categoryLimit='categoryLimit'" method="get">
				<input type="text" id="textFields" placeholder="Category Name" name="categoryName" onfocus="this.placeholder=''"
					onblur="this.placeholder = 'Category Name'">
				<input type="text" id="textFields" placeholder="Category Limit" name="categoryLimit" onfocus="this.placeholder=''"
					onblur="this.placeholder = 'Category Limit'">
				<input type="submit" value="Save">
			</form>
		</div>
	</body>
</html>