<%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<!DOCTYPE html>
<html>
	<head>
   		 <link href="<c:url value="/css/style.css" />" rel="stylesheet">
	</head>
	<body>
		<div id="loginForm">
			<form action="homePage?username='username'&password='password'" method="GET">
				<input type="text" id="textFields" name="username" placeholder="Username" onfocus="this.placeholder=''"
				onblur="this.placeholder = 'Username'">
				<input type="password" id="textFields" name="password" placeholder="Password" onfocus="this.placeholder=''"
				onblur="this.placeholder = 'Password'">
				<input type="submit" id="button" value="Login">
			</form>
		</div>
	</body>
</html>