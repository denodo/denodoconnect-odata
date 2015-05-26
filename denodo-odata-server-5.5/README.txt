This application is a web server ODATA that uses Apache Olingo http://olingo.apache.org/ .
The project was initialized with the following archetype: 

mvn archetype:generate \
  -DinteractiveMode=false \
  -Dversion=1.0.0-SNAPSHOT \
  -DgroupId=com.sample \
  -DartifactId=my-car-service \
  -DarchetypeGroupId=org.apache.olingo \
  -DarchetypeArtifactId=olingo-odata2-sample-cars-service-archetype \
  -DarchetypeVersion=RELEASE
 
http://olingo.apache.org/doc/odata2/sample-setup.html
 
  
It is a very simple sample car service that can work as a starting point for implementing a custom OData service.
This service consists of a very simple EDM with two entity sets that are cars and manufactures 
and a memory based data provider that is a simple hash map. 
Therefore the project implements a very basic single OData processor supporting a minimal readonly scenario.

You can deploy the project using  directly in the tomcat mvn clean install tomcat7:run

Or you can use mvn clean install and  Maven will build the project with the result car-service.war 
in the Maven target directory which can be deployed to any JEE compliant web application server.

You can modify the project in Eclipse using mvn eclipse:clean eclipse:eclipse and importing the project to eclipse, 
it should be recognized as a web application project. Deploy the Eclipse project to a server and it should run as well.

You can access to the service in the url http://localhost:8080/my-car-service/

And you can access to the service using the platform denodo using the denodo odata custom wrapper.
