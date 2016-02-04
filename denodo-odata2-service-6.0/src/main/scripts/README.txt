Steps for configuring Denodo Odata Service inside Denodo internal web container:

(1) Copy the denodo-odata2-service-6.0.war into the following location: 
     <DENODOHOME>/resources/apache-tomcat/webapps/
     
(2) Create a context xml for the Denodo Odata Service.  You can use the attached xml template, denodo-odata2-service-6.0.xml

(3) Copy the denodo-odata2-service-6.0.xml file into the following location:
     <DENODOHOME>/resources/apache-tomcat/conf/DenodoPlatform-6.0/localhost
     
(4) Create launch scripts for the Denodo Odata Service.  You can use the attached templates. 
     Make sure to modify the DENODO_HOME variable in the script templates to point to your Denodo installation.

(5) Copy the launch scripts into <DENODOHOME>/bin

(6) Check that in <DENODOHOME>/resources/apache-tomcat/conf/catalina.properties in the property common.loader there is a reference to the vdp-jdbcdriver. In 
tomcat 5.5 the reference could be in the property shared.loader. If there isn´t this reference, you should add ${catalina.base}/../../tools/client-drivers/jdbc/denodo-vdp-jdbcdriver.jar


After copying all the necessary files into the correct directories, run the odata_service_startup.bat (or odata_service_startup-sh)  launch script. 
Then navigate to http://localhost:9090/denodo-odata2-service-6.0/denodo-odata.svc/<DBNAME>.  

You can stop the service executing odata_service_shutdown and restarting executing again the script odata-service-startup 