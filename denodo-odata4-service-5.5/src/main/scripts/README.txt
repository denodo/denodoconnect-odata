Steps for configuring Denodo Odata Service inside Denodo internal web container:

(1) Copy the denodo-odata4-service-5.5.war into the following location: 
     <DENODOHOME>/resources/apache-tomcat/webapps/
     
(2) Create a context xml for the Denodo Odata Service.  You can use the attached xml template, denodo-odata4-service-5.5.xml

(3) Copy the denodo-odata4-service-5.5.xml file into the following location:
     <DENODOHOME>/resources/apache-tomcat/conf/DenodoPlatform-5.5/localhost
     
(4) Create launch scripts for the Denodo Odata Service.  You can use the attached templates. 
     Make sure to modify the DENODO_HOME variable in the script templates to point to your Denodo installation.

(5) Copy the launch scripts into <DENODOHOME>/bin

(6) Check that in <DENODOHOME>/resources/apache-tomcat/conf/catalina.properties in the property common.loader there is a reference to the vdp-jdbcdriver. In 
tomcat 5.5 the reference could be in the property shared.loader. If there isn´t this reference, you should add ${catalina.base}/../../lib/vdp-jdbcdriver-core/denodo-vdp-jdbcdriver.jar


After copying all the necessary files into the correct directories, run the odata4_service_startup.bat (or odata4_service_startup-sh)  launch script. 
Then navigate to http://localhost:9090/denodo-odata4-service-5.5/denodo-odata.svc/<DBNAME>.  

You can stop the service executing odata4_service_shutdown and restarting executing again the script odata4-service-startup 