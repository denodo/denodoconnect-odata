Steps for configuring Denodo Odata Service inside Denodo internal web container, there are two options, with the option A , you can execute manually the scripts.
And with the option B, you can run the start and the stop of the application as a Service in Windows NT/2000/2003/XP/Vista.


OPTION A
=========

(1) Copy the denodo-odata4-service-6.0.war into the following location: 
     <DENODO_HOME>/resources/apache-tomcat/webapps/
     
(2) Create a context xml for the Denodo Odata Service.  You can use the attached xml template, denodo-odata4-service-6.0.xml

(3) Copy the denodo-odata4-service-6.0.xml file into the following location:
     <DENODO_HOME>/resources/apache-tomcat/conf/DenodoPlatform-6.0/localhost

(4) Check that in <DENODO_HOME>/resources/apache-tomcat/conf/catalina.properties in the property common.loader there is a reference to the vdp-jdbcdriver. In 
tomcat 6.0 the reference could be in the property shared.loader. If there isn´t this reference, you should add ${catalina.base}/../../lib/vdp-jdbcdriver-core/denodo-vdp-jdbcdriver.jar
     
(5) Create launch scripts for the Denodo Odata Service(odata4_service_startup and odata4_service_shutdown). You can use the attached templates.
     Make sure to modify the DENODO_HOME variable in the script templates to point to your Denodo installation.

(6) Copy the launch scripts into <DENODO_HOME>/bin




After copying all the necessary files into the correct directories, run the odata4_service_startup.bat (or odata4_service_startup-sh)  launch script. 
Then navigate to http://localhost:9090/denodo-odata4-service-6.0/denodo-odata.svc/<DBNAME>.  

You can stop the service executing odata4_service_shutdown and restarting executing again the script odata4-service-startup

OPTION B
========
This option is only for Windows Systems.

(1-4) Same as in the option A

(5)   Create service script for the Denodo Odata Service(odata4_service_service). You can use the attached template.
      Make sure to modify the DENODO_HOME variable in the script templates to point to your Denodo installation.

(6)   Create the file service.conf and copy in the folder <DENODO_HOME>/conf/denodo_odata4-service-service/, you can use the attached template.
Make sure to modify the DENODO_HOME variable in the script templates to point to your Denodo installation. 

(7)   Install as a Windows service. Execute "odata4-service-service.bat install"

(8)   You can manage the installed service from the application of Windows: Services. 




