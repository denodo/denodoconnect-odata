
 Configuring Denodo Odata Service inside Denodo internal web container
======================================================================

 There are two options for configuring Denodo Odata Service inside Denodo internal web container.
 - With the option A, you can execute manually the scripts.
 - With the option B, you can run the start and the stop of the application as a Service in
   Windows NT/2000/2003/XP/Vista.


OPTION A
=========

(1) Copy the denodo-odata2-service-${denodo.version}.war into the following location: 
     <DENODO_HOME>/resources/apache-tomcat/webapps/
     
(2) Create a context xml for the Denodo Odata Service. You can use the attached xml template,
    denodo-odata2-service-${denodo.version}.xml

(3) Copy the denodo-odata2-service-${denodo.version}.xml file into the following location:
     <DENODO_HOME>/resources/apache-tomcat/conf/DenodoPlatform-${denodo.version}/localhost
 
(4) Check that in <DENODO_HOME>/resources/apache-tomcat/conf/catalina.properties in the property
    common.loader there is a reference to the vdp-jdbcdriver. In older tomcats the reference could be
    in the property shared.loader. If there is no reference, you should add it:
    ${denodo.driver.path}

(5) Create launch scripts for the Denodo Odata Service(odata2_service_startup and odata2_service_shutdown).
    You can use the attached templates. Make sure to modify the DENODO_HOME variable in the script templates
    to point to your Denodo installation.

(6) Copy the launch scripts into <DENODO_HOME>/bin


After copying all the necessary files into the correct directories, run the odata2_service_startup.bat
(or odata2_service_startup-sh)  launch script. 

Then navigate to http://localhost:9090/denodo-odata2-service-${denodo.version}/denodo-odata.svc/<DBNAME>.  

You can stop the service executing odata2_service_shutdown and restarting executing again the script odata2-service-startup


OPTION B
========
This option is only for Windows Systems.

(1-4) Same as in the option A

(5)   Create service script for the Denodo Odata Service(odata2_service_service) in <DENODO_HOME>/bin.
      You can use the attached template. Make sure to modify the DENODO_HOME variable in the script
      templates to point to your Denodo installation.

(6)   Create the file service.conf and copy in the folder <DENODO_HOME>/conf/denodo-odata2-service-service/,
      you can use the attached template. Make sure to modify the DENODO_HOME variable in the script templates
      to point to your Denodo installation. 

(7)   Install as a Windows service. Execute "odata2-service-service.bat install"

(8)   You can manage the installed service from the application of Windows: Services. 
