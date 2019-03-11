@echo off
REM ---------------------------------------------------------------------------
REM  Environment variable JAVA_HOME must be set and exported
REM ---------------------------------------------------------------------------

REM ---------------------------------------------------------------------------
REM  DENODO_HOME variable must be set!
REM ---------------------------------------------------------------------------
SET DENODO_HOME=..

SET DENODO_LAUNCHER_CLASSPATH=%DENODO_HOME%\lib\${denodo.launcher.jar}
if  "%DENODO_EXTERNAL_CLASSPATH%" == "" goto configuredlauncherclasspath
SET DENODO_LAUNCHER_CLASSPATH=%DENODO_LAUNCHER_CLASSPATH%;%DENODO_EXTERNAL_CLASSPATH%
:configuredlauncherclasspath


SET DENODO_JRE_HOME=%DENODO_HOME%\jre
SET DENODO_JAVA_HOME=$DENODO_JAVA_HOME


SET JAVA_BIN=%DENODO_JRE_HOME%\bin\javaw.exe
if exist "%JAVA_BIN%" goto configuredjavabin

SET JAVA_BIN=%DENODO_JAVA_HOME%\jre\bin\java.exe
if exist "%JAVA_BIN%" goto configuredjavabin

SET JAVA_BIN=%DENODO_JAVA_HOME%\bin\java.exe
if exist "%JAVA_BIN%" goto configuredjavabin

SET JAVA_BIN=%DENODO_JRE_HOME%\bin\java.exe
if exist "%JAVA_BIN%" goto configuredjavabin

SET JAVA_BIN=%JAVA_HOME%\jre\bin\java.exe
if exist "%JAVA_BIN%" goto configuredjavabin

SET JAVA_BIN=%JAVA_HOME%\bin\java.exe

:configuredjavabin

if not exist "%JAVA_BIN%" goto notconfigured 

   "%JAVA_BIN%" -DverboseMode=false -classpath "%DENODO_LAUNCHER_CLASSPATH%" com.denodo.util.launcher.Launcher com.denodo.tomcat.TomcatBootstrap --conf "%DENODO_CONF%" --conf "%DENODO_HOME%\resources\apache-tomcat\conf"   ^
	--lib "%DENODO_HOME%\lib\contrib" --arg start --arg denodo-odata2-service-${denodo.version}
    goto :end


:notconfigured
echo "Unable to execute %0: Unable to locate java jre location"
:end



