
@echo off
REM ---------------------------------------------------------------------------
REM  Installation Script for running Denodo Odata4 Service
REM  as a Service in Windows NT/2000/2003/XP/Vista.
REM ---------------------------------------------------------------------------

SET DENODO_HOME=..
if "%DENODO_HOME%"=="" goto exit

SET DENODO_JRE_HOME=%DENODO_HOME%/jre
SET LIB=%DENODO_HOME%/lib

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

if exist "%JAVA_BIN%" (
    if "%1"=="start" goto :doStart
    if "%1"=="stop" goto :doStop
    if "%1"=="install" goto :doInstall
    if "%1"=="reinstall" goto :doReInstall
    if "%1"=="remove" goto :doRemove
    if "%1"=="state" goto :doInfo
    if "%1"=="none" goto :end
    
goto :usage
)
echo "Unable to execute %0: Environment variable JAVA_HOME must be set"
goto :end

:doStart
"%JAVA_BIN%" -jar "%DENODO_HOME%/lib/service-wrapper/wrapper.jar" -t "%DENODO_HOME%/conf/denodo-odata4-service-service/service.conf"
goto :end

:doStop
"%JAVA_BIN%" -jar "%DENODO_HOME%/lib/service-wrapper/wrapper.jar" -p "%DENODO_HOME%/conf/denodo-odata4-service-service/service.conf"
goto :end

:doInstall
"%JAVA_BIN%" -DverboseMode=false -classpath "%LIB%\denodo-launcher-util.jar" com.denodo.util.launcher.Launcher com.denodo.util.services.WindowsServiceChecker --lib "%LIB%\contrib" --arg "%JAVA_BIN%" --arg "%LIB%/service-wrapper/wrapper.jar" --arg "%DENODO_HOME%/conf/denodo-odata4-service-service/service.conf" --arg "%DENODO_HOME%/conf/denodo-odata4-service-service/service.conf" 
"%JAVA_BIN%" -jar "%DENODO_HOME%/lib/service-wrapper/wrapper.jar" -i "%DENODO_HOME%/conf/denodo-odata4-service-service/service.conf"
goto :end

:doReInstall
"%JAVA_BIN%" -jar "%DENODO_HOME%/lib/service-wrapper/wrapper.jar" -r "%DENODO_HOME%/conf/denodo-odata4-service-service/service.conf"
"%JAVA_BIN%" -jar "%DENODO_HOME%/lib/service-wrapper/wrapper.jar" -i "%DENODO_HOME%/conf/denodo-odata4-service-service/service.conf"
goto :end

:doRemove
"%JAVA_BIN%" -jar "%DENODO_HOME%/lib/service-wrapper/wrapper.jar" -r "%DENODO_HOME%/conf/denodo-odata4-service-service/service.conf"
goto :end

:doInfo
"%JAVA_BIN%" -jar "%DENODO_HOME%/lib/service-wrapper/wrapper.jar" -q "%DENODO_HOME%/conf/denodo-odata4-service-service/service.conf"
goto :end

:exit
 echo "Unable to execute %0: Environment variables JAVA_HOME and DENODO_HOME must be set (absolute path)"
 goto :end
 
:usage
 echo "Usage: %0 [start|stop|install|reinstall|remove|state]"

:end
