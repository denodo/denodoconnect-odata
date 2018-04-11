@echo off
REM ---------------------------------------------------------------------------
REM  Environment variable JAVA_HOME must be set and exported
REM ---------------------------------------------------------------------------

REM ---------------------------------------------------------------------------
REM  DENODO_HOME variable must be set!
REM ---------------------------------------------------------------------------
SET DENODO_HOME=..

SET DENODO_CONF=%DENODO_HOME%\conf\vdp

CALL "%DENODO_HOME%\bin\webcontainer" stop denodo-odata2-service-${denodo.version}
