@echo off
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\stop.ps1" %*
set CODE=%ERRORLEVEL%
echo.
if not "%CODE%"=="0" echo Parada finalizada com erro. Codigo: %CODE%
if "%CODE%"=="0" echo Containers parados.
pause
exit /b %CODE%
