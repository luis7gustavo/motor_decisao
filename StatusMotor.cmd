@echo off
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\status.ps1" %*
set CODE=%ERRORLEVEL%
echo.
if not "%CODE%"=="0" echo Status finalizado com erro. Codigo: %CODE%
pause
exit /b %CODE%
