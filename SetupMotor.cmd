@echo off
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\setup_local.ps1" %*
set CODE=%ERRORLEVEL%
echo.
if not "%CODE%"=="0" echo Setup finalizado com erro. Codigo: %CODE%
if "%CODE%"=="0" echo Setup finalizado com sucesso.
pause
exit /b %CODE%
