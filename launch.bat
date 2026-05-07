@echo off
title Weather Data Explorer
:: ─────────────────────────────────────────────────────────────────────────────
::  Weather Data Explorer — Windows Launcher
::  Double-click this file to install packages and start the app.
::  First run installs packages; subsequent runs skip that step (a few seconds).
:: ─────────────────────────────────────────────────────────────────────────────

:: Move to the project root (directory containing this file)
cd /d "%~dp0"

echo ==================================================
echo   Weather Data Explorer
echo ==================================================
echo.

:: ── 1. Find Python ────────────────────────────────────────────────────────────
set PYTHON=

:: Check common Miniconda / Anaconda install locations
if exist "%USERPROFILE%\miniconda3\python.exe"          set PYTHON=%USERPROFILE%\miniconda3\python.exe
if exist "%USERPROFILE%\anaconda3\python.exe"           set PYTHON=%USERPROFILE%\anaconda3\python.exe
if exist "%LOCALAPPDATA%\miniconda3\python.exe"         set PYTHON=%LOCALAPPDATA%\miniconda3\python.exe
if exist "%LOCALAPPDATA%\anaconda3\python.exe"          set PYTHON=%LOCALAPPDATA%\anaconda3\python.exe
if exist "%LOCALAPPDATA%\Programs\Python\Python312\python.exe" set PYTHON=%LOCALAPPDATA%\Programs\Python\Python312\python.exe
if exist "%LOCALAPPDATA%\Programs\Python\Python311\python.exe" set PYTHON=%LOCALAPPDATA%\Programs\Python\Python311\python.exe
if exist "%LOCALAPPDATA%\Programs\Python\Python310\python.exe" set PYTHON=%LOCALAPPDATA%\Programs\Python\Python310\python.exe

:: Fall back to python on system PATH
if "%PYTHON%"=="" (
    where python >nul 2>&1
    if not errorlevel 1 set PYTHON=python
)

if "%PYTHON%"=="" (
    echo.
    echo  ERROR: Python not found.
    echo.
    echo  Please install Miniconda from:
    echo    https://docs.conda.io/en/latest/miniconda.html
    echo  or Python 3.10+ from:
    echo    https://www.python.org/downloads/
    echo.
    echo  After installing, run this file again.
    echo.
    pause
    exit /b 1
)

echo Python : %PYTHON%
echo.

:: ── 2. Install / update packages ─────────────────────────────────────────────
echo Checking required packages...
"%PYTHON%" -m pip install --quiet --upgrade pip
"%PYTHON%" -m pip install --quiet -r scripts\requirements.txt

if errorlevel 1 (
    echo.
    echo  ERROR: Package installation failed.
    echo  Check your internet connection and try again.
    echo.
    pause
    exit /b 1
)

echo All packages ready.
echo.

:: ── 3. Launch Streamlit ───────────────────────────────────────────────────────
echo Starting app at ^> http://localhost:8501
echo Close this window to stop the server.
echo.

"%PYTHON%" -m streamlit run scripts\app.py

pause
