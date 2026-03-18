@echo off
title SOC SYSTEM QUICK START
color 0A

set PROJECT_DIR=D:\Data-Science---SGU\Nam 3\HKII\Big Data\PrjBigData
cd /d "%PROJECT_DIR%"

:MENU
cls
echo ==========================================================
echo      HE THONG PHAN TICH DU LIEU LON VA BAO MAT (SOC)
echo ==========================================================
echo.
echo      [1]. CHAY HE THONG (Processor - Producer - Web)
echo      [2]. DUNG TOAN BO VA DONG CUA SO (Stop All)
echo      [3]. THOAT
echo.
echo ==========================================================
set /p choice="NHAP LENH (1/2/3): "

if "%choice%"=="1" goto START_SYS
if "%choice%"=="2" goto STOP_SYS
if "%choice%"=="3" goto EXIT_SYS
goto MENU

:START_SYS
cls
echo [1/4] DANG XOA FOLDER CU...
if exist "checkpoint" rmdir /s /q "checkpoint"
if exist "ket_qua_output" rmdir /s /q "ket_qua_output"
echo - Da don dep xong!
echo.

echo [2/4] DANG BAT PROCESSOR (TRONG DOCKER)...
start "" cmd /c "docker exec -u 0 -it prjbigdata-spark-master-1 /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /app/processor.py"
echo - Cho Spark khoi tao...
timeout /t 10 >nul

echo [3/4] DANG BAT PRODUCER (GIA LAP DATA)...
start "" cmd /c "python producer.py"
timeout /t 3 >nul

echo [4/4] DANG BAT WEB DASHBOARD (STREAMLIT)...
start "" cmd /c "streamlit run dashboard.py"
echo.

echo [OK] DA KICH HOAT 3 CUA SO!
pause
goto MENU

:STOP_SYS
cls
echo [!] DANG DUNG TIEN TRINH VA DONG CUA SO...

taskkill /F /IM python.exe /T >nul 2>&1
taskkill /F /IM streamlit.exe /T >nul 2>&1
docker exec -it prjbigdata-spark-master-1 pkill -f spark-submit >nul 2>&1
taskkill /F /IM docker.exe /T >nul 2>&1

echo [OK] DA TAT VA DONG MOI THU SACH SE!
pause
goto MENU

:EXIT_SYS
exit