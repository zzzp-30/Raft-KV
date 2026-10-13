@echo off
call "D:\VS\VC\Auxiliary\Build\vcvarsall.bat" x64 >nul 2>&1
cd /d D:\kbsscode\kv-storage-raft
echo === Building project ===
cmake --build build 2>&1
if errorlevel 1 (
    echo BUILD FAILED
    exit /b 1
)
echo.
echo === Running tests ===
bin\raft_test.exe 2>&1
echo.
echo === Build and test complete ===
