@echo off
call "D:\VS\VC\Auxiliary\Build\vcvarsall.bat" x64 >nul 2>&1
cd /d D:\kbsscode\kv-storage-raft\build
"D:\VS\Common7\IDE\CommonExtensions\Microsoft\CMake\Ninja\ninja.exe" %*
