cd ..\bin
start rmiregistry

cd ..\scripts

set n=3
set fase=2

start cmd /k testserver.bat %n% %fase%

ping 1.1.1.1 -n 2

start cmd /c config.bat %n% %fase%

ping 1.1.1.1 -n 2

for /L %%i in (1,1,%n%) do (
	start cmd /k server.bat %%i %n% %fase%
)
