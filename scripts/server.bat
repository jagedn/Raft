call setpath.bat
echo argumentos
echo %1
echo %2 
echo %3
set FILE="../results/f_%1"
java -classpath %cp% -DFILE=%FILE% -Djava.rmi.server.codebase=file:../bin/ recipesService.Server %2 -phase %3



