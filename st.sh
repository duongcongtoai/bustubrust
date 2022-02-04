echo $1
regex="\./.*$1$"
st=$( find . -maxdepth 1 -regex $regex );
echo "\./.*$1$"
echo $st;
