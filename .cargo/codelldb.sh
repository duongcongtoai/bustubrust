#!/bin/sh
# compile using cargo rr
cd $2;
echo $1
traceid=$1

# find mmap file with syntax
regex="mmap_.*"
cd $2;

test_exec=$( ls | grep $regex );
echo "set target to this file $traceid/$test_exec";
rr replay $2 -d rust-gdb -s 8081
