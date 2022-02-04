#!/bin/sh
allarg="$*"
echo "cargo rr test $allarg";
cargo rr test $allarg
