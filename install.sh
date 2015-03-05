#!/bin/env bash

mkdir -p ~/tools
cd ~/tools

wget --no-check-certificate https://codeload.github.com/xxjapp/HBShell/zip/master -O /tmp/m
unzip /tmp/m

rm -rf HBShell
mv HBShell-master HBShell

cd HBShell
chmod +x run.rb
./run.rb
