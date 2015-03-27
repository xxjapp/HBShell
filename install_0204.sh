#!/bin/env bash

mkdir -p ~/tools
cd ~/tools

wget -q --no-check-certificate https://codeload.github.com/xxjapp/HBShell/zip/e26c46ac569696c75c0d16add28d0f77684eac25 -O /tmp/h
unzip -q /tmp/h

rm -rf HBShell
mv HBShell-master HBShell

cd HBShell
chmod +x run.rb
./run.rb
