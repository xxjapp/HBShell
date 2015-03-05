#!/bin/env bash

mkdir -p ~/tools
cd ~/tools

wget -q --no-check-certificate https://codeload.github.com/xxjapp/HBShell/zip/master -O /tmp/h
unzip /tmp/h

rm -rf HBShell
mv HBShell-master HBShell

cd HBShell
chmod +x run.rb
./run.rb
