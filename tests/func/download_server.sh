#! /bin/sh

URL=$1
ZIP_NAME=$2

if [ ! -d "server" ]; then
   mkdir "server"
fi

cd server
wget $URL
cd ..
