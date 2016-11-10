#! /bin/sh

URL=$1
ZIP_NAME=$2
DIR=$3

if [ ! -d $DIR ]; then
   mkdir $DIR
fi

cd $DIR
wget $URL
cd ..
