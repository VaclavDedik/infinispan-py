#! /bin/sh

URL=$1
ZIP_NAME=$2
SERVER_DIR=$3
DIR_NAME=$4

if [ ! -d $SERVER_DIR ]; then
    mkdir $SERVER_DIR
fi

cd $SERVER_DIR
if [ ! -f $ZIP_NAME ]; then
    wget $URL
fi
if [ -f $DIR_NAME ]; then
    rm -rf $DIR_NAME
fi
unzip -q $ZIP_NAME
cd ..
