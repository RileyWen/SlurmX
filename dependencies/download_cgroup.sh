#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
DOWNLOAD_DIR="$SCRIPT_DIR/download"
INSTALL_DIR="$SCRIPT_DIR/download/cgroup"

LIBCG_TAR='libcgroup-0.41.tar.gz'  #v0.41

sudo apt install automake m4 libtool bison flex

function check_and_download() {
  local FILE=$1
  local LINK=$2
  if [ ! -f $FILE ]; then
    wget --retry-connrefused --waitretry=1 -O $FILE $LINK
  fi
}

if [ ! -f .extracted ]; then
  check_and_download $LIBCG_TAR https://github.com/libcgroup/libcgroup/releases/download/v0.41/libcgroup-0.41.tar.gz

  tar xzf $LIBCG_TAR -C $DOWNLOAD_DIR

  touch $DOWNLOAD_DIR/.extracted
fi

#--enable-static  enables static libraries
#--disable-shared disable shared libraries to save time
pushd $DOWNLOAD_DIR/libcgroup-0.41 &&
  ./configure --prefix $INSTALL_DIR  --disable-pam  --enable-static  --disable-shared &&
  make && make install &&
  popd
