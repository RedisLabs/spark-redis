#!/usr/bin/env bash

set -e

VALID_VERSIONS=( 2.11 2.12 )

SCALA_211_MINOR_VERSION="12"
SCALA_212_MINOR_VERSION="9"

usage() {
  echo "Usage: $(basename $0) [-h|--help] <version>
where :
  -h| --help Display this help text
  valid version values : ${VALID_VERSIONS[*]}
" 1>&2
  exit 1
}

if [[ ($# -ne 1) || ( $1 == "--help") ||  $1 == "-h" ]]; then
  usage
fi

TO_MAJOR_VERSION=$1

check_scala_version() {
  for i in ${VALID_VERSIONS[*]}; do [ $i = "$1" ] && return 0; done
  echo "Invalid Scala version: $1. Valid versions: ${VALID_VERSIONS[*]}" 1>&2
  exit 1
}

check_scala_version "$TO_MAJOR_VERSION"

if [ $TO_MAJOR_VERSION = "2.12" ]; then
  FROM_MAJOR_VERSION="2.11"
  FROM_MINOR_VERSION=$SCALA_211_MINOR_VERSION
  TO_MINOR_VERSION=$SCALA_212_MINOR_VERSION
else
  FROM_MAJOR_VERSION="2.12"
  FROM_MINOR_VERSION=$SCALA_212_MINOR_VERSION
  TO_MINOR_VERSION=$SCALA_211_MINOR_VERSION
fi

sed_i() {
  sed -e "$1" "$2" > "$2.tmp" && mv "$2.tmp" "$2"
}

export -f sed_i

# change <artifactId>
BASEDIR=$(dirname $0)/..
find "$BASEDIR" -name 'pom.xml' -not -path '*target*' -print \
  -exec bash -c "sed_i 's/\(artifactId.*\)_'$FROM_MAJOR_VERSION'/\1_'$TO_MAJOR_VERSION'/g' {}" \;

# change <scala.major.version>
find "$BASEDIR" -name 'pom.xml' -not -path '*target*' -print \
  -exec bash -c "sed_i 's/\(<scala.major.version>\)'$FROM_MAJOR_VERSION'/\1'$TO_MAJOR_VERSION'/g' {}" \;

# change <scala.complete.version>
find "$BASEDIR" -name 'pom.xml' -not -path '*target*' -print \
  -exec bash -c "sed_i 's/\(<scala.complete.version>.*\.\)'$FROM_MINOR_VERSION'/\1'$TO_MINOR_VERSION'/g' {}" \;

