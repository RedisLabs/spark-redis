#!/usr/bin/env bash
#
# Switches the build between Scala 2.12 and 2.13, like Spark's dev/change-scala-version.sh:
# it sets the artifact name (spark-redis_<scala version>) and the Scala version properties in pom.xml.
#
#   dev/change-scala-version.sh 2.13
#
# The Scala patch versions themselves are the scala.2.12.version / scala.2.13.version properties in pom.xml.

set -euo pipefail

usage() {
  echo "Usage: $(basename "$0") <2.12|2.13>" >&2
  exit 1
}

[ $# -eq 1 ] || usage
TO_VERSION=$1
case "$TO_VERSION" in
  2.12|2.13) ;;
  *) usage ;;
esac

POM="$(cd "$(dirname "$0")/.." && pwd)/pom.xml"

# write to a temporary file: 'sed -i' takes different arguments on Linux and macOS
replace() {
  sed -E "$1" "$POM" > "$POM.tmp"
  mv "$POM.tmp" "$POM"
}

replace 's|<artifactId>spark-redis_2\.1[23]</artifactId>|<artifactId>spark-redis_'"$TO_VERSION"'</artifactId>|'
replace 's|<scala\.major\.version>2\.1[23]</scala\.major\.version>|<scala.major.version>'"$TO_VERSION"'</scala.major.version>|'
replace 's|<scala\.complete\.version>\$\{scala\.2\.1[23]\.version\}</scala\.complete\.version>|<scala.complete.version>${scala.'"$TO_VERSION"'.version}</scala.complete.version>|'

# fail loudly if pom.xml no longer has the expected lines
for expected in \
    "<artifactId>spark-redis_$TO_VERSION</artifactId>" \
    "<scala.major.version>$TO_VERSION</scala.major.version>" \
    "<scala.complete.version>\${scala.$TO_VERSION.version}</scala.complete.version>"; do
  if ! grep -qF "$expected" "$POM"; then
    echo "pom.xml was not updated as expected, missing: $expected" >&2
    exit 1
  fi
done

echo "pom.xml switched to Scala $TO_VERSION"
