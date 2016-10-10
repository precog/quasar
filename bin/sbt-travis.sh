#!/usr/bin/env bash
#
# Post-processing sbt output to remove noise.
# We get it, you do a lot of Resolving.

set -euo pipefail

unset SBT_OPTS JVM_OPTS JDK_HOME JAVA_HOME
: ${TRAVIS_SCALA_VERSION:=2.11.8}
: ${SBT:=./sbt}
[[ $# -eq 0 ]] && set -- test

# Filter out any lines which begin with one of these strings
# (possibly after an [info] marker)
readarray -t ignore_words <<-"EOT"
Resolving
Loading
Attempting
Formatting
Reapplying
downloading
\[SUCCESSFUL\s*\]
Passed: Total 0,
0 example,
ScalaCheck
not simplified
not implemented in aggregation
Server started listening
Stopped last server
TODO
log4j:
EOT

ignore_start='(\[info\][[:space:]]*)?'
ignore_rest="$(printf "%s\n" "${ignore_words[@]}" | paste -sd'|' -)"
ignore_regex="${ignore_start}${ignore_rest}"
pass_regex="(^${ignore_start}Passed:|, 0 failure, 0 error)"
fail_regex="(?:[1-9][0-9]|[1-9]) (?:failure|error)"
test_fail_regex='^\[error\]'

RESET="\e[0m"
RED="\e[31m"
GREEN="\e[32m"
YELLOW="\e[33m"
BG_RED="\e[41m"
BG_GREEN="\e[42m"

handle_line () {
  while read -r line; do
    if [[ "$line" =~ $ignore_regex ]]; then
      true
    elif [[ "$line" =~ $test_fail_regex ]]; then
      echo -ne "$RED"
      printf "%s" "$line"
      echo -e "$RESET"
    elif [[ "$line" =~ $fail_regex ]]; then
      echo -ne "$BG_RED"
      printf "%s" "$line"
      echo -e "$RESET"
    elif [[ "$line" =~ $pass_regex ]]; then
      echo -ne "$BG_GREEN"
      printf "%s" "$line"
      echo -e "$RESET"
    else
      printf "%s\n" "$line"
    fi
  done

  echo ""
}

"$SBT" ++$TRAVIS_SCALA_VERSION -no-colors -v "$@" | stdbuf -oL uniq | stdbuf -oL tee sbt.log | handle_line
