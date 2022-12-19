#!/bin/bash
set -e

mvn -T 2C clean install -DskipTests -Dmaven.javadoc.skip -Dfindbugs.skip -Dcheckstyle.skip -Dlicense.skip

mkdir alluxio-2.9.0

items=(assembly bin client conf integration lib libexec LICENSE webui)
for item in ${items[@]};
do
  cp -r $item alluxio-2.9.0
done

tar -czf alluxio-2.9.0-bin.tar.gz alluxio-2.9.0
