#!/bin/bash
set -e

npm config set registry https://repo.huaweicloud.com/repository/npm/

mvn -T 2C clean install -DskipTests -Dmaven.javadoc.skip -Dfindbugs.skip -Dcheckstyle.skip -Dlicense.skip

mkdir alluxio-2.9.2

items=(assembly bin client conf integration lib libexec LICENSE webui)
for item in ${items[@]};
do
  cp -r $item alluxio-2.9.2
done

tar -czf alluxio-2.9.2-bin.tar.gz alluxio-2.9.2
