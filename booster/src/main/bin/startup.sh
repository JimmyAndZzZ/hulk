#!/bin/bash

current_path=`pwd`
class_path=$CLASSPATH

case "`uname`" in
    Linux)
		bin_abs_path=$(readlink -f $(dirname $0))
		;;
	*)
		bin_abs_path=`cd $(dirname $0); pwd`
		;;
esac
base=${bin_abs_path}/..
export LANG=en_US.UTF-8
export BASE=$base

if [ -f $base/bin/adapter.pid ] ; then
	echo "found adapter.pid , Please run stop.sh first ,then startup.sh" 2>&2
    exit 1
fi

if [ ! -d $base/logs ] ; then
	mkdir -p $base/logs
fi

## set java path
if [ -z "$JAVA" ] ; then
  JAVA=$(which java)
fi

ALIBABA_JAVA="/usr/jimmy/java/bin/java"
TAOBAO_JAVA="/opt/jimmy/java/bin/java"
if [ -z "$JAVA" ]; then
  if [ -f $ALIBABA_JAVA ] ; then
  	JAVA=$ALIBABA_JAVA
  elif [ -f $TAOBAO_JAVA ] ; then
  	JAVA=$TAOBAO_JAVA
  else
  	echo "Cannot find a Java JDK. Please set either set JAVA or put java (>=1.5) in your PATH." 2>&2
    exit 1
  fi
fi


JAVA_OPTS="-server -Xms1g -Xmx1g"
JAVA_OPTS="$JAVA_OPTS -XX:+UseG1GC -XX:MaxGCPauseMillis=250 -XX:+UseGCOverheadLimit -XX:+ExplicitGCInvokesConcurrent -XX:+HeapDumpOnOutOfMemoryError  -XX:HeapDumpPath=dump.hprof"
JAVA_OPTS=" $JAVA_OPTS -Djava.awt.headless=true -Djava.net.preferIPv4Stack=true -Dfile.encoding=UTF-8 -Duser.timezone=GMT+08"
HULK_OPTS="-DappName=ss-booster-0.0.1-SNAPSHOT"

for i in $base/lib/*;
    do class_path=$i:"$class_path";
done

class_path="$base/conf:$class_path";

echo "cd to $bin_abs_path for workaround relative path"
cd $bin_abs_path

echo CLASSPATH :$class_path
$JAVA $JAVA_OPTS $JAVA_DEBUG_OPT $HULK_OPTS -classpath .:$class_path com.jimmy.hulk.booster.Launch 1>>/dev/null 2>&1 &
echo $! > $base/bin/hulk.pid

echo "cd to $current_path for continue"
cd $current_path





