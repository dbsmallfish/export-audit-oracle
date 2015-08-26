#!/bin/bash
timeInterval=2   #MINUTES
current_path=`pwd`
pre=$(dirname $0)
cd $pre
base=`dirname $PWD`
cd - >/dev/null
#echo BaseDIR : $base
pidfile=$base/bin/nid

if [ -f $pidfile ];then
   echo "Program is running,You should exec shutdown.sh first"
   exit
fi


JAVA_OPTS="-server -Xms32m -Xmx128m -XX:MaxPermSize=64m "
## set java path
if [ -z "$JAVA" ] ; then
  JAVA=$(which java)
fi
if [ -z "$JAVA" ]; then
echo "Cannot find a Java JDK. Please set either set JAVA or put java (>=1.5) in your PATH." 2>&2
exit 1
fi
for i in $base/lib/*;do
CLASSPATH=$i:"$CLASSPATH";
done
CLASSPATH=$CLASSPATH:$base/conf
$JAVA $JAVA_OPTS -Dbase.home=$base -DtimeInterval=$timeInterval -classpath .:$CLASSPATH com.cninfo.export.audit.Client &

echo $! >$pidfile