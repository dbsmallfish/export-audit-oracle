#!/bin/bash
current_path=`pwd`
pre=$(dirname $0)
cd $pre
base=`dirname $PWD`
cd - >/dev/null
#echo BaseDIR : $base
pidfile=$base/bin/nid
classname=com.cninfo.export.audit.Client
if [ ! -f $pidfile ];then
echo "Program is not running"
exit
fi
shpid=$(cat $pidfile)
javapid=`ps aux | grep $classname |grep -v grep| awk '{print $2}'`
#echo PID:$shpid,$javapid
kill $shpid $javapid

rm -f $pidfile