#!/bin/sh

cd /Users/yimeng/Documents/workspace-ij/chocolate-forked/marketing-tracking
rm -r chocolate-1.0-SNAPSHOT-bin
rm chocolate-1.0-SNAPSHOT-bin.gz
sh make-distribution.sh

if [ -d chocolate-1.0-SNAPSHOT-bin ];then
	tar cvf chocolate-1.0-SNAPSHOT-bin.gz chocolate-1.0-SNAPSHOT-bin/
	
	scp chocolate-1.0-SNAPSHOT-bin.gz yimeng@phxbastion100.phx.ebay.com:
fi