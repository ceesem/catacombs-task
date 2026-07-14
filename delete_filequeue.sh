#!/bin/sh
filepath=$(echo $TASKQUEUE_PATH | sed 's|^fq://||')
rm -r $filepath