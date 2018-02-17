#!/usr/bin/ksh

# STEP-1 Run the profile file

	USR_PROF=$HOME/dbmig/accdba.profile
        . $USR_PROF > /dev/null 2>&1
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then
                echo "Profile file accdba.profile cannot be found, Exiting"
                exit 902
        fi
		
	
	in_dir=$1
	in_file=$2
	out_file=$3
	
	
	rm -f $TEMPDIR/$in_file.err
	rm -f $TEMPDIR/$in_file.delim1
	rm -f $TEMPDIR/$in_file.delim2

	
	# Identify Errors in the process
	cat $in_dir/$in_file | grep -i -w -n "failure" > $TEMPDIR/$in_file.err
	cat $in_dir/$in_file | grep -i -n "+---------+---------+---------+---------+---------+" | cut -f1 -d':'  > $TEMPDIR/$in_file.delim1
	cat $in_dir/$in_file | grep -i -n "\;" | grep -i -v '\-\-' |  cut -f1 -d':'  > $TEMPDIR/$in_file.delim2
				
	cat $TEMPDIR/$in_file.err | cut -f1 -d':' | uniq | while  read -r errline ; do
		
		startPos="0"
		endPos="0"
		
		cat $TEMPDIR/$in_file.delim1 | while read -r position; do
			if [ $position -gt $errline ]
			then
					break 
			else
					startLine=$position
			fi
		done
		
		startPos=`expr $startLine + 1`
				
		cat $TEMPDIR/$in_file.delim2 | while read -r position; do
			if [ $position -ge $startLine ]
			then
					endPos=$position
					break 
			else
					endPos=$position
			fi
		done
		#echo "$startPos $endPos"
	
		
		if [ $startPos -gt 0 ]  && [ $endPos -gt 0 ] 
		then
			if [ $startPos -eq $endPos ]
			then
				sed -n ''$startPos' p' $in_dir/$in_file  >> $out_file
			else
				sed -n ''$startPos','$endPos' p' $in_dir/$in_file  >> $out_file
			fi
			errorText=`sed -n ''$errline' p' $in_dir/$in_file`  
			echo "-- $errorText" >> $out_file
		fi
		
	done
	
	rm -f $TEMPDIR/$in_file.err
	rm -f $TEMPDIR/$in_file.delim1
	rm -f $TEMPDIR/$in_file.delim2
	
	