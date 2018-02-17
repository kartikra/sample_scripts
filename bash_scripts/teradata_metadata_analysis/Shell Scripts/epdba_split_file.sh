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
		
	
	pattern=$1
	in_file=$2
	no_of_aplits=$3

# STEP-2 Split the input file

	# Get the start position and number of occurences of the pattern
	cat $in_file | grep -i -w -n "$pattern" > $in_file.tmp
	
	inTotal=`cat $in_file | wc -l`
	total=`cat $in_file.tmp | wc -l`
	
	splits=`expr $total / $no_of_aplits`
	startLine="1"
	currEndLine="1"
	i="1"
	
	while [ $i -le $no_of_aplits ]
	do
		currSplit=`expr $splits \\* $i`		
		currEndLine=`sed -n ''$currSplit' p' $in_file.tmp | cut -f1 -d':'`
		endLine=`expr $currEndLine - 1`
		
		if [ $i -eq $no_of_aplits ]
		then
			endLine=`expr $inTotal`
		fi
		#echo "$startLine $endLine"
		sed -n ''$startLine','$endLine'p' $in_file > "$in_file"_$i

		i=`expr $i + 1`
		startLine=$currEndLine
	done
	
	rm -f $in_file.tmp
	
