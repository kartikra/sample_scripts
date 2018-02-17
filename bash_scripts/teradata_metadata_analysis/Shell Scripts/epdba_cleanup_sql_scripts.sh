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




# STEP-2 Cleanup the script file


	var_path=$1
	var_fileName=$2
	option=$3


	edit_file()
	{
			
		# Remove the --------- before the VIEW and TABLE defintions
		cat $TEMPDIR/$var_fileName | grep -n '\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-' | cut -f1 -d':'  > "$var_fileName"_rownum.dat
		
		rm -f $TEMPDIR/"$var_fileName"_final
		lineNo="0"
		cat $TEMPDIR/$var_fileName | while read -r origLine ; do
			lineNo=`expr $lineNo + 1`
			foundInd=`grep -w "$lineNo" "$var_fileName"_rownum.dat | wc -l`
			if [ "$foundInd" -eq 0 ]
			then
				echo "$origLine" >> $TEMPDIR/"$var_fileName"_final
			fi
		done
	
	
		if [ -s $TEMPDIR/"$var_fileName"_final ] 
		then
			mv $TEMPDIR/"$var_fileName"_final $TEMPDIR/$var_fileName
		fi
		
		
	
		# Remove the old CRQ and WO numbers
		cd $TEMPDIR

		if [ -z "$option" ]
		then

			cat $var_fileName | grep "\/\*" | grep -v '\-\-' | egrep -i "CRQ000|CHG000|WO000" | cut -f2 -d'/' | sed 's/\*\*//g' | sort | uniq  > "$var_fileName"_crqno.dat

			rm -f "$var_fileName"_crqno.tmp 
			rm -f "$var_fileName"_processed.dat
			
			
			# Remove Any String thats greater than 20 charcters
			cat "$var_fileName"_crqno.dat | while read -r line ; do
				wl=`expr length "$line"`
				if [ $wl -lt 20 ]
				then
					echo $line >> "$var_fileName"_crqno.tmp
				fi
			done
			if [ -f "$var_fileName"_crqno.tmp ]
			then
				mv "$var_fileName"_crqno.tmp "$var_fileName"_crqno.dat
			fi

			
			while [ -s "$var_fileName"_crqno.dat ]
			do	
			
				# Replace the contents of each line
				cat "$var_fileName"_crqno.dat | sort | uniq | while read -r line2 ; do

					line2=`echo $line2 | sed 's/\ //g'`
					sed -e 's/\/\*\ /\/\*/g' -e 's/\ \*\//\*\//g' -e 's/'$line2'//g' -e 's/\/\*\///g' $var_fileName > "$var_fileName".tmp
					mv "$var_fileName".tmp $var_fileName
					echo "$line2" >> "$var_fileName"_processed.dat
				done


				# Remove line for future processing if it has been processed already
				cat $var_fileName | grep "\/\*" | grep -v '\-\-' | egrep -i "CRQ000|CHG000|WO000" | cut -f2 -d'/' | sed 's/\*\*//g' | sort | uniq > "$var_fileName"_crqno.dat
				rm -f "$var_fileName"_crqno.tmp 
				cat "$var_fileName"_crqno.dat | while read -r line ; do
					wl=`expr length "$line"`
					line2=`echo $line | sed 's/\ //g'`
					processedCount=`grep -i "$line2" "$var_fileName"_processed.dat | wc -l`
					if [ $wl -lt 20 ] && [ $processedCount -eq 0 ]
					then
						echo $line >> "$var_fileName"_crqno.tmp
					fi
				done
				if [ -f "$var_fileName"_crqno.tmp ]
				then
					mv "$var_fileName"_crqno.tmp "$var_fileName"_crqno.dat
				fi
								
			done

			rm -f "$var_fileName"_crqno.dat
		fi

		
		rm -f "$var_fileName"_rownum.dat
		cat $var_fileName >> "$var_fileName"_final

	}
	
	
	# Main Function
	rm -f $TEMPDIR/"$var_fileName"_final
	
	cp "$var_path"/"$var_fileName" $TEMPDIR/"$var_fileName"
	edit_file
	
	
	if [ -s $TEMPDIR/"$var_fileName"_final ] 
	then
		mv $TEMPDIR/"$var_fileName"_final $var_path/$var_fileName
		rm -f $TEMPDIR/$var_fileName
	fi
	
	
	


