app/sorter.sh target/popper.0* > popper.merged_dupes
awk -F, '{if ( $2 == last ) { print $0 } ; last = $2}' popper.merged_dupes

