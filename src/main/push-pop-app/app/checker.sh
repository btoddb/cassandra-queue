pushd target
sort -u popper.0* > popper.merged
sort -u pusher.0* > pusher.merged
wc -l pusher.*
wc -l popper.*
popd
