#!/bin/bash
for i in {1..10}
do
	for j in {1..6}; do go test -run 2A -race | tee -a $i$i.log; done &
done

wait
echo "All done"
