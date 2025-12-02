echo "With N/4 nodes N stage2 batches are processing in parallel. In the worst case with the first_fit_decreasing method we get the largest N files being processed simultaneously. Empirically, temporary files take 7*size(input text.zst)."
echo "Estimated space for temporary files:"
echo -e "N\tTB"
sort -k1rn $1 | cut -f1 -d' '|awk '{sum+=$1; print 7*sum/2**40}'|cat -n
