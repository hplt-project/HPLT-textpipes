FIN=sample_halvecc_head100.zst
JOBS=1
mkdir -p out
time zstdcat $FIN | parallel  -j$JOBS --block 100M --pipe "python -m hplt_textpipes.stage3.xml2md 2>out/xml2md_{#}.err  >/dev/null"
time zstdcat $FIN | parallel  -j$JOBS --block 100M --pipe "python -m hplt_textpipes.stage3.fastertext_lid.proto_langid --identity openlid-v3 2>out/openlid-v3_{#}.err  >/dev/null"
time zstdcat $FIN | parallel  -j$JOBS --block 100M --pipe "python -m hplt_textpipes.stage3.fastertext_lid.proto_langid --identity glotlid-v3 2>out/glotlid-v3_{#}.err >/dev/null"
