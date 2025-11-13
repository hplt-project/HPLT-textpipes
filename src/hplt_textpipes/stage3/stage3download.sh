DDIR=~/.cache/hplt
mkdir -p $DDIR

curl -o ${DDIR}/openlid-v3.bin https://zenodo.org/records/17601701/files/openlid-v3.bin
curl --location -o ${DDIR}/glotlid-v3.bin https://huggingface.co/cis-lmu/glotlid/resolve/main/model_v3.bin?download=true
