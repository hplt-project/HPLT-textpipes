DDIR=~/.cache/hplt
mkdir -p $DDIR

wget -P $DDIR https://zenodo.org/records/17593102/files/openlid_v3_model.bin
curl --location -o ${DDIR}/glotlid-v3.bin https://huggingface.co/cis-lmu/glotlid/resolve/main/model_v3.bin?download=true
