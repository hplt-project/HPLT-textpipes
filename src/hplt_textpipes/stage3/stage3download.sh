DDIR=~/.cache/hplt
mkdir -p $DDIR

wget -P $DDIR https://zenodo.org/records/15056559/files/openlid_v2_180325.bin
curl --location -o ${DDIR}/glotlid-v3.bin https://huggingface.co/cis-lmu/glotlid/resolve/main/model_v3.bin?download=true
cp -a cp /scratch/project_465002259/eurolid/frp/model.bin ${DDIR}/openlid-v3.bin
