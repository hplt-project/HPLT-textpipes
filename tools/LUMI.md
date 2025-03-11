# Bitexting on LUMI

## Paths

Scripts: `/project/project_462000252/zaragoza/cirrus-scripts/`

This is [the lumi branch of cirrus-scripts](https://github.com/paracrawl/cirrus-scripts/tree/lumi).

Data: `/scratch/project_465000498/one/text/{ia,cc}/`

Folders are split in `*-batches` and `*-shards`. The `plain_text.gz`, `url.gz` and `source.gz` are sourced from the data centres that have the warcs and we ran warc2text and giashard on. All other files are results from the processing steps in cirrus-scripts. See the [README](https://github.com/paracrawl/cirrus-scripts/tree/lumi#data-structure) for what file contains what.

## Running
Go to the scripts folder, `/project/project_462000252/zaragoza/cirrus-scripts`.

Generally, every step is ran like e.g. `TPB=64 TPN=8 ./03.split-text.sh -r wide16 en`:

`TPB` is the number of batch files to process per job in the job array. The lower bound is that a smaller TPB will lead to a larger number of jobs being scheduled in Slurm. The upper bound is that there's a chance you will run out of time on the job if TPB is too high. No worries, you can resume processing (at the batch level at least) with the `-r` option.

`TPN` is a way to process multiple batches in parallel inside a Slurm job. Normally, `TPB` are all run sequentially. With `TPN` you can say how many processing flows should be started inside your job. Lower bound is 1 (everything sequentially), upper bound is `$TPN * $THREADS < NUM_CPUS`, and `$TPN <= $TPB`.

You can use `-h` to see all options for each of the submission scripts. The main one is `-r`, which checks existing files and will only schedule processing of missing ones. It is safe to just always call every script with `-r` but sometimes it is slower to schedule since the script has to go through all the batches to see whether the files exist or not.

The `wide16` part is the collection name to operate on. Collections are configured in `condig.d/10.lumi.sh`. You can use `./collections.sh` to see which collections are configured.

All the remaining arguments are assumed to be language codes. Use the ones that match the language codes you see in the data folder.

## Dashboard

Jelmer maintains [a job dashboard](https://github.com/jelmervdl/paracrawl-dashboard) that is a left-over from Paracrawl.

You can access this dashboard at https://paracrawl.ikhoefgeen.nl/. It shows all jobs that have been scheduled through the shared cirrus-scripts/.
