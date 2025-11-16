#!/bin/bash

if [ -z "${1}" -o ! -d "${1}" ]; then
  echo "zstdconcat: missing or invalid directory argument; exit." >&2;
  exit 1;
fi

input=${1};

prefix=/tmp/.pipe.${USER}.${$};
declare -a pipes;
i=3;
if [ -f ${input}/allowed.zst ]; then
  pipes[${i}]=${prefix}.${i};
  [ -e ${pipes[${i}]} ] && /bin/rm -f ${pipes[${i}]}
  mkfifo ${pipes[${i}]};
  zstdcat ${input}/allowed.zst > ${pipes[${i}]} &
  eval "exec ${i}< ${pipes[${i}]}";
  allowed=${i};
fi
i=4;
for file in ${input}/*.zst; do
  [ "${file##*/}" = "allowed.zst" ] && continue;
  pipes[${i}]=${prefix}.${i};
  [ -e ${pipes[${i}]} ] && /bin/rm -f ${pipes[${i}]}
  mkfifo ${pipes[${i}]};
  zstdcat ${file} > ${pipes[${i}]} &
  eval "exec ${i}< ${pipes[${i}]}";
  i=$[${i} + 1];
done
n=$[${i} - 1];
while :; do
  i=4;
  if [ -n "${allowed}" ]; then
    read -r -u 3 line;
    if [ "${line#*false}" != "${line}" ]; then
      while [ ${i} -le ${n} ]; do read -r -u ${i}; i=$[${i} + 1]; done
      continue;
    fi
  fi
  while [ ${i} -le ${n} ]; do
    read -r -u ${i} line;
    [ -z "${line}" ] && break 2;
    if [ ${n} -gt 4 -a ${i} -eq 4 ]; then echo -n "${line%\}},";
    elif [ ${n} -gt 4 -a ${i} -lt ${n} ]; then line="${line#\{}"; echo -n "${line%\}},";
    elif [ ${n} -gt 4  ]; then echo "${line#\{}";
    else echo "${line}"; fi
    i=$[${i} + 1];
  done
done

/bin/rm -f ${pipes[@]};
