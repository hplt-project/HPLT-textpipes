// 
// Copyright (c) 2022–22 Stephan Oepen <oe@ifi.uio.no>
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation; either version 2.1 of the License, or (at
// your option) any later version.
//

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main() {

  char *buf = (char *)NULL;
  int size = 1024;
  int i, j, l, n;

  setvbuf(stdin, (char *)NULL, _IOFBF, 1024 * 1024);
  setvbuf(stdout, (char *)NULL, _IOFBF, 1024 * 1024);

  if((buf = (char *)malloc(size)) == NULL) {
    fprintf(stderr, "jsonl: out of memory; exit.\n");
    exit(-1);
  } // if

  while(1) {
    l = 0;
    //
    // read one newline-terminated line, increasing buffer size as needed
    //
    if(fgets(buf, size, stdin) == NULL) {
      exit(0);
    } // if
    n = strlen(buf);
    while(buf[n - 1] != '\n') {
      size += size;
      if((buf = (char *)realloc(buf, size)) == NULL) {
        fprintf(stderr, "jsonl: [%d] out of memory; exit.\n", l);
        exit(-1);
      } // if
      if(fgets(&buf[n], size / 2, stdin) == NULL) {
        fprintf(stderr, "jsonl: [%d] incomplete line; exit.\n", l);
        exit(0);
      } // if
      n += strlen(&buf[n]);
    } // while

    //
    // find the JSON-encoded raw string, unescape, and dump to stdout
    // _fix_me_
    // could alternatively directly use putc(3), which should be a macro
    //
    char *in, *out, *foo;
    if((in = strstr(buf, "\"s\": \"")) == NULL) {
      fprintf(stderr, "jsonl: [%d] invalid JSON; exit.\n", l);
      exit(-1);
    } // if
    for(in += 6, out = foo = &buf[0]; *in; ++in, ++foo) {
      if(*in == '"') break;
      if(*in == '\\') {
        ++in;
        switch(*in) {
          case '\\':
            *foo = '\\';
            break;
          case 'n':
            *foo = '\n';
            break;
          case '"':
            *foo = '"';
            break;
          default:
            fprintf(stderr,
                    "jsonl: [%d] invalid escape character '\\%c'; ignore.\n",
                    l, *in);
            --foo;
        } // switch
      } // if
      else *foo = *in;
    } // for
    *foo++ = '\n'; *foo = (char)0;
    fputs(out, stdout);
    ++l;
  } // while

  exit(0);
      
} // main()
