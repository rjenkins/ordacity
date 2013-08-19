#include <string.h>
#include <stdio.h>
#include "../jsmn/jsmn.h"
#include "NodeInfo.h"

NodeInfo* get_node_info(jsmntok_t tokens[], char *buffer) {

  int start = tokens[2].start;
  int end = tokens[2].end;
  char *state = substring(start, end, buffer);
  printf("state is %s\n", state);

  start = tokens[4].start;
  end = tokens[4].end;
  char *connectionID = substring(start, end, buffer);

  struct NodeInfo *node_info = calloc(1, sizeof(struct NodeInfo));
  node_info->state = strndup(state, strnlen(state, 1024));
  node_info->connection_id = strndup(connectionID, strnlen(connectionID, 1024));

  return node_info;
}

char *substring(int start, int end, char* buffer) {

  char *string = calloc(1, (end - start) + 1);
  char *s = string;
  char *p = buffer;
  p = p + start;
  while (start < end) {
    *s++ = *p++;
    start++;
  }

  *s++ = '\0';
  return string;
}

