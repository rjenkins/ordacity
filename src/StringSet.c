#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "StringSet.h"

/**
 * Create a new Set
 */

StringSet* create_string_set() {
  StringSet *set = (StringSet *) malloc(sizeof(const StringSet *));
  set->add = &add;
  set->elements = (char **)malloc(INITIAL_SET_SIZE *sizeof(char *));

  for(int i=0; i<INITIAL_SET_SIZE; i++) {
    set->elements[i] = malloc(sizeof(char *));
  }
  set->current_size = INITIAL_SET_SIZE;
  return set;
}

static void add(StringSet * set, char *element) {

  int i = set->current_size - 1;
  
  // Work backwards through set looking for first NULL
  while(i>=0 && set->elements[i] == NULL) {
    i--;
  }

  // We have an empty set
  if(i == -1) {
    set->elements[0] = element;
  } else { // Verify this isn't a duplicate
    int insert_index = i + 1;
    while(i >= 0) {
      if(strcmp(set->elements[i], element) == 0) {
        return;
      }
      i--;
    }

    // Resize set
    if(insert_index > set->current_size - 1) {
      set->elements = realloc(set->elements, set->current_size * 2 * sizeof(char*));
      set->current_size = set->current_size * 2;
    }

    set->elements[insert_index] = element;
  }
}

static void print(StringSet *set) {
  for(int i=0; i<set->current_size && set->elements[i] != NULL; i++) {
    printf("%s\n", set->elements[i]);
  }
}
