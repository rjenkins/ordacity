#define INITIAL_SET_SIZE 10

typedef struct StringSet {
  void (*add)(void *set, char *element);
  int (*size)(void *set);
  char **elements;
  int current_size;
} StringSet;


/**
 * Public API
 */
StringSet *create_string_set();

/**
 * Private Functions
 */

static int size(StringSet *set); 
static void add(StringSet *set, char *element);
static void print(StringSet *set);
