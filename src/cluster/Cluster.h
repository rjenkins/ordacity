#include "ClusterConfig.h"
#include "ClusterListener.h"

#ifdef DEBUG
# define DEBUG_PRINT(x) printf x
#else
# define DEBUG_PRINT(x) do {} while (0)
#endif

#define NODE_STATE_FRESH 0
#define NODE_STATE_SHUTDOWN 1
#define NODE_STATE_DRAINING 2
#define NODE_STATE_STARTED 3

#define FALSE 0
#define TRUE 1

// ERROR CODES
#define ERROR_STARTING_CLAIMER 10
#define ERROR_WATCHING_NODES 20
#define ERROR_WATCHING_UNITS 30

typedef struct Cluster {
    const char *name;
    void (*join)();
//    void (*shutdown)();
//    void (*rebalance)();
} Cluster;

typedef struct key {
  char *key;
} key;

typedef struct value {
  char *value;
}value;

Cluster *create_cluster(const char *name, ClusterListener *cluster_listener, ClusterConfig* config);

/**
 * private functions
 */

static void join();
static void join_cluster();
static void cluster_connect();
static int start_claimer();
static void * claim_run();
static void claim_work();
static void force_shutdown();
static void set_node_state(char *state);
static void register_watchers();
static void register_node_change_watchers(struct String_vector nodes, char * nodes_path);
static void register_work_unit_watchers(struct String_vector units, char * units_path);
static void on_connect();
static int is_previous_zk_active();
static void my_strings_completion(int rc, const struct String_vector *strings,
                const void *data);

static void my_data_completion(int rc, const char *value, int value_len,
      const struct Stat *stat, const void *data);

static void my_stat_completion(int rc, const struct Stat *stat, const void *data);
static void ensure_clean_startup();

static unsigned int node_hash(void *str);
static int node_info_equal(void *node_info1,void *node_info2);
static char *substring(int start, int end, char* buffer);
static int string_equal(void *key1,void *key2);
