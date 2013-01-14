#include "ClusterConfig.h"
#include "ClusterListener.h"

#define NODE_STATE_FRESH 0
#define NODE_STATE_SHUTDOWN 1
#define NODE_STATE_DRAINING 2
#define NODE_STATE_STARTED 3

#define FALSE 0
#define TRUE 1

// ERROR CODES
#define ERROR_STARTING_CLAIMER 10

typedef struct {
    char *name;
    void (*join)();
    void (*shutdown)();
    void (*rebalance)();
} Cluster;

Cluster *create_cluster(char *name, ClusterListener *cluster_listener, ClusterConfig* config);

/**
 * private functions
 */

static void join();
static void ensure_ordacity_paths();
static void ensure_path();
static void cluster_connect();
static int start_claimer();
static void *claim();
static void force_shutdown();
static void on_connect();
static int is_previous_zk_active();
static void my_strings_completion(int rc, const struct String_vector *strings,
                const void *data);

static void my_data_completion(int rc, const char *value, int value_len,
      const struct Stat *stat, const void *data);

static void ensure_clean_startup();

