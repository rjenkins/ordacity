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
