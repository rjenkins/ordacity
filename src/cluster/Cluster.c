/*
 * ClusterUtil.h
 *
 *  Created on: Apr 14, 2013
 *      Author: rjenkins
 */

#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include "hashtable.h"
#include "../collection/queue.h"
#include "../collection/StringSet.h"
#include "../jsmn/jsmn.h"
#include "Cluster.h"
#include "ClusterUtil.h"
#include "NodeInfo.h"
#include "WorkUnit.h"
#include "zookeeper.h"

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
static void ensure_clean_startup();

static unsigned int node_hash(void *str);
static int string_equal(void *key1,void *key2);

/**
 * Lock for our node_state, initialization state and Zookeeper connection state
 */
pthread_mutex_t state_lock;
pthread_mutex_t initialized_lock;
pthread_mutex_t connected_lock;
pthread_mutex_t watches_registered_lock;

pthread_t claimer_thread;

int node_state = NODE_STATE_FRESH;
int initialized = 0;
int connected = 0;
int watches_registered = 0;

ClusterConfig *cluster_config;
ClusterListener *cluster_listener;

static zhandle_t *zh;
struct queue_root *queue;

Cluster *cluster;

static clientid_t myid;

// Cluster, node and work unit state
struct hashtable *nodes_table;
struct hashtable *all_work_units;

StringSet *my_work_units;

void *context;

#define _LL_CAST_ (long long)

/**
 * create_cluster - Our public function for instantiating a new cluster instance
 * param name - name of our cluster
 * param cluster_listener a pointer to a ClusterListener
 * param config our cluster configuration
 *
 * return a reference to a Cluster
 *
 */
Cluster *create_cluster(const char *name, ClusterListener *listener, ClusterConfig *config) {

  cluster_config = config;
  cluster_listener = listener;

  // Initialize mutexes
  if (pthread_mutex_init(&state_lock, NULL ) != 0) {
    printf("\n mutex init failed\n");
    return NULL ;
  }

  if (pthread_mutex_init(&initialized_lock, NULL ) != 0) {
    printf("\n mutex init failed\n");
    return NULL ;
  }

  if (pthread_mutex_init(&connected_lock, NULL ) != 0) {
    printf("\n mutex init failed\n");
    return NULL ;
  }

  if (pthread_mutex_init(&watches_registered_lock, NULL ) != 0) {
    printf("\n mutex init failed\n");
    return NULL ;
  }

  queue = ALLOC_QUEUE_ROOT();
  struct queue_head *item = malloc(sizeof(struct queue_head));
  INIT_QUEUE_HEAD(item);

  //Initialize our Hashtables
  nodes_table = create_hashtable(32, node_hash, string_equal);
  all_work_units = create_hashtable(32, node_hash, string_equal);

  my_work_units = create_string_set();
  my_work_units->add(my_work_units, "foo");
  my_work_units->add(my_work_units, "fa");
  my_work_units->add(my_work_units, "fa");
  my_work_units->add(my_work_units, "figaro");

  my_work_units->size(my_work_units);

  cluster = (Cluster *) malloc(sizeof(const Cluster *));
  cluster->name = name;
  cluster->join = &join;
  return cluster;
}

/**
 *
 * join - our private implementation of join exposed in our cluster struct returned to the user
 * atomically inspects our node_state and calls connect if we're in NODE_STATE_FRESH or 
 * NODE_STATE_SHUTDOWN otherwise ignores  
 *
 */
static void join() {
  pthread_mutex_lock(&state_lock);
  if (node_state == NODE_STATE_FRESH) {
    pthread_mutex_unlock(&state_lock);
    cluster_connect();
  } else if (node_state == NODE_STATE_SHUTDOWN) {
    pthread_mutex_unlock(&state_lock);
    cluster_connect();
  } else if (node_state == NODE_STATE_DRAINING) {
    printf("join called while draining; ignoring\n");
  } else if (node_state == NODE_STATE_STARTED) {
    printf("join called while started; ignoring\n");
  }

  pthread_mutex_unlock(&state_lock);
}

/**
 * Our implemenation of a zookeeper watcher is passed on zookeeper_init and called when
 * zookeeper connection state changes. On a succesful connection we launch our service with
 * a call to on_connect
 */
static void connection_watcher(zhandle_t *zzh, int type, int state, const char *path,
    void *watcherCtx) {
  if (state == ZOO_CONNECTED_STATE) {
    printf("\nZookeeper session estabslished!\n");
    const clientid_t *id = zoo_client_id(zzh);
    if (myid.client_id == 0 || myid.client_id != id->client_id) {
      myid = *id;
      fprintf(stderr, "Got a new session id: 0x%llx\n", _LL_CAST_ myid.client_id);
    }
    pthread_mutex_lock(&connected_lock);
    connected = 1;
    context = watcherCtx;
    pthread_mutex_unlock(&connected_lock);

    pthread_mutex_lock(&state_lock);
    if (node_state != NODE_STATE_SHUTDOWN) {
      pthread_mutex_unlock(&state_lock);
      on_connect();
    } else {
      printf("This node is shut down. ZK connection re-established, but not relaunching.\n");
    }
  } else if (state == ZOO_EXPIRED_SESSION_STATE) {
    printf("Zookeeper session expired\n");
    pthread_mutex_lock(&connected_lock);
    connected = 0;
    pthread_mutex_unlock(&connected_lock);
    force_shutdown();

    // TODO look into whether we need to implement await reconnect
    //await_reconnect();
  } else {
    printf("ZooKeeper session interrupted. Shutting down and awaiting reconnect");
    pthread_mutex_lock(&connected_lock);
    connected = 0;
    pthread_mutex_unlock(&connected_lock);

    // TODO look into whether we need to implement await reconnect
    //await_reconnect();
    //await_reconnect();
  }
}

static void on_connect() {
  pthread_mutex_lock(&state_lock);
  if (node_state != NODE_STATE_FRESH) {
    if (is_previous_zk_active()) {
      //TODO implementation
    } else {
      printf("Rejoined after session timeout. Forcing shutdown and clean startup.\n");
      ensure_clean_startup();
    }
  }
  pthread_mutex_unlock(&state_lock);

  printf("Connected to Zookeeper (ID: %s).\n", cluster_config->node_id);

  ensure_ordacity_paths(zh, cluster, cluster_config);

  join_cluster();
  cluster_listener->on_join(zh);

  pthread_mutex_lock(&watches_registered_lock);
  if (watches_registered == 0) {
    watches_registered = 1;
  }
  register_watchers();
  pthread_mutex_unlock(&watches_registered_lock);

  pthread_mutex_lock(&initialized_lock);
  initialized = 1;
  pthread_mutex_unlock(&initialized_lock);

  pthread_mutex_lock(&state_lock);
  node_state = NODE_STATE_STARTED;
  set_node_state("Started");
  pthread_mutex_unlock(&state_lock);

}

void set_node_state(char * state) {
  char *new_node_state = malloc(
      snprintf(NULL, 0, "{\"state\": \"%s\", \"connectionID\": %lu}", state, myid.client_id) + 1);
  sprintf(new_node_state, "{\"state\": \"%s\", \"connectionID\": %lu}", state, myid.client_id);

  char *node_name = cluster->name;

  char path_buffer[1024];
  strcpy(path_buffer, "/");
  strcat(path_buffer, node_name);
  strcat(path_buffer, "/nodes/");
  strncat(path_buffer, cluster_config->node_id, strlen(cluster_config->node_id));

  int zoo_set_ret_val = zoo_set(zh, path_buffer, new_node_state, strlen(new_node_state), -1);
}

/**
 * Watch the nodes directory
 */
void nodes_dir_watcher(zhandle_t *zzh, int type, int state, const char *path, void* context) {

  printf("created is %d\n", ZOO_CREATED_EVENT);
  printf("changed is %d\n", ZOO_CHANGED_EVENT);
  printf("zoo child is %d\n", ZOO_CHILD_EVENT);

  pthread_mutex_lock(&initialized_lock);
  if (initialized == 0) {
    pthread_mutex_unlock(&initialized_lock);
    return;
  }

  pthread_mutex_unlock(&initialized_lock);

  struct String_vector str;
  int rc;

  printf("The event path %s, event type %d\n", path, type);
  if (type == ZOO_SESSION_EVENT) {
    if (state == ZOO_CONNECTED_STATE) {
      return;
    } else if (state == ZOO_AUTH_FAILED_STATE) {
      zookeeper_close(zzh);
      exit(1);
    } else if (state == ZOO_EXPIRED_SESSION_STATE) {
      zookeeper_close(zzh);
      exit(1);
    }
  }

  rc = zoo_wget_children(zh, path, nodes_dir_watcher, context, &str);
  if (ZOK != rc) {
    printf("Problems  %d\n", rc);
  } else {
    int i = 0;
    while (i < str.count) {
      printf("Children %s\n", str.data[i++]);
    }

    // Add a new item to the queue to start processing
    struct queue_head *item = malloc(sizeof(struct queue_head));
    queue_put(item, queue);

    if (str.count) {
      deallocate_String_vector(&str);
    }
  }
}

void handoff_results_watcher(void *watcherCtx, stat_completion_t completion, const void *data) {

}

void work_units_watcher(zhandle_t *zzh, int type, int state, const char *path, void* context) {

//  printf("in work_units_watcher\n");
//  printf("type is %d\n", type);
//  printf("ZOO_CREATED_EVENT is: %d\n", ZOO_CREATED_EVENT);
//  printf("ZOO_DELETED_EVENT is: %d\n", ZOO_DELETED_EVENT);
  DEBUG_PRINT(("in verify integrety watcher"));

  char buffer[1024];
  memset(buffer, 0, 1024);

  int buflen = sizeof(buffer);
  struct Stat stat;

  DEBUG_PRINT(("full_unit_path is: %s\n", path));

  struct String_vector available_work_units;

  int work_unit_ret_val = zoo_wget_children(zh, path, work_units_watcher, context,
      &available_work_units);

  if (work_unit_ret_val != ZOK) {
    printf("error fetching children in verify_integrity_watcher for: %s\n", path);
    exit(ERROR_WATCHING_NODES);
  } else {
    // Iterate through watchers
    register_work_unit_watchers(available_work_units, path);
  }

}

static void register_watchers() {
  char *nodes_path = malloc(snprintf(NULL, 0, "%s%s%s", "/", cluster->name, "/nodes") + 1);
  sprintf(nodes_path, "%s%s%s", "/", cluster->name, "/nodes");

  struct String_vector nodes;
  int nodes_ret_val = zoo_wget_children(zh, nodes_path, nodes_dir_watcher, context, &nodes);

  if (nodes_ret_val != ZOK) {
    printf("error fetching children in register_watchers for: %s\n", nodes_path);
    exit(ERROR_WATCHING_NODES);
  } else {
    register_node_change_watchers(nodes, nodes_path);
  }

  // free nodes if any results were returned
  if (nodes.count)
    deallocate_String_vector(&nodes);

  //
  char *work_unit_name_path = malloc(
      snprintf(NULL, 0, "%s%s", "/", cluster_config->work_unit_name) + 1);
  sprintf(work_unit_name_path, "%s%s", "/", cluster_config->work_unit_name);

  struct String_vector available_work_units;
  int work_unit_ret_val = zoo_wget_children(zh, work_unit_name_path, work_units_watcher, context,
      &available_work_units);

  if (work_unit_ret_val != ZOK) {
    printf("error fetching children in register_watchers for: %s\n", work_unit_name_path);
    exit(ERROR_WATCHING_UNITS);
  } else {
    register_work_unit_watchers(available_work_units, work_unit_name_path);
  }

  if (available_work_units.count)
    deallocate_String_vector(&available_work_units);

  free(work_unit_name_path);

//  char *claimed_work_unit_path = malloc(
//      snprintf(NULL, 0, "%s%s%s%s", "/", cluster->name, "/claimed-",
//          cluster_config->work_unit_short_name) + 1);
//
//  sprintf(claimed_work_unit_path, "%s%s%s%s", "/", cluster->name, "/claimed-",
//      cluster_config->work_unit_short_name);
//
//  work_unit_ret_val = zoo_wget_children(zh, claimed_work_unit_path, verify_integrity_watcher, NULL,
//      (struct String_vector *) malloc(sizeof(struct String_vector)));
//
//  free(claimed_work_unit_path);
//
//  if (cluster_config->use_soft_handoff == TRUE) {
//
//    char *handoff_requests_path = malloc(
//        snprintf(NULL, 0, "%s%s%s", "/", cluster->name, "/handoff-requests") + 1);
//    sprintf(handoff_requests_path, "%s%s%s", "/", cluster->name, "/handoff-requests");
//
//    int hand_off_ret_val = zoo_wget_children(zh, handoff_requests_path, verify_integrity_watcher,
//        NULL, (struct String_vector *) malloc(sizeof(struct String_vector)));
//
//    free(handoff_requests_path);
//
//    char *handoff_result_path = malloc(
//        snprintf(NULL, 0, "%s%s%s", "/", cluster->name, "/handoff-result") + 1);
//    snprintf(handoff_result_path, "%s%s%s", "/", cluster->name, "/handoff-result");
//
//    hand_off_ret_val = zoo_wget_children(zh, handoff_result_path, handoff_results_watcher, NULL,
//        (struct String_vector *) malloc(sizeof(struct String_vector)));
//
//    free(handoff_result_path);
//
//  }

  if (cluster_config->use_smart_balancing == TRUE) {
    //TODO impl smart balancing ?
  }
}

static void get_and_register_unit_watcher(zhandle_t *zzh, int type, int state, const char *path,
    char* context) {

  if (type == ZOO_DELETED_EVENT) {
    //TODO DEAL WITH WORK UNITS DELETED
  }
}

/**
 * Retrieve each of the available work units and register a watcher for each unit.
 */
static void register_work_unit_watchers(struct String_vector units, char * units_path) {
  int i = 0;
  while (i < units.count) {

    DEBUG_PRINT(("child of %s is %s\n", units_path, units.data[i]));

    char *unit = units.data[i];
    struct String_vector unit_info;

    char *full_unit_path = malloc(snprintf(NULL, 0, "%s%s%s", units_path, "/", unit) + 1);
    sprintf(full_unit_path, "%s%s%s", units_path, "/", unit);

    char buffer[1024];
    memset(buffer, 0, 1024);

    int buflen = sizeof(buffer);
    struct Stat stat;

    DEBUG_PRINT(("full_unit_path is: %s\n", full_unit_path));
    int get_code = zoo_wget(zh, full_unit_path, get_and_register_unit_watcher, unit, buffer,
        &buflen, &stat);

    if (get_code != ZOK) {
      printf("error fetching contents of znode: %s", full_unit_path);
      DEBUG_PRINT(("get code is %d\n", get_code));
      DEBUG_PRINT(("result is %s\n", buffer));
    }

    struct key * work_unit_key = (struct key *) malloc(sizeof(struct key));
    work_unit_key->key = unit;

    // trololololo
    hashtable_insert(all_work_units, work_unit_key, work_unit_key);
    key * res = hashtable_search(all_work_units, work_unit_key);

    i++;
  }
}

/**
 * get_and_register_node_watcher and register_node_change_watchers should be refactored
 */
static void get_and_register_node_watcher(zhandle_t *zzh, int type, int state, const char *path,
    char* context) {

  char buffer[1024];
  memset(buffer, 0, 1024);

  int buflen = sizeof(buffer);
  struct Stat stat;

  //int get_code = zoo_get(zh, full_node_path, 0, buffer, &buflen, &stat);
  int get_code = zoo_wget(zh, path, get_and_register_node_watcher, context, buffer, &buflen, &stat);

  if (get_code != ZOK) {
    printf("error fetching contents of znode: %s", path);
    DEBUG_PRINT(("get code is %d\n", get_code));
    DEBUG_PRINT(("result is %s\n", buffer));
  }

  jsmn_parser parser;
  jsmn_init(&parser);
  jsmntok_t tokens[256];
  if (jsmn_parse(&parser, &buffer, tokens, 256) != JSMN_SUCCESS) {
    printf("error parsing JSON for ordasity node");
  } else {
    NodeInfo * node_info = get_node_info(tokens, buffer);
    struct key * node_info_key = (struct key *) malloc(sizeof(struct key));
    node_info_key->key = context;
    hashtable_insert(nodes_table, node_info_key, node_info);
    NodeInfo *res = hashtable_search(nodes_table, node_info_key);
    printf("res->state is now: %s\n", res->state);
  }
}

static void register_node_change_watchers(struct String_vector nodes, char * nodes_path) {
  int i = 0;
  while (i < nodes.count) {

    DEBUG_PRINT(("child of %s is %s\n", nodes_path, nodes.data[i]));

    char *node = nodes.data[i];
    struct String_vector node_info;

    char *full_node_path = malloc(snprintf(NULL, 0, "%s%s%s", nodes_path, "/", node) + 1);
    sprintf(full_node_path, "%s%s%s", nodes_path, "/", node);

    char buffer[1024];
    memset(buffer, 0, 1024);

    int buflen = sizeof(buffer);
    struct Stat stat;

    int get_code = zoo_wget(zh, full_node_path, get_and_register_node_watcher, node, buffer,
        &buflen, &stat);

    if (get_code != ZOK) {
      printf("error fetching contents of znode: %s", full_node_path);
      DEBUG_PRINT(("get code is %d\n", get_code));
      DEBUG_PRINT(("result is %s\n", buffer));
    }

    jsmn_parser parser;
    jsmn_init(&parser);
    jsmntok_t tokens[256];
    if (jsmn_parse(&parser, &buffer, tokens, 256) != JSMN_SUCCESS) {
      printf("error parsing JSON for ordasity node");
    } else {
      NodeInfo * node_info = get_node_info(tokens, buffer);
      struct key * node_info_key = (struct key *) malloc(sizeof(struct key));
      node_info_key->key = node;
      hashtable_insert(nodes_table, node_info_key, node_info);
      NodeInfo *res = hashtable_search(nodes_table, node_info_key);
      printf("res->state is now: %s\n", res->state);
    }
    i++;
  }
  free(nodes_path);
}

static void join_cluster() {

  const int n = snprintf(NULL, 0, "%lu", myid.client_id);
  char buf[n + 1];
  snprintf(buf, n + 1, "%lu", myid.client_id);

  //TODO Refcator this out with set_node_state

  char buffer[1024];
  memset(buffer, 0, 1024);
  strcpy(buffer, "{\"state\": \"Fresh\", \"connectionID\":");
  strcat(buffer, buf);
  strcat(buffer, "}");

  char *node_name = cluster->name;

  char path_buffer[1024];
  strcpy(path_buffer, "/");
  strcat(path_buffer, node_name);
  strcat(path_buffer, "/nodes/");
  strncat(path_buffer, cluster_config->node_id, strlen(cluster_config->node_id));

  while (TRUE) {
    int zoo_create_ret_val = zoo_create(zh, path_buffer, buffer, sizeof(buffer),
        &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL, NULL, 0);
    if (zoo_create_ret_val == 0) {
      return;
    } else {
      printf("Unable to register with Zookeeper on launch. \n");
      printf("Is %s already running on this host? Retrying in 1 second...\n", cluster->name);
      sleep(1);
    }
  }
}

static void ensure_clean_startup() {
}

static int is_previous_zk_active() {
  char nodeName[1024];
  strcat(nodeName, cluster->name);
  strcat(nodeName, "/nodes/");
  strcat(nodeName, cluster_config->node_id);
  printf("nodeName is: %s\n", nodeName);
  zoo_aget(zh, nodeName, 0, NULL, nodeName);
  return 0;
}

/**
 * cluster_connect - atomically check cluster connection state 
 * start claimer and connect to Zookeeper. If claimer fails
 * to start exit with ERROR_STARTING_CLAIMER
 */
static void cluster_connect() {

  pthread_mutex_lock(&initialized_lock);
  if (initialized == 0) { //Not initialized
    printf("Connecting to host %s\n", cluster_config->hosts);

    // Exit if unable to start claimer
    if (start_claimer() != 0)
      exit(ERROR_STARTING_CLAIMER);

    zh = zookeeper_init(cluster_config->hosts, connection_watcher, 30000, 0, 0, 0);
  }
  pthread_mutex_unlock(&initialized_lock);
}

/**
 * start our claimer thread
 */
static int start_claimer() {
  int err = pthread_create(&claimer_thread, NULL, claim_run, NULL );
  if (err != 0)
    printf("\ncan't create thread :[%d]", err);
  return err;
}

/**
 * Our claimer implementation - waits on a blocking work queue to claim work
 */
static void *claim_run() {
  printf("Claimer started\n");
  pthread_mutex_lock(&state_lock);
  while (node_state != NODE_STATE_SHUTDOWN) {
    struct queue_head *item = queue_get(queue);
    DEBUG_PRINT(("checking claim queue, queue_head is %s\n", item));
    if (item != NULL ) {
      DEBUG_PRINT(("calling claim work"));
      claim_work();
    }
    sleep(2);
  }
  pthread_mutex_unlock(&state_lock);
  return NULL ;
}

/**
 * Our logic for claiming work 
 */

static void claim_work() {
  DEBUG_PRINT(("in claim_work\n"));
  pthread_mutex_lock(&state_lock);
  pthread_mutex_lock(&connected_lock);
  if (node_state != NODE_STATE_STARTED || connected != 1) {
    pthread_mutex_unlock(&state_lock);
    pthread_mutex_unlock(&connected_lock);
    DEBUG_PRINT(("in claim_work node not started, exiting\n"));
    return;
  }

  pthread_mutex_unlock(&state_lock);
  pthread_mutex_unlock(&connected_lock);
  printf("about to check workunit size\n");
  printf("my_work_units size is: %d\n", my_work_units->size(my_work_units));
}

/**
 * force_shutdown - handle cleaning up of balancing policy and workunit and free resources 
 */
static void force_shutdown() {
  //TODO
  // shutdown balancing policy
  //
  // shutdown individual work units
  //
  // cleanup local recources and exit
}


static unsigned int node_hash(void *str) {
  unsigned int hash = 5381;
  int c;
  const char* cstr = (const char*) str;
  while ((c = *cstr++))
    hash = ((hash << 5) + hash) + c; /* hash * 33 + c */

  return hash;
}

static int string_equal(void *key1, void *key2) {
  return strcmp((const char*) key1, (const char*) key2) == 0;
}
