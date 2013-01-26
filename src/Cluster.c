#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include "queue.h"
#include "Cluster.h"
#include "NodeInfo.h"

/**
 * Lock for our node_state, initialization state and zookeeper connection state
 *
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

Cluster *c;

static clientid_t myid;

// Cluster, node and work unit state
struct hashtable* nodes_table;

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
Cluster *create_cluster(const char *name, ClusterListener *listener, ClusterConfig *config)
{

  cluster_config = config;
  cluster_listener = listener;

  if (pthread_mutex_init(&state_lock, NULL) != 0)
  {
    printf("\n mutex init failed\n");
    return NULL;
  }

  if (pthread_mutex_init(&initialized_lock, NULL) != 0)
  {
    printf("\n mutex init failed\n");
    return NULL;
  }

  if (pthread_mutex_init(&connected_lock, NULL) != 0)
  {
    printf("\n mutex init failed\n");
    return NULL;
  }

  if (pthread_mutex_init(&watches_registered_lock, NULL) != 0)
  {
    printf("\n mutex init failed\n");
    return NULL;
  }

  queue = ALLOC_QUEUE_ROOT();
  struct queue_head *item = malloc(sizeof(struct queue_head));
  INIT_QUEUE_HEAD(item);

  nodes_table = create_hashtable(32,node_hash,node_info_equal);

  c = (Cluster *) malloc(sizeof(const Cluster *));
  c->name = name;
  c->join = &join;
  return c;
}

/**
 *
 * join - our private implementation of join exposed in our cluster struct returned to the user
 * atomically inspects our node_state and calls connect if we're in NODE_STATE_FRESH or 
 * NODE_STATE_SHUTDOWN otherwise ignores  
 *
 */
static void join() 
{
  pthread_mutex_lock(&state_lock);
  if(node_state == NODE_STATE_FRESH) {
    pthread_mutex_unlock(&state_lock);
    cluster_connect();
  } else if(node_state == NODE_STATE_SHUTDOWN) {
    pthread_mutex_unlock(&state_lock);
    cluster_connect();
  } else if(node_state == NODE_STATE_DRAINING) {
    printf("join called while draining; ignoring\n");
  } else if(node_state == NODE_STATE_STARTED) {
    printf("join called while started; ignoring\n");
  }

  pthread_mutex_unlock(&state_lock);
}

/**
 * Our implemenation of a zookeeper watcher is passed on zookeeper_init and called when
 * zookeeper connection state changes on succesful connection we launch our service with 
 * a call to on_connect
 */
static void connection_watcher(zhandle_t *zzh, int type, int state, const char *path, void *watcherCtx) 
{

    if(state == ZOO_CONNECTED_STATE) {
      printf("\nZookeeper session estabslished!\n");
      const clientid_t *id = zoo_client_id(zzh);
      if(myid.client_id == 0 || myid.client_id != id->client_id) {
        myid = *id;
        fprintf(stderr, "Got a new session id: 0x%llx\n", _LL_CAST_ myid.client_id);
      }
      pthread_mutex_lock(&connected_lock);
      connected = 1;      
      context = watcherCtx;
      pthread_mutex_unlock(&connected_lock);

      pthread_mutex_lock(&state_lock);
      if(node_state != NODE_STATE_SHUTDOWN) {
        pthread_mutex_unlock(&state_lock);
        on_connect();
      } else {
        printf("This node is shut down. ZK connection re-established, but not relaunching.\n");
      }
    } else if(state == ZOO_EXPIRED_SESSION_STATE) {
      printf("Zookeeper session expired\n");
      pthread_mutex_lock(&connected_lock);
      connected = 0;      
      pthread_mutex_unlock(&connected_lock);
      force_shutdown();

      // TODO look into whether we need to implement await reconnect
      //await_reconnect();
    } else  {
      printf("ZooKeeper session interrupted. Shutting down and awaiting reconnect");
      pthread_mutex_lock(&connected_lock);
      connected = 0;      
      pthread_mutex_unlock(&connected_lock);

      // TODO look into whether we need to implement await reconnect
      //await_reconnect();
      //await_reconnect();
    }
}

static void on_connect() 
{
  pthread_mutex_lock(&state_lock);
  if(node_state != NODE_STATE_FRESH) {
    if(is_previous_zk_active()) {
      //TODO implementation
    } else {
       printf("Rejoined after session timeout. Forcing shutdown and clean startup.\n");
       ensure_clean_startup();
    }
  }
  pthread_mutex_unlock(&state_lock);

  printf("Connected to Zookeeper (ID: %s).\n", cluster_config->node_id);

  ensure_ordacity_paths();

  join_cluster();
  cluster_listener->on_join(zh);

  pthread_mutex_lock(&watches_registered_lock);
  if(watches_registered == 0) {
    watches_registered = 1;
  }
  pthread_mutex_unlock(&watches_registered_lock);
  register_watchers();
  
  pthread_mutex_lock(&initialized_lock);
  initialized = 1;
  pthread_mutex_unlock(&initialized_lock);

}

void nodes_watcher(zhandle_t *zzh, int type, int state, const char *path, void* context)
{
  pthread_mutex_lock(&initialized_lock);
  if(initialized == 0)
    return;
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

  rc = zoo_wget_children(zh, path, nodes_watcher, context, &str);                
  if (ZOK != rc){                                          
    printf("Problems  %d\n", rc);                        
  } else {                                                 
    int i = 0;                                           
    while (i < str.count) {                              
      printf("Children %s\n", str.data[i++]);          
    }                                                    
    if (str.count) {                                     
      deallocate_String_vector(&str);                  
    }                                                    
  }                         
}

void verify_integrity_watcher(void *watcherCtx, stat_completion_t completion, const void *data)
{

}

void handoff_results_watcher(void *watcherCtx, stat_completion_t completion, const void *data)
{

}

static void register_watchers() 
{
  //TODO move these paths into a helper function 
  char buffer[1024];
  memset(buffer, 0, 1024);
  strcpy(buffer, "/");
  strncat(buffer, c->name, strlen(c->name));
  strncat(buffer, "/nodes", strlen("/nodes"));

  printf("buffer is %s\n", buffer);

  struct String_vector nodes; 

  int nodes_ret_val = zoo_wget_children(zh, strdup(&buffer), 
      nodes_watcher, context, &nodes);

  printf("nodes_ret_val is: %d\n", nodes_ret_val); 

  memset(buffer, 0, 1024);
  strcpy(buffer, "/");
  strncat(buffer, cluster_config->work_unit_name, strlen(cluster_config->work_unit_name));

  int work_unit_ret_val = zoo_wget_children(zh, strdup(&buffer),
      verify_integrity_watcher, NULL,
      (struct String_vector *) malloc(sizeof(struct String_vector)));
      
  memset(buffer, 0, 1024);
  strcpy(buffer, "/");
  strncat(buffer, c->name, strlen(c->name));
  strcat(buffer, "/claimed-");
  strncat(buffer, cluster_config->work_unit_short_name, strlen(cluster_config->work_unit_short_name));

  work_unit_ret_val = zoo_wget_children(zh, strdup(&buffer),
      verify_integrity_watcher, NULL, 
      (struct String_vector *) malloc(sizeof(struct String_vector)));


  if(cluster_config->use_soft_handoff == TRUE) {
    memset(buffer, 0, 1024);
    strcpy(buffer, "/");
    strncat(buffer, c->name, strlen(c->name));
    strcat(buffer, "/handoff-requests");

    int hand_off_ret_val = zoo_wget_children(zh, strdup(&buffer),
      verify_integrity_watcher, NULL, 
      (struct String_vector *) malloc(sizeof(struct String_vector)));

    memset(buffer, 0, 1024);
    strcpy(buffer, "/");
    strncat(buffer, c->name, strlen(c->name));
    strcat(buffer, "/handoff-result");

    hand_off_ret_val = zoo_wget_children(zh, strdup(&buffer),
      handoff_results_watcher, NULL, 
      (struct String_vector *) malloc(sizeof(struct String_vector)));
  }

  if(cluster_config->use_smart_balancing == TRUE) {
    //TODO impl smart balancing ?
  }


}

static void join_cluster() 
{

  const int n = snprintf(NULL, 0, "%lu", myid.client_id);
  char buf[n+1];
  snprintf(buf, n+1, "%lu", myid.client_id);

  char buffer[1024];
  memset(buffer, 0, 1024);
  strcpy(buffer, "{\"state\": \"Fresh\", \"connectionID\":");
  strcat(buffer, buf);
  strcat(buffer, "}");

  printf("BUFFER IS %s\n", buffer);

  char *node_name = c->name;

  char path_buffer[1024];
  strcpy(path_buffer, "/");
  strcat(path_buffer, node_name);
  strcat(path_buffer, "/nodes/");
  strncat(path_buffer, cluster_config->node_id, strlen(cluster_config->node_id));

  while(TRUE) {
    int zoo_create_ret_val = zoo_create(zh, path_buffer, buffer, sizeof(buffer), 
        &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL, NULL, 0);
    if(zoo_create_ret_val == 0) {
      return;
    } else {
      printf("Unable to register with Zookeeper on launch. \n");
      printf("Is %s already running on this host? Retrying in 1 second...\n", c->name);
      sleep(1);
    }
  }
}

static void ensure_ordacity_paths() {
  char *root_path = "/";
  char *root = c->name;

  char buffer[1024];

  memset(buffer, 0, 1024);

  // TODO - Make this safer dogg
  // check and add /<name> and /name/nodes
  strcpy(buffer, root_path);
  strcat(buffer, root);

  ensure_path(&buffer);

  strcat(buffer, "/nodes");

  ensure_path(&buffer); 

  // - /<name>/meta

  memset(buffer, 0, 1024);
  strcpy(buffer, root_path);
  strcat(buffer, root);
  strcat(buffer, "/meta");

  ensure_path(&buffer);

  strcat(buffer, "/rebalance");

  ensure_path(&buffer);
  
  memset(buffer, 0, 1024);
  strcpy(buffer, root_path);
  strcat(buffer, root);
  strcat(buffer, "/meta/workload");

  ensure_path(&buffer);

  memset(buffer, 0, 1024);
  strcpy(buffer, root_path);
  strcat(buffer, root);
  strcat(buffer, "/claimed-");
  strcat(buffer, cluster_config->work_unit_short_name);

  ensure_path(&buffer);

  memset(buffer, 0, 1024);
  strcpy(buffer, root_path);
  strcat(buffer, root);
  strcat(buffer, "/handoff-requests");

  ensure_path(&buffer);

  memset(buffer, 0, 1024);
  strcpy(buffer, root_path);
  strcat(buffer, root);
  strcat(buffer, "/handoff-result");

  ensure_path(&buffer);
}

static void ensure_path(char *path) {

  if(zoo_exists(zh, path, 0, NULL) == ZNONODE) {
    zoo_create(zh, path, "", 1, &ZOO_OPEN_ACL_UNSAFE, 0,
    NULL, 0);
  }
}


static void ensure_clean_startup() {}

static int is_previous_zk_active() 
{
  char nodeName[1024];
  strcat(nodeName, c->name);
  strcat(nodeName, "/nodes/");
  strcat(nodeName, cluster_config->node_id);
  printf("nodeName is: %s\n", nodeName);
  zoo_aget(zh, nodeName, 0, my_data_completion, nodeName); 
  return 0;
}

/**
 * cluster_connect - atomically check cluster connection state 
 * start claimer and connect to zookeeper. If claimer fails
 * to start exit with ERROR_STARTING_CLAIMER
 */
static void cluster_connect() 
{

  pthread_mutex_lock(&initialized_lock);
  if(initialized == 0) { //Not initialized
    printf("Connecting to hosts %s\n", cluster_config->hosts);

    // Exit if unable to start claimer
    if(start_claimer() != 0) 
      exit(ERROR_STARTING_CLAIMER);

    zh = zookeeper_init(cluster_config->hosts, connection_watcher, 30000,0, 0, 0);
  }
  pthread_mutex_unlock(&initialized_lock);
}

/**
 * start our claimer thread
 */
static int start_claimer() {
   int err = pthread_create(&claimer_thread, NULL, claim, NULL);
   if (err != 0)
    printf("\ncan't create thread :[%d]", err);
   return err;
}

/**
 * Our claimer implemenation - waits on a blocking work queue to claim work
 */
static void *claim() {
  printf("Claimer started\n");
  pthread_mutex_lock(&state_lock);
  while(node_state != NODE_STATE_SHUTDOWN) {
    struct queue_head *item = queue_get(queue);
    //printf("item is: %s\n", item);
  }
  pthread_mutex_unlock(&state_lock);
  return NULL;
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

void my_strings_completion(int rc, const struct String_vector *strings,
            const void *data) {

  if (strings) {
    for (int i=0; i < strings->count; i++) {
      fprintf(stderr, "\t%s\n", strings->data[i]);
    }   
    free((void*)data);
  }
}

void my_data_completion(int rc, const char *value, int value_len,
  const struct Stat *stat, const void *data) {
  if (value) 
  {
    fprintf(stderr, " value_len = %d\n", value_len);
  }
  fprintf(stderr, "\nStat:\n");
  free((void*)data);
}

void my_stat_completion(int rc, const struct Stat *stat, const void *data) {
  fprintf(stderr, "%s: rc = %d Stat:\n", (char*)data, rc);
}

static unsigned int node_hash(void *str)
{
  unsigned int hash = 5381;
  int c;
  const char* cstr = (const char*)str;
  while ((c = *cstr++))
    hash = ((hash << 5) + hash) + c; /* hash * 33 + c */

  return hash;
}

static int node_info_equal(void *node_info1,void *node_info2)
{
  struct NodeInfo * node1 = (struct NodeInfo *) node_info1;
  struct NodeInfo * node2 = (struct NodeInfo *) node_info2;
  return strcmp(node1->state,node2->state) ==0 &&
    node1->connection_id == node2->connection_id;
}
