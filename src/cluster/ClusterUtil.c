#include <zookeeper.h>
#include "Cluster.h"
#include "ClusterUtil.h"

static void ensure_path(zhandle_t *zh, char *path);

unsigned int string_hash(void *str) {
  unsigned int hash = 5381;
  int c;
  const char* cstr = (const char*) str;
  while ((c = *cstr++))
    hash = ((hash << 5) + hash) + c; /* hash * 33 + c */

  return hash;
}

int string_equal(void *key1, void *key2) {
  return strcmp((const char*) key1, (const char*) key2) == 0;
}

void ensure_ordacity_paths(zhandle_t *zh, Cluster *cluster, ClusterConfig *cluster_config) {
  char *root_path = "/";
  char *root = cluster->name;

  char buffer[1024];

  memset(buffer, 0, 1024);

  // TODO - Make this safer dogg
  // check and add /<name> and /name/nodes
  strcpy(buffer, root_path);
  strcat(buffer, root);

  ensure_path(zh, &buffer);

  strcat(buffer, "/nodes");

  ensure_path(zh, &buffer);

  // work units
  memset(buffer, 0, 1024);

  strcpy(buffer, root_path);
  strcat(buffer, cluster_config->work_unit_name);

  ensure_path(zh, &buffer);

  // - /<name>/meta

  memset(buffer, 0, 1024);
  strcpy(buffer, root_path);
  strcat(buffer, root);
  strcat(buffer, "/meta");

  ensure_path(zh, &buffer);

  strcat(buffer, "/rebalance");

  ensure_path(zh, &buffer);

  memset(buffer, 0, 1024);
  strcpy(buffer, root_path);
  strcat(buffer, root);
  strcat(buffer, "/meta/workload");

  ensure_path(zh, &buffer);

  memset(buffer, 0, 1024);
  strcpy(buffer, root_path);
  strcat(buffer, root);
  strcat(buffer, "/claimed-");
  strcat(buffer, cluster_config->work_unit_short_name);

  ensure_path(zh, &buffer);

  memset(buffer, 0, 1024);
  strcpy(buffer, root_path);
  strcat(buffer, root);
  strcat(buffer, "/handoff-requests");

  ensure_path(zh, &buffer);

  memset(buffer, 0, 1024);
  strcpy(buffer, root_path);
  strcat(buffer, root);
  strcat(buffer, "/handoff-result");

  ensure_path(zh, &buffer);
}

static void ensure_path(zhandle_t *zh, char *path) {

  if (zoo_exists(zh, path, 0, 0x00) == ZNONODE) {
    zoo_create(zh, path, "", 1, &ZOO_OPEN_ACL_UNSAFE, 0, 0x00, 0);
  }
}
