#include <stdio.h>
#include "Cluster.h"

void my_join(zhandle_t *zh);

int main() {

  ClusterListener *listener = (ClusterListener *) malloc(sizeof(const ClusterListener *));
  ClusterConfig *config = (ClusterConfig *) malloc(sizeof(const ClusterConfig *));

  listener->on_join = &my_join;

  config->hosts = "localhost:2181";
  config->node_id = "foobar";
  config->zkTimeout = 100000;
  config->work_unit_name = "foobar_units";
  config->work_unit_short_name = "funits";

  Cluster *c = create_cluster("test", listener, config);
  c->join();
  while(1) {}
}

void my_join(zhandle_t *zh) {
  printf("my_join called\n");
}
