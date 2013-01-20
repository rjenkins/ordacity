#include <stdio.h>
#include "Cluster.h"

void my_join(zhandle_t *zh);

int main() {
  

  ClusterListener *listener = (ClusterListener *) malloc(sizeof(ClusterListener *));
  ClusterConfig *config =  malloc(sizeof(ClusterConfig));

  listener->on_join = &my_join;

  config->hosts = "localhost:2181";
  config->work_unit_name = "foobar_units";
  config->work_unit_short_name = "f_units";
  config->node_id = "foobar";
  config->enable_auto_rebalance = 0;
  config->auto_rebalance_interval = 60;
  config->drain_time = 60;
  config->use_smart_balancing = 0;
  config->node_id = "foobar";
  config->zkTimeout = 100000;
  config->use_soft_handoff = 0;
  config->handoff_shutdown_delay = 0;

  Cluster *c = create_cluster("test", listener, config);
  c->join();
  while(1) {}
}

void my_join(zhandle_t *zh) {
  printf("my_join called\n");
}
