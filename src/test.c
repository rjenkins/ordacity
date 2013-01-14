#include <stdio.h>
#include "Cluster.h"

int main() {

  ClusterListener *listener = (ClusterListener *) malloc(sizeof(const ClusterListener *));
  ClusterConfig *config = (ClusterConfig *) malloc(sizeof(const ClusterConfig *));

  config->hosts = "localhost:2181";
  config->node_id = "foobar";
  config->zkTimeout = 100000;
  config->work_unit_name = "foobar_units";
  config->work_unit_short_name = "funits";

  Cluster *c = create_cluster("test", listener, config);
  c->join();
  while(1) {}
}
