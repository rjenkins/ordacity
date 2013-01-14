#include "zookeeper.h"

typedef struct {          
  void (*onJoin)(zhandle_t *zh);
  void (*onLeave)();
  void (*shutdownWork)(char *workUnit);
  void (*startWork)(char *workUnit);
} ClusterListener;

