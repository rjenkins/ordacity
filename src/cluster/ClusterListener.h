#include "../zookeeper.h"

typedef struct {          
  void (*on_join)(zhandle_t *zh);
  void (*onLeave)();
  void (*shutdownWork)(char *workUnit);
  void (*startWork)(char *workUnit);
} ClusterListener;

