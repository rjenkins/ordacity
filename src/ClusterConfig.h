#define AUTO_REBALANCE_OFF 0
#define AUTO_REBALANCE_ON 1

#define SMART_BALANCING_OFF 0
#define SMART_BALANCING_ON 1

#define DEFAULT_WORK_UNIT_NAME "work-units"
#define DEFAULT_WORK_UNIT_SHORT_NAME "work"

#define SHORT_HANDOFF_OFF 0
#define SHORT_HANDOFF_ON 1

typedef struct  {
  const char* hosts; // a comma separated list of host:port
  int enable_auto_rebalance;
  int auto_rebalance_interval;
  int drain_time;
  int use_smart_balancing;
  int zkTimeout;
  const char *work_unit_name;
  const char *work_unit_short_name;
  const char *node_id;
  int use_soft_handoff;
  int handoff_shutdown_delay;

} ClusterConfig;

