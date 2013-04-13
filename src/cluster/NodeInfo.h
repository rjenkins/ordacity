typedef struct NodeInfo {
  char *state;
  char *connection_id;
} NodeInfo;

NodeInfo* get_node_info(jsmntok_t tokens[], char *buffer);
char *substring(int start, int end, char* buffer);
