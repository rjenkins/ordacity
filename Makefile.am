AUTOMAKE_OPTIONS = foreign

# whatever flags you want to pass to the C compiler & linker
AM_CFLAGS = --pedantic -Wall -std=c99 -O2 
AM_LDFLAGS = -lm 

lib_LIBRARIES = libordacity.a
libordacity_a_SOURCES = src/cluster/Cluster.c src/cluster/NodeInfo.c src/collection/queue_lock_mutex.c src/collection/StringSet.c src/jsmn/jsmn.c
include_HEADERS = src/cluster/ClusterConfig.h src/cluster/ClusterListener.h src/jsmn/jsmn.h


