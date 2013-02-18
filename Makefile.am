AUTOMAKE_OPTIONS = foreign

# whatever flags you want to pass to the C compiler & linker
AM_CFLAGS = --pedantic -Wall -std=c99 -O2 
AM_LDFLAGS = -lm 

lib_LIBRARIES = libordacity.a
libordacity_a_SOURCES = src/Cluster.c src/queue_lock_mutex.c src/StringSet.c
include_HEADERS = src/ClusterConfig.h src/ClusterListener.h


