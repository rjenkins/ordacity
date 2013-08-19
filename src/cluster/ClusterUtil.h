/*
 * ClusterUtil.h
 *
 *  Created on: Apr 14, 2013
 *      Author: rjenkins
 */

#ifndef CLUSTERUTIL_H_
#define CLUSTERUTIL_H_


void ensure_ordacity_paths(zhandle_t *zh, Cluster *cluster, ClusterConfig *cluster_config);
int string_equal(void *key1, void *key2);
unsigned int string_hash(void *str);

#endif /* CLUSTERUTIL_H_ */
