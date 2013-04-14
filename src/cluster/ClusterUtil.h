/*
 * ClusterUtil.h
 *
 *  Created on: Apr 14, 2013
 *      Author: rjenkins
 */

#ifndef CLUSTERUTIL_H_
#define CLUSTERUTIL_H_

unsigned int string_hash(void *str);
void ensure_ordacity_paths(zhandle_t *zh, Cluster *cluster, ClusterConfig *cluster_config);

#endif /* CLUSTERUTIL_H_ */
