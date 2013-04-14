/*
 * ClusterUtil.h
 *
 *  Created on: Apr 14, 2013
 *      Author: rjenkins
 */

#ifndef CLUSTERUTIL_H_
#define CLUSTERUTIL_H_

void ensure_ordacity_paths(zhandle_t *zh, Cluster *cluster, ClusterConfig *cluster_config);
static void ensure_path(zhandle_t *zh, char *path);

#endif /* CLUSTERUTIL_H_ */
