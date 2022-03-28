#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include "mapreduce.h"
#include <pthread.h>

pthread_rwlock_t rwlock;
int num_partitions;
Partitioner partitioner;
struct kv {
    char* key;
    char* value;
};

struct kv_list {
    struct kv** elements;
    size_t num_elements;
    size_t size;
};

struct partition_map {
    unsigned long partition_number;
    char** list_of_words;
    size_t num_words;
};

struct partition_map_list {
    struct partition_map** elements;
    size_t num_elements; 
};

struct kv_list kvl;
size_t kvl_counter;

struct partition_map_list partition_list;

void init_partition_map_list(size_t size) {
    pthread_rwlock_wrlock(&rwlock);
    //partition_list.elements = (struct partition_map**) malloc(size * (sizeof(size_t) + sizeof(char**) * 256));
    partition_list.elements = (struct partition_map**) malloc(size * sizeof(struct partition_map*));
    for (int i = 0; i < size; i++) {
        struct partition_map *map = (struct partition_map*) malloc(sizeof(struct partition_map));
        partition_list.elements[i] = map;
        map->partition_number = (unsigned long) i + 1;
        for (int i = 0; i < 256; i++) {
            map->list_of_words[i] = NULL;
        } 
        map-> num_words = 0;
    }
    partition_list.num_elements = 0;
    pthread_rwlock_unlock(&rwlock);
}


void init_kv_list(size_t size) {
    pthread_rwlock_wrlock(&rwlock);
    kvl.elements = (struct kv**) malloc(size * sizeof(struct kv*));
    kvl.num_elements = 0;
    kvl.size = size;
    pthread_rwlock_unlock(&rwlock);
}

// we need to lock this because it is changing a shared state
void add_to_list(struct kv* pair) {
    pthread_rwlock_wrlock(&rwlock);
    if (kvl.num_elements == kvl.size) { // resize our key value list
        kvl.size *= 2;
        kvl.elements = realloc(kvl.elements, kvl.size * sizeof(struct kv*));
    }
    kvl.elements[kvl.num_elements++] = pair;
    pthread_rwlock_unlock(&rwlock);
}

void add_to_map_list(struct partition_map* map){
    pthread_rwlock_wrlock(&rwlock);
    partition_list.elements[partition_list.num_elements++] = map;
    pthread_rwlock_unlock(&rwlock);
}
// called by Map()
// takes a key, value pair (both strings) as input 
// key: word, value: count (1)
// we need to store these key value pairs from various threads globally 
// do partitioning here?
void MR_Emit(char *key, char *value) 
{
    pthread_rwlock_wrlock(&rwlock);
    struct kv *pair = (struct kv*) malloc(sizeof(struct kv));
    struct partition_map *map = (struct partition_map*) malloc(sizeof(struct partition_map));
    if (pair == NULL) {
        printf("Malloc error! %s\n", strerror(errno));
        pthread_rwlock_unlock(&rwlock);
        exit(1);
    }
    pair->key = strdup(key);
    pair->value = strdup(value);
    unsigned long partitionNumber = partitioner(key, num_partitions);
    map->partition_number = partitionNumber;
    map->list_of_words[map->num_words++] = strdup(key); 
    pthread_rwlock_unlock(&rwlock);
    add_to_list(pair);
    add_to_map_list(map);
}

// num partitions equal to num_reducers
// map -> partition -> reduce
unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

char* get_func(char *key, int partition_number) {
    for(int i = 0; i < 100; i++) {
       if(strcmp(partition_list.elements[partition_number - 1].list_of_words[i], key) == 0){
           if(partition_list.elements[partition_number - 1].list_of_words[i + 1] == NULL) {
	         return NULL;
	   } else {
		 return partition_list.elements[partition_number - 1].list_of_words[i + 1];
       }
    }
}

void MR_Run(int argc, char *argv[],
            Mapper map, int num_mappers,
            Reducer reduce, int num_reducers,
            Partitioner partition)
{
    // create data structure to to pass keys and values from mappers to reducers
    init_kv_list(10); // init kv list to size 10
    num_partitions = num_reducers;
    partitioner = partition;
    init_partition_map_list(num_partitions);
    int num_files = argc - 1;
    int num_map_threads_created = 0;
    
    if (num_mappers > num_files) {
        // TODO: handle this case
        printf("Error: There are more mappers than files\n");
        exit(1);
    }

    // create num_mappers threads to perform map tasks
    pthread_t map_threads[num_mappers];
    
    for (int i  = 0; i < num_mappers; i++) {
        char *filename = argv[i + 1];
        num_map_threads_created++;
        pthread_create(&map_threads[i], NULL, (void *) map, (void *) filename); // create thread calling Map() 
    }

    // look for completed threads to run uncompleted map tasks when (num_files > num_mappers)
    //while (num_map_threads_created != num_files) {
    //    for (int i = 0; i < num_mappers; i++) {
    //        pthread_t curr_thread = map_threads[i];
            // how to check whether a thread is still alive?
    //     }
    // }

    // wait for all threads before partitioning
    //for (int i = 0; i < num_mappers; i++) {
    //    pthread_join(map_threads[i], NULL);
    // }

    // create num_reducers threads to perform reduction tasks
    //pthread_t reduce_threads[num_reducers];
    //for (int i = 0; i < num_reducers; i++) {
    //     pthread_t thread;
    //     reduce_threads[i] = thread;
    //     pthread_create(&reduce_threads[i], NULL, (void *) reduce,)
    //}
}
