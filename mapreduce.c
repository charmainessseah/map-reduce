#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include "mapreduce.h"
#include <pthread.h>
pthread_rwlock_t rwlock = PTHREAD_RWLOCK_INITIALIZER;

struct kv {
    char* key;
    char* value;
};

struct kv_list {
    struct kv** elements;
    size_t num_elements;
    size_t size;
};

struct kv_list kvl;
size_t kvl_counter;

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

// takes a key, value pair (both strings) as input 
// key: word, value: count (1)
// we need to store these key value pairs from various threads globally 
// will be accessed by reducers 
void MR_Emit(char *key, char *value) 
{
    pthread_rwlock_wrlock(&rwlock);
    struct kv *pair = (struct kv*) malloc(sizeof(struct kv));
    if (pair == NULL) {
        printf("Malloc error! %s\n", strerror(errno));
        pthread_rwlock_unlock(&rwlock);
        exit(1);
    }
    pair->key = strdup(key);
    pair->value = strdup(value);
    pthread_rwlock_unlock(&rwlock);
    add_to_list(pair);
    
}

// num partitions has to be equal to number of reducers
// map -> partition -> reduce
unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

void MR_Run(int argc, char *argv[],
            Mapper map, int num_mappers,
            Reducer reduce, int num_reducers,
            Partitioner partition)
{
    // create data structure to to pass keys and values from mappers to reducers
    init_kv_list(10); // init kv list to size 10

    if (argc - 1 != num_mappers) {
        printf("incorrect number of args...\n");
        return;
    }

    // create num_mappers threads to perform map tasks
    pthread_t map_threads[num_mappers];
    for (int i  = 1; i <= num_mappers; i++) {
        pthread_t thread;
        map_threads[i - 1] = thread;
        char *filename = argv[i];
        pthread_create(&map_threads[i - 1], NULL, (void *) map, (void *) filename); // create thread calling Map() 
    }
    // wait for all threads before calling reduce
    for (int i = 0; i < num_mappers; i++) {
        pthread_join(map_threads[i], NULL);
    }

    // create num_reducers threads to perform reduction tasks
    // pthread_t reduce_threads[num_reducers];
    // for (int i = 0; i < num_reducers; i++) {
    //     pthread_t thread;
    //     reduce_threads[i] = thread;
    //     pthread_create(&reduce_threads[i], NULL, (void *) reduce,)
    // }
}