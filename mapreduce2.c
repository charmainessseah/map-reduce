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

struct file_data {
    char** filenames;
    int* file_processed_flags_array;
};

// for map threads
struct file_data global_file_data;
int num_files;
int processed_files;

// for reduce threads
struct reduce_data {
    void* reduce_function;
    int* partition_number;
};
Reducer reduce_function;

void init_partition_map_list(size_t size) {
    pthread_rwlock_wrlock(&rwlock);
    printf("init partition_map_list\n");
    partition_list.elements = (struct partition_map**) malloc(size * sizeof(struct partition_map*));
    for (int i = 0; i < size; i++) {
        struct partition_map *map = (struct partition_map*) malloc(sizeof(struct partition_map));
        partition_list.elements[i] = map;
        map->partition_number = (unsigned long) i + 1;
        for (int i = 0; i < 256; i++) {
            map->list_of_words = (char **) malloc(sizeof(char *) * 256);
            map->list_of_words[i] = NULL;
        } 
        map-> num_words = 0;
    }
    partition_list.num_elements = 0;
    pthread_rwlock_unlock(&rwlock);
}


void init_kv_list(size_t size) {
    printf("init kv list\n");
    pthread_rwlock_wrlock(&rwlock);
    kvl.elements = (struct kv**) malloc(size * sizeof(struct kv*));
    kvl.num_elements = 0;
    kvl.size = size;
    pthread_rwlock_unlock(&rwlock);
}

// we need to lock this because it is changing a shared state
void add_to_list(struct kv* pair) {
    pthread_rwlock_wrlock(&rwlock);
    printf("adding kv pair\n");
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
    printf("enter emit\n");
    struct kv *pair = (struct kv*) malloc(sizeof(struct kv));
    //struct partition_map *map = (struct partition_map*) malloc(sizeof(struct partition_map));
    if (pair == NULL) {
        printf("Malloc error! %s\n", strerror(errno));
        pthread_rwlock_unlock(&rwlock);
        exit(1);
    }
    pair->key = strdup(key);
    pair->value = strdup(value);
    unsigned long partitionNumber = partitioner(key, num_partitions);
    partition_list.elements[partitionNumber - 1]->partition_number = partitionNumber;
    partition_list.elements[partitionNumber - 1]->list_of_words[partition_list.elements[partitionNumber - 1]->num_words++] = strdup(key); 
    printf("exit emit\n");
    pthread_rwlock_unlock(&rwlock);
    add_to_list(pair);
    //add_to_map_list(map);
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
        if(strcmp(partition_list.elements[partition_number - 1]->list_of_words[i], key) == 0){
            if(partition_list.elements[partition_number - 1]->list_of_words[i + 1] == NULL) {
                return NULL;
            } else {
                return partition_list.elements[partition_number - 1]->list_of_words[i + 1];
            }
        }
    }
    return NULL;
}


void Thread_Map(void* map) {
    printf("Entering Thread_Map() func\n");
    printf("num files: %d, processed files: %d\n", num_files, processed_files);
    while(processed_files != num_files) {
        for (int i = 0; i < num_files; i++) {
            if (!global_file_data.file_processed_flags_array[i]) {
                processed_files++;
                ((Mapper)(map)) (global_file_data.filenames[i]);
                global_file_data.file_processed_flags_array[i] = 1;                    
            }
        }
    }
    printf("num files: %d, processed files: %d\n", num_files, processed_files);
    printf("leaving Thread_Map() func\n");    
}

void Thread_Reduce(void* args) {
    int curr_partition_num = *((int*) args);
    for (int i = 0; i < num_partitions; i++) {
        // get list of words associated with partition number, and call reduce() on each word
        if (partition_list.elements[i]->partition_number == curr_partition_num) {
            for (int j = 0; j < partition_list.elements[i]->num_words; j++) {
                (*reduce_function) (partition_list.elements[i]->list_of_words[j], get_func, 0);
               // ((Reducer)(reduce)) ((partition_list->elements[i])->list_of_words[j], get_func, 0);

            }
            break;
        }
        // (*reduce)((kvl.elements[kvl_counter])->key, get_func, 0);
    }
}

int cmpstr(const void* a, const void* b) {
    const char* aa = (const char*)a;
    const char* bb = (const char*)b;
    return strcmp(aa, bb);
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
    num_files = argc - 1;

    // malloc and init global_file_data filenames and flags
    global_file_data.filenames = (char**) malloc(sizeof(char*) * num_files);
    global_file_data.file_processed_flags_array = malloc(sizeof(int) * num_files);

    for(int i = 0; i < num_files; i++) {
        global_file_data.filenames[i] = strdup(argv[i + 1]);
        global_file_data.file_processed_flags_array[i] = 0;
    }

    if (num_files < num_mappers) {
        num_mappers = num_files;
    }
   
    printf("num files: %d, num mappers: %d\n", num_files, num_mappers);
    // create num_mappers threads to perform map tasks
    pthread_t *map_threads = (pthread_t *) malloc(sizeof(pthread_t) * num_mappers);
    // create mapper threads
    int threadCounter = 1;
    for (int i  = 0; i < num_mappers; i++) {
        printf("creating thread num %d\n", threadCounter);
        threadCounter++;
        pthread_create(&map_threads[i], NULL, (void *) Thread_Map, (void *) map); 
    }

    printf("going to wait for all map threads\n");
    // wait for all threads before sort and reduce
    for (int i = 0; i < num_mappers; i++) {
        printf("now waiting for map_thread number %d\n", i + 1);
        pthread_join(map_threads[i], NULL);
    }

    // do sorting here
    printf("about to do sorting\n");
    for (int i = 0; i< num_partitions; i++) {
        qsort((void*)partition_list.elements[i]->list_of_words, partition_list.elements[i]->num_words,sizeof(char*),cmpstr); 
    }
    printf("finished sorting\n");
    //reduce_thread_data.reduce_function = (void *) malloc(sizeof(Reducer)); 
    //reduce_thread_data.partition_number = (int *) malloc(sizeof(int));

    // create reduce threads
    printf("creating reduce threads\n");
    pthread_t *reduce_threads = (pthread_t *) malloc(sizeof(pthread_t) * num_reducers);
    reduce_function = reduce;
    for (int i  = 0; i < num_reducers; i++) {
        int *curr_partition_num = (int *) malloc(sizeof(int));
        *curr_partition_num = i + 1;
   //     struct reduce_data *reduce_thread_data = (struct reduce_data*) malloc(sizeof(struct reduce_data)); 
    //    reduce_data->partition_number = i + 1;
    //    reduce_data->reduce_function = (void *) reduce;
        pthread_create(&reduce_threads[i], NULL, (void *) Thread_Reduce, (void *) curr_partition_num); 
    }
    printf("finished creating reduce threads. Waiting for reduce threads to finish their work\n");

    // wait for all reduce threads
    for (int i = 0; i < num_reducers; i++) {
        pthread_join(reduce_threads[i], NULL);
    }
    printf("all reduced threads have completed\n");
}