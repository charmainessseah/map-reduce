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
    size_t curr_index;
};

struct partition_map_list {
    struct partition_map** elements;
    size_t num_elements;
};

struct kv_list kvl;
size_t kvl_counter;

struct partition_map_list *partition_list;

struct file_data {
    char** filenames;
    int* file_processed_flags_array;
};

// for map threads
struct file_data *global_file_data;
int num_files;
int processed_files;

Reducer reduce_function;

void init_partition_map_list(size_t size) {
    pthread_rwlock_wrlock(&rwlock);
    printf("init partition_map_list\n");
    partition_list = (struct partition_map_list*) malloc(sizeof(struct partition_map_list));
    partition_list->elements = (struct partition_map**) malloc(size * sizeof(struct partition_map*));
    for (int i = 0; i < size; i++) {
        struct partition_map *map = (struct partition_map*) malloc(sizeof(struct partition_map));
        partition_list->elements[i] = map;
        map->partition_number = (unsigned long) i;
        map->list_of_words = (char **) malloc(sizeof(char *) * 256);
        for (int i = 0; i < 256; i++) {
            //            map->list_of_words = (char **) malloc(sizeof(char *) * 256);
            map->list_of_words[i] = (char *) malloc(sizeof(char) * 256);
        } 
        map-> num_words = 0;
        map-> curr_index = 0;
    }
    partition_list->num_elements = 0;
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

void MR_Emit(char *key, char *value) 
{
    pthread_rwlock_wrlock(&rwlock);
    printf("enter emit\n");
    unsigned long partitionNumber = partitioner(key, num_partitions);
    printf("key: %s, part num: %ld\n", key, partitionNumber);
    int numwords = partition_list->elements[partitionNumber]->num_words;
    printf("here 1\n");
    partition_list->elements[partitionNumber]->partition_number = partitionNumber;
    printf("here 2\n");
    partition_list->elements[partitionNumber]->list_of_words[partition_list->elements[partitionNumber]->num_words++] = strdup(key); 
    printf("just stored word: %s\n", partition_list->elements[partitionNumber]->list_of_words[numwords]);
    printf("exit emit\n");
    pthread_rwlock_unlock(&rwlock);
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

char* get_func(char *key, int partition_number) {
    int index = partition_list->elements[partition_number]->curr_index;
    if(strcmp(partition_list->elements[partition_number]->list_of_words[index], key) == 0){
        partition_list->elements[partition_number]->curr_index++;
        return partition_list->elements[partition_number]->list_of_words[index];
    }
    return NULL;
}

void Thread_Map(void* map) {
    printf("Entering Thread_Map() func\n");
    printf("num files: %d, processed files: %d\n", num_files, processed_files);
    while(processed_files != num_files) {
        for (int i = 0; i < num_files; i++) {
            if (!global_file_data->file_processed_flags_array[i]) {
                processed_files++;
                ((Mapper)(map)) (global_file_data->filenames[i]);
                global_file_data->file_processed_flags_array[i] = 1;                    
            }
        }
    }
    printf("num files: %d, processed files: %d\n", num_files, processed_files);
    printf("leaving Thread_Map() func\n");    
}

void Thread_Reduce(void* args) {
    int curr_partition_num = *((int*) args);
    size_t word_count = partition_list->elements[curr_partition_num]->num_words;
    
    //char **unique_words = (char **) malloc(256 * sizeof(char*));
    //for (int i = 0; i < 256; i++) {
    //    unique_words[i] = (char *) malloc(sizeof(char) * 256);
   // }
    char unique_words[word_count][256];
    int num_unique_words = 0;
    printf("curr part num: %d, word count: %ld\n", curr_partition_num, word_count);
    for (int i = 0; i < word_count; i++) {
        if (num_unique_words != 0) {
            printf("we are now comparing %s and %s\n", unique_words[num_unique_words - 1], partition_list->elements[curr_partition_num]->list_of_words[i]);
            if (strcmp(unique_words[num_unique_words - 1], partition_list->elements[curr_partition_num]->list_of_words[i]) != 0) {
                //unique_words[i] = strdup(partition_list->elements[curr_partition_num]->list_of_words[word_count]);
                strcpy(unique_words[num_unique_words++], partition_list->elements[curr_partition_num]->list_of_words[i]);
                printf("adding unique word for partition num %d: %s\n", curr_partition_num, unique_words[num_unique_words - 1]);
                //num_unique_words++;
                printf("adding subsequent unique word\n");
            }
        } else {
            strcpy(unique_words[num_unique_words++], partition_list->elements[curr_partition_num]->list_of_words[i]);
            //num_unique_words++;
            printf("adding first unique word: %s\n", unique_words[num_unique_words - 1]);
        }
    }
    printf("part num %d has %d unique words\n", curr_partition_num, num_unique_words);

    for (int i = 0; i < num_unique_words; i++) {
        (*reduce_function) (unique_words[i], get_func, curr_partition_num);
    }
/*
    // free unique_words
    for (int i = 0; i < num_unique_words; i++) {
        free(unique_words[i]);
    }
    free(unique_words);
*/
}

int cmpstr(const void* a, const void* b) {
    const char* aa = *(const char**)a;
    const char* bb = *(const char**)b;
    int result = strcmp(aa, bb);
    printf("strcmp result between %s and %s: %d\n", aa, bb, result);
    return result;
}

void MR_Run(int argc, char *argv[],
            Mapper map, int num_mappers,
            Reducer reduce, int num_reducers,
            Partitioner partition)
{
    num_partitions = num_reducers;
    partitioner = partition;
    init_partition_map_list(num_partitions);
    num_files = argc - 1;

    // malloc and init global_file_data filenames and flags
    global_file_data = (struct file_data*) malloc(sizeof(struct file_data)); 
    global_file_data->filenames = (char**) malloc(sizeof(char*) * num_files);
    global_file_data->file_processed_flags_array = malloc(sizeof(int) * num_files);

    for(int i = 0; i < num_files; i++) {
        global_file_data->filenames[i] = strdup(argv[i + 1]);
        global_file_data->file_processed_flags_array[i] = 0;
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

printf("before sorting word lists:\n"); 
     for (int i = 0; i < num_partitions; i++) {
         printf("part num: %d , num words: %ld ", i, partition_list->elements[i]->num_words);
         printf(", list of words: ");
         for (int j = 0; j < partition_list->elements[i]->num_words; j++) {
             printf("%s, ", partition_list->elements[i]->list_of_words[j]);
         }
         printf("\n");
     }

    // do sorting here
    printf("about to do sorting\n");
    for (int i = 0; i < num_partitions; i++) {
        qsort((void*)partition_list->elements[i]->list_of_words, partition_list->elements[i]->num_words, sizeof(char *), cmpstr); 
    }
    printf("finished sorting\n");
    
    printf("after sorting word lists:\n");    
    for (int i = 0; i < num_partitions; i++) {
        printf("part num: %d , num words: %ld ", i, partition_list->elements[i]->num_words);
        printf(", list of words: ");
        for (int j = 0; j < partition_list->elements[i]->num_words; j++) {
            printf("%s, ", partition_list->elements[i]->list_of_words[j]);
        }
        printf("\n");
    }
    // create reduce threads
    printf("creating reduce threads\n");
    printf("num reduce threads: %d\n", num_reducers);
    pthread_t *reduce_threads = (pthread_t *) malloc(sizeof(pthread_t) * num_reducers);
    reduce_function = reduce;
    int reduce_thread_num = 1;
    for (int i  = 0; i < num_reducers; i++) {
        printf("creating reducer thread num: %d\n", reduce_thread_num++);
        int *curr_partition_num = (int *) malloc(sizeof(int));
        *curr_partition_num = i;
        pthread_create(&reduce_threads[i], NULL, (void *) Thread_Reduce, (void *) curr_partition_num); 
    }
    printf("finished creating reduce threads. Waiting for reduce threads to finish their work\n");

    // wait for all reduce threads
    for (int i = 0; i < num_reducers; i++) {
       pthread_join(reduce_threads[i], NULL);
    }
    printf("all reduced threads have completed\n");

}
