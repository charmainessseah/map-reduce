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

/*
struct kv_list {
    struct kv** elements;
    size_t num_elements;
    size_t size;
};*/

struct partition_map {
    unsigned long partition_number;
    char** list_of_words;
    int num_words;
    size_t curr_index;
    size_t capacity;
};

struct partition_map_list {
    struct partition_map** elements;
    size_t num_elements;
};

//struct kv_list kvl;
//size_t kvl_counter;

struct partition_map_list *partition_list;

struct file_data {
    char** filenames;
    int* file_processed_flags_array;
};

struct file_data *global_file_data;
int num_files;
int processed_files;

Reducer reduce_function;

void Free_Partition_List() {
    for (int i = 0; i < num_partitions; i++) {
        size_t capacity = partition_list->elements[i]->capacity;
        for (int j = 0; j < capacity; j++) {
            free(partition_list->elements[i]->list_of_words[j]);
        }
        free(partition_list->elements[i]);
    }
    free(partition_list->elements);
    free(partition_list);
}

void init_partition_map_list(size_t size) {
    pthread_rwlock_wrlock(&rwlock);
    partition_list = (struct partition_map_list*) malloc(sizeof(struct partition_map_list));
    partition_list->elements = (struct partition_map**) malloc(size * sizeof(struct partition_map*));
    for (int i = 0; i < size; i++) {
        partition_list->elements[i] = (struct partition_map*) malloc(sizeof(struct partition_map));
        partition_list->elements[i]->partition_number = (unsigned long) i;
        int init_capacity = 256;
        partition_list->elements[i]->list_of_words = (char **) malloc(sizeof(char *) * init_capacity);
        for (int j = 0; j < 256; j++) {
            partition_list->elements[i]->list_of_words[j] = (char *) malloc(sizeof(char) * 80);
        } 
        partition_list->elements[i]->num_words = 0;
        partition_list->elements[i]->curr_index = 0;
        partition_list->elements[i]->capacity = 256;
    }
    partition_list->num_elements = 0;
    pthread_rwlock_unlock(&rwlock);
}

/*
void init_kv_list(size_t size) {
    pthread_rwlock_wrlock(&rwlock);
    kvl.elements = (struct kv**) malloc(size * sizeof(struct kv*));
    kvl.num_elements = 0;
    kvl.size = size;
    pthread_rwlock_unlock(&rwlock);
}
*/
int wordcount = 0;
void MR_Emit(char *key, char *value) 
{
    pthread_rwlock_wrlock(&rwlock);
    wordcount++;
    unsigned long partitionNumber = partitioner(key, num_partitions);
    int numwords = partition_list->elements[partitionNumber]->num_words;
    int capacity = partition_list->elements[partitionNumber]->capacity;
    if (numwords == (capacity / 2)) {
        int new_capacity = capacity * 2;
            char **newptr = (char **) realloc(partition_list->elements[partitionNumber]->list_of_words, sizeof(char *) * new_capacity);
            partition_list->elements[partitionNumber]->list_of_words = newptr;
        for (int i = capacity; i < new_capacity; i++) {
            partition_list->elements[partitionNumber]->list_of_words[i] = (char *) malloc(sizeof(char) * 80);
        }
        partition_list->elements[partitionNumber]->capacity = new_capacity;
    }
    partition_list->elements[partitionNumber]->partition_number = partitionNumber;
    partition_list->elements[partitionNumber]->list_of_words[partition_list->elements[partitionNumber]->num_words++] = strdup(key); 
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
    int capacity = partition_list->elements[partition_number]->capacity;
    if (index == capacity || key == NULL) {
        return NULL;
    }
    if(strcmp(partition_list->elements[partition_number]->list_of_words[index], key) == 0){
        partition_list->elements[partition_number]->curr_index++;
        return "1";
    }
    return NULL;
}

void Thread_Map(void* map) {
    while(processed_files < num_files) {
        for (int i = 0; i < num_files; i++) {
            pthread_rwlock_wrlock(&rwlock);
            if (!global_file_data->file_processed_flags_array[i]) {
                processed_files++;
                global_file_data->file_processed_flags_array[i] = 1;
                pthread_rwlock_unlock(&rwlock);
                 ((Mapper)(map)) (global_file_data->filenames[i]);
    pthread_rwlock_wrlock(&rwlock);
            }
            pthread_rwlock_unlock(&rwlock);
        }
    }
}

void Thread_Reduce(void* args) {
    pthread_rwlock_wrlock(&rwlock);
    int curr_partition_num = *((int*) args);
    int word_count = partition_list->elements[curr_partition_num]->num_words;
   free((int*)args); 
    char **unique_words = (char **) malloc(sizeof(char *) * word_count);
    for (int i = 0; i < word_count; i++) {
        unique_words[i] = (char *) malloc(sizeof(char) * 256);
    }
    int num_unique_words = 0;
    for (int i = 0; i < word_count; i++) {
        if (num_unique_words != 0) {
            if (strcmp(unique_words[num_unique_words - 1], partition_list->elements[curr_partition_num]->list_of_words[i]) != 0) {
                strcpy(unique_words[num_unique_words++], partition_list->elements[curr_partition_num]->list_of_words[i]);
            }
        } else {
            strcpy(unique_words[num_unique_words++], partition_list->elements[curr_partition_num]->list_of_words[i]);
        }
    }
pthread_rwlock_unlock(&rwlock);
    for (int i = 0; i < num_unique_words; i++) {
        (*reduce_function) (unique_words[i], get_func, curr_partition_num);
    }
    for (int i = 0; i < word_count; i++) {
        free(unique_words[i]);
    }
    free(unique_words);
}

int cmpstr(const void* a, const void* b) {
    const char* aa = *(const char**)a;
    const char* bb = *(const char**)b;
    int result = strcmp(aa, bb);
    return result;
}

void Free_Global_File_Data(int num_files) {
     for (int i = 0; i < num_files; i++) {
         free(global_file_data->filenames[i]);
     }
     free(global_file_data->filenames);
     free(global_file_data->file_processed_flags_array);
     free(global_file_data);
}

#include <ctype.h>

char* trimRight(char* s) {
    //Safeguard against empty strings
    int len = strlen(s);
    if(len == 0) {
        return s;
    }
    //Actual algorithm
    char* pos = s + len - 1;
    while(pos >= s && isspace(*pos)) {
        *pos = '\0';
        pos--;
    }
    return s;
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
    processed_files = 0;

    // malloc and init global_file_data filenames and flags
    global_file_data = (struct file_data*) malloc(sizeof(struct file_data)); 
    global_file_data->filenames = (char**) malloc(sizeof(char*) * num_files);
    global_file_data->file_processed_flags_array = malloc(sizeof(int) * num_files);
    for(int i = 0; i < num_files; i++) {
    //    char *filename = trimRight(argv[i + 1]);
        global_file_data->filenames[i] = strdup(argv[i + 1]);
        global_file_data->file_processed_flags_array[i] = 0;
    }

    if (num_files < num_mappers) {
        num_mappers = num_files;
    }

    pthread_t *map_threads = (pthread_t *) malloc(sizeof(pthread_t) * num_mappers);
    int threadCounter = 1;
    for (int i  = 0; i < num_mappers; i++) {
        threadCounter++;
        pthread_create(&map_threads[i], NULL, (void *) Thread_Map, (void *) map); 
    }

    for (int i = 0; i < num_mappers; i++) {
        pthread_join(map_threads[i], NULL);
    }
    
    for (int i = 0; i < num_partitions; i++) {
        qsort((void*)partition_list->elements[i]->list_of_words, partition_list->elements[i]->num_words, sizeof(char *), cmpstr); 
    }
    
    pthread_t *reduce_threads = (pthread_t *) malloc(sizeof(pthread_t) * num_reducers);
    reduce_function = reduce;
    for (int i  = 0; i < num_reducers; i++) {
        int *curr_partition_num = (int *) malloc(sizeof(int));
        *curr_partition_num = i;
        pthread_create(&reduce_threads[i], NULL, (void *) Thread_Reduce, (void *) curr_partition_num); 
    }

    for (int i = 0; i < num_reducers; i++) {
       pthread_join(reduce_threads[i], NULL);
    }
   
    // Free Memory 
/*    for(int i = 0; i < num_mappers; i++) {
        free(map_threads[i]);
    }
    for (int i = 0; i < num_reducers; i++) {
        free(reduce_threads[i]);
    }*/
    Free_Global_File_Data(num_files);
    free(map_threads);
    free(reduce_threads); 
    Free_Partition_List();       
}
