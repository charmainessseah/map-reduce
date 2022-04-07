#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include "hashmap.h"
#include "pthread.h"

#define FNV_OFFSET 14695981039346656037UL
#define FNV_PRIME 1099511628211UL

pthread_rwlock_t rwlock = PTHREAD_RWLOCK_INITIALIZER;

HashMap* MapInit(void)
{
//    pthread_rwlock_wrlock(&rwlock);
//    printf("init lock\n");
    HashMap* hashmap = (HashMap*) malloc(sizeof(HashMap));
    hashmap->contents = (MapPair**) calloc(MAP_INIT_CAPACITY, sizeof(MapPair*));
    hashmap->capacity = MAP_INIT_CAPACITY;
    hashmap->size = 0;
//    printf("init unlock\n");
//    pthread_rwlock_unlock(&rwlock);
    return hashmap;
}

void MapPut(HashMap* hashmap, char* key, void* value, int value_size)
{
    pthread_rwlock_wrlock(&rwlock);
    if (hashmap->size > (hashmap->capacity / 2)) {
        if (resize_map(hashmap) < 0) { // resize has a lock
            pthread_rwlock_unlock(&rwlock);
	    exit(0);
        }
    }

   // pthread_rwlock_wrlock(&rwlock);
   // printf("map put lock\n");

    MapPair* newpair = (MapPair*) malloc(sizeof(MapPair));
    int h;

    newpair->key = strdup(key);
    newpair->value = (void *)malloc(value_size);
    memcpy(newpair->value, value, value_size);

   // printf("map put unlock\n");
   pthread_rwlock_unlock(&rwlock);

    h = Hash(key, hashmap->capacity); // hash has a lock

    pthread_rwlock_wrlock(&rwlock);
   // printf("map put lock\n");

    while (hashmap->contents[h] != NULL) {
	// if keys are equal, update
        if (!strcmp(key, hashmap->contents[h]->key)) {
            free(hashmap->contents[h]);
            hashmap->contents[h] = newpair;
           // printf("map put unlock\n");
            pthread_rwlock_unlock(&rwlock);
            return;
        }
        h++;
        if (h == hashmap->capacity)
            h = 0;
    }

    // key not found in hashmap, h is an empty slot
    hashmap->contents[h] = newpair;
    hashmap->size += 1;
   // printf("map put unlock\n");
    pthread_rwlock_unlock(&rwlock);
}

char* MapGet(HashMap* hashmap, char* key)
{
    //pthread_rwlock_rdlock(&rwlock);
    int h = Hash(key, hashmap->capacity); // hash has a lock

    //printf("map get lock\n");
    pthread_rwlock_rdlock(&rwlock);

    while (hashmap->contents[h] != NULL) {
       // printf("here1\n");
        if (!strcmp(key, hashmap->contents[h]->key)) {
           // printf("map get unlock\n");
            pthread_rwlock_unlock(&rwlock);
            return hashmap->contents[h]->value;
        }
       // printf("here2\n");
        h++;
      //  printf("here3\n");
        if (h == hashmap->capacity) {
         //   printf("here4\n");
            h = 0;
        }
       // printf("here5\n");
    }
   // printf("map get unlock\n");
    pthread_rwlock_unlock(&rwlock);
    return NULL;
}

size_t MapSize(HashMap* map)
{
    return map->size;
}

// fix locks here
int resize_map(HashMap* map)
{
  //  pthread_rwlock_wrlock(&rwlock);
  //  printf("resize map lock\n");
    MapPair** temp;
    size_t newcapacity = map->capacity * 2; // double the capacity

    // allocate a new hashmap table
    temp = (MapPair**) calloc(newcapacity, sizeof(MapPair*));
    if (temp == NULL) {
        printf("Malloc error! %s\n", strerror(errno));
     //   printf("resize map unlock\n");
    //    pthread_rwlock_unlock(&rwlock);
        return -1;
    }

    size_t i;
    int h;
    MapPair* entry;
    // rehash all the old entries to fit the new table
    for (i = 0; i < map->capacity; i++) {
        if (map->contents[i] != NULL)
            entry = map->contents[i];
        else 
            continue;
        
      //  printf("resize map unlock\n");
      //  pthread_rwlock_unlock(&rwlock);

        h = Hash(entry->key, newcapacity); // we need to unlock before hash and acquire lock again after it returns

      //  printf("resize map lock\n");
      //  pthread_rwlock_wrlock(&rwlock);

        while (temp[h] != NULL) {
            h++;
            if (h == newcapacity)
            h = 0;
        }
        temp[h] = entry;
    }

    // free the old table
    free(map->contents);
    // update contents with the new table, increase hashmap capacity
    map->contents = temp;
    map->capacity = newcapacity;
   // printf("resize map unlock\n");
   // pthread_rwlock_unlock(&rwlock);
    return 0;
}

// FNV-1a hashing algorithm
// https://en.wikipedia.org/wiki/Fowler-Noll-Vo_hash_function#FNV-1a_hash
size_t Hash(char* key, size_t capacity) {
   // pthread_rwlock_wrlock(&rwlock);
   // printf("hash lock\n");
    size_t hash = FNV_OFFSET;
    for (const char *p = key; *p; p++) {
        hash ^= (size_t)(unsigned char)(*p);
        hash *= FNV_PRIME;
        hash ^= (size_t)(*p);
    }
  //  printf("hash unlock\n");
    //pthread_rwlock_unlock(&rwlock);
    return (hash % capacity);
}
