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
    HashMap* hashmap = (HashMap*) malloc(sizeof(HashMap));
    hashmap->contents = (MapPair**) calloc(MAP_INIT_CAPACITY, sizeof(MapPair*));
    hashmap->capacity = MAP_INIT_CAPACITY;
    hashmap->size = 0;
    return hashmap;
}

void MapPut(HashMap* hashmap, char* key, void* value, int value_size)
{
    pthread_rwlock_wrlock(&rwlock);
    if (hashmap->size > (hashmap->capacity / 2)) {
        if (resize_map(hashmap) < 0) { 
            pthread_rwlock_unlock(&rwlock);
	    exit(0);
        }
    }

    MapPair* newpair = (MapPair*) malloc(sizeof(MapPair));
    int h;

    newpair->key = strdup(key);
    newpair->value = (void *)malloc(value_size);
    memcpy(newpair->value, value, value_size);

    h = Hash(key, hashmap->capacity);


    while (hashmap->contents[h] != NULL) {
        if (!strcmp(key, hashmap->contents[h]->key)) {
            free(hashmap->contents[h]);
            hashmap->contents[h] = newpair;
            pthread_rwlock_unlock(&rwlock);
            return;
        }
        h++;
        if (h == hashmap->capacity)
            h = 0;
    }

    hashmap->contents[h] = newpair;
    hashmap->size += 1;
    pthread_rwlock_unlock(&rwlock);
}

char* MapGet(HashMap* hashmap, char* key)
{
    pthread_rwlock_rdlock(&rwlock);
    int h = Hash(key, hashmap->capacity);

    while (hashmap->contents[h] != NULL) {
        if (!strcmp(key, hashmap->contents[h]->key)) {
            pthread_rwlock_unlock(&rwlock);
            return hashmap->contents[h]->value;
        }
        h++;
        if (h == hashmap->capacity) {
            h = 0;
        }
    }
    pthread_rwlock_unlock(&rwlock);
    return NULL;
}

size_t MapSize(HashMap* map)
{
    return map->size;
}

int resize_map(HashMap* map)
{
    MapPair** temp;
    size_t newcapacity = map->capacity * 2;

    temp = (MapPair**) calloc(newcapacity, sizeof(MapPair*));
    if (temp == NULL) {
        printf("Malloc error! %s\n", strerror(errno));
        return -1;
    }

    size_t i;
    int h;
    MapPair* entry;
    for (i = 0; i < map->capacity; i++) {
        if (map->contents[i] != NULL)
            entry = map->contents[i];
        else 
            continue;
        
        h = Hash(entry->key, newcapacity);

        while (temp[h] != NULL) {
            h++;
            if (h == newcapacity)
            h = 0;
        }
        temp[h] = entry;
    }

    free(map->contents);
    map->contents = temp;
    map->capacity = newcapacity;
    return 0;
}

// FNV-1a hashing algorithm
// https://en.wikipedia.org/wiki/Fowler-Noll-Vo_hash_function#FNV-1a_hash
size_t Hash(char* key, size_t capacity) {
    size_t hash = FNV_OFFSET;
    for (const char *p = key; *p; p++) {
        hash ^= (size_t)(unsigned char)(*p);
        hash *= FNV_PRIME;
        hash ^= (size_t)(*p);
    }
    return (hash % capacity);
}
