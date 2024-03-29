#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "sht_table.h"
#include "ht_table.h"
#include "record.h"

#define CALL_OR_DIE(call)     \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK) {      \
      BF_PrintError(code);    \
      exit(code);             \
    }                         \
  }

int hash_string(char*);
// φτιαχνει το αρχειο και αρχικοποιει τα δεδομενα του
int SHT_CreateSecondaryIndex(char *sfileName,  int buckets, char* fileName){
  
  if(buckets > MAX_BUCKETS) return -1;

  CALL_OR_DIE(BF_CreateFile(sfileName));
  int fptr;
  
  CALL_OR_DIE(BF_OpenFile(sfileName,&fptr));
  
  BF_Block* block;
  BF_Block_Init(&block);
  
  CALL_OR_DIE(BF_AllocateBlock(fptr,block));
  
  SHT_info info;
  info.fptr = fptr;
  info.num_of_records = 0;
  info.num_of_blocks = 1;
  info.num_of_records_per_block = (BF_BLOCK_SIZE - sizeof(SHT_block_info))/sizeof(tuple); // poses egrafes xwraei to kathe block
  info.num_of_buckets = buckets;

  BF_Block* bucket_block;
  BF_Block_Init(&bucket_block);

  for(int i = 0 ; i < buckets; i++) {
    info.last_insert[i] = info.block_of_bucket[i] = i+1;
    SHT_block_info block_info;
    block_info.num_of_records = 0;
    block_info.next_block_id = 0;
    CALL_OR_DIE(BF_AllocateBlock(fptr,bucket_block));
    char* data;
    data = BF_Block_GetData(bucket_block);
    int offset_of_info = BF_BLOCK_SIZE - sizeof(SHT_block_info);
    memcpy(data+offset_of_info,&block_info,sizeof(SHT_block_info));
    BF_Block_SetDirty(bucket_block);
    BF_UnpinBlock(bucket_block);
  }
  BF_Block_Destroy(&bucket_block);

  char* data;
  data = BF_Block_GetData(block); 
  memcpy(data,&info,sizeof(SHT_info));
  SHT_info return_info;
  
  BF_Block_SetDirty(block);
  CALL_OR_DIE(BF_UnpinBlock(block));
  BF_Block_Destroy(&block);

  CALL_OR_DIE(BF_CloseFile(fptr));
  return 0;
}

// Ανοιγει το αρχειο
SHT_info* SHT_OpenSecondaryIndex(char *indexName){
    int fptr;
    SHT_info *return_info = malloc(sizeof(SHT_info));
    BF_Block *block_0;
    BF_Block_Init(&block_0);

    CALL_OR_DIE(BF_OpenFile(indexName,&fptr));
    CALL_OR_DIE(BF_GetBlock(fptr,0,block_0));
    memcpy(return_info,BF_Block_GetData(block_0),sizeof(SHT_info));
    return_info->fptr = fptr;
    BF_UnpinBlock(block_0);
    BF_Block_Destroy(&block_0);
    return return_info;
}

// Κλεινει το αρχειο και αποδεσμευει την μνημη
int SHT_CloseSecondaryIndex( SHT_info* SHT_info ){
  int fptr = SHT_info->fptr;
  free(SHT_info);
  CALL_OR_DIE(BF_CloseFile(fptr));
  return 0;
}


int SHT_SecondaryInsertEntry(SHT_info* sht_info, Record record, int block_id){

  // Hassαρει το ονομα και βρισκει το τελευταιο block του κουβα  
  int hash_value = hash_string(record.name)%sht_info->num_of_buckets;
  
  int block_of_last_insert = sht_info->last_insert[hash_value];
  

  BF_Block *block;
  BF_Block_Init(&block);

  BF_GetBlock(sht_info->fptr,block_of_last_insert,block);
  
  char* data;
  data = BF_Block_GetData(block);
  
  // Παιρνουμε τα δεδομενα του μπλοκ
  SHT_block_info block_info;
  int offset_of_info = BF_BLOCK_SIZE - sizeof(SHT_block_info);
  memcpy(&block_info,data + offset_of_info, sizeof(block_info));
  
  // Αν το μπλοκ ειναι γεματο τοτε δημιουργουμε καινουργιο μπλοκ και εισαγουμε το tuple εκει
  if(block_info.num_of_records == sht_info->num_of_records_per_block) {
    BF_Block *new_block;
    BF_Block_Init(&new_block);
    BF_AllocateBlock(sht_info->fptr,new_block);
    sht_info->num_of_blocks++;
   
    int new_block_value;
    BF_GetBlockCounter(sht_info->fptr,&new_block_value);
    sht_info->last_insert[hash_value] = --new_block_value;

    SHT_block_info new_block_info;
    new_block_info.num_of_records = 1;
    
    char* data;
    data = BF_Block_GetData(new_block);
    memcpy(data + offset_of_info,&new_block_info,sizeof(SHT_block_info));

    tuple t;
    t.block_id = block_id;
    strcpy(t.key,record.name);
    memcpy(data,&t,sizeof(tuple));

    block_info.next_block_id = new_block_value;


    BF_Block_SetDirty(new_block);
    BF_UnpinBlock(new_block);
    BF_Block_Destroy(&new_block);
    
  } else {
    // Εισαγουμε το tuple στο μπλοκ
    tuple t;
    t.block_id = block_id;
    strcpy(t.key,record.name);
    memcpy(data + (block_info.num_of_records++)*sizeof(tuple),&t,sizeof(tuple));
  }

  // +1 εγγραφή
  sht_info->num_of_records++;

  // Ανανεωνουμε τα δεδομενα του μπλοκ της προ τελευταιας εγγραφης
  memcpy(data + offset_of_info,&block_info,sizeof(block_info));
 
  BF_Block_SetDirty(block);
  BF_UnpinBlock(block);

  // Ανανεωνουμε τα δεδομενα του μηδενικου μπλοκ
  BF_Block* zero_block;
  BF_Block_Init(&zero_block);
  BF_GetBlock(sht_info->fptr,0,zero_block);
  data = BF_Block_GetData(zero_block);
 
  memcpy(data,sht_info,sizeof(SHT_info));
  
  BF_Block_SetDirty(zero_block);
  BF_UnpinBlock(zero_block);
  
  // Επιστρεφουμε το μπλοκ εισαγωγης
  return sht_info->last_insert[hash_value];
  
}


int SHT_SecondaryGetAllEntries(HT_info* ht_info, SHT_info* sht_info, char* name) {
  
  // Hassαρουμε και βρισκουμε το πρωτο μπλοκ του κουβα
  int hash_value = hash_string(name)%sht_info->num_of_buckets;
  int block_of_bucket = sht_info->block_of_bucket[hash_value];
  
  int num_of_block = 0;
  BF_Block *block;
  BF_Block_Init(&block);
  BF_GetBlock(sht_info->fptr,block_of_bucket,block);
  
  char* data;
  data = BF_Block_GetData(block);

  int readen_bucket_records = 0;
  num_of_block++;

  SHT_block_info sht_block_info;
  int offset_of_info = BF_BLOCK_SIZE - sizeof(SHT_block_info);
  memcpy(&sht_block_info,data + offset_of_info, sizeof(sht_block_info));

  // Οσο υπαρχουν εγγραφες στο μπλοκ
  while (sht_block_info.num_of_records != 0) {

    // διαβαζουμε καινουργιο μπλοκ
    num_of_block++;

    // για ολες τις εγγραφες του μπλοκ
    for(int i = 0; i<sht_block_info.num_of_records;i++) {

      // Βρισκουμε το block id τις εγγραφης
      tuple t;
      memcpy(&t,data + i*sizeof(tuple),sizeof(tuple));
      BF_Block *ht_block;
      BF_Block_Init(&ht_block);
      BF_GetBlock(ht_info->fptr,t.block_id,ht_block);
      char* ht_data = BF_Block_GetData(ht_block);
      HT_block_info ht_block_info;
      int offset_of_info = BF_BLOCK_SIZE - sizeof(HT_block_info);
      memcpy(&ht_block_info,ht_data + offset_of_info, sizeof(ht_block_info));
      
      // Ψαχνουμε στο HT αρχειο ολες τις εγγραφες του μπλοκ 
      // και εκτυπωνουμε ολες τις εγγραφες που πληρουν τις προυποθεσεις
      for(int j = 0; j<ht_block_info.num_of_records;j++) {
        Record record;
        int offset = j * sizeof(record);
        memcpy(&record,ht_data + offset,sizeof(record));
        if(!strcmp(record.name,name)) {
          printRecord(record);
        }
      }
    }
    BF_UnpinBlock(block);

    // Αν δεν υπάρχει επόμενο μπλοκ σταματάμε
    if (sht_block_info.next_block_id == 0) break;

    // Παιρνουμε τα δεδομενα του επομενου μπλοκ
    BF_GetBlock(sht_info->fptr,sht_block_info.next_block_id,block);
    data = BF_Block_GetData(block);
    memcpy(&sht_block_info,data + offset_of_info, sizeof(sht_block_info));

  }

  BF_UnpinBlock(block);
  return 0;
}

// κάνει casting τον καθε χαρακτηρα του ονοματος σε int ,τα αθροιζει και τα επιστρεφει
int hash_string(char* string) {
  int sum = 0;
  for(int i = 0; i < strlen(string); i++) {
    sum += (int)string[i];
  }
  return sum;
}


int SHT_HashStatistics(char* filename) {
    printf("Statistics for sht file: %s\n",filename);
    SHT_info *info;
    info = SHT_OpenSecondaryIndex(filename);
    
    printf("\n\nNumber of blocks in file %s %d\n",filename,info->num_of_blocks);
    int *num_of_records_per_bucket = malloc(sizeof(int)*info->num_of_buckets);
    int *blocks_per_buckets = malloc(sizeof(int)*info->num_of_buckets);
    int min_records;
    int max_records;
    int total_records = 0;
    for(int i = 0;i< info->num_of_buckets; i ++) {
      
      num_of_records_per_bucket[i] = 0;
      blocks_per_buckets[i] = 0;
      int num_of_bucket = info->block_of_bucket[i];
      BF_Block *block;
      BF_Block_Init(&block);

      BF_GetBlock(info->fptr,num_of_bucket,block);
     
      char* data = BF_Block_GetData(block);
      
      SHT_block_info block_info;
      int offset_of_info = BF_BLOCK_SIZE - sizeof(SHT_block_info);
      memcpy(&block_info,data + offset_of_info,sizeof(SHT_block_info));
      BF_UnpinBlock(block);
      while (block_info.next_block_id != 0) {

        num_of_records_per_bucket[i] += block_info.num_of_records;
        blocks_per_buckets[i]++;
        BF_GetBlock(info->fptr,block_info.next_block_id,block);
        data = BF_Block_GetData(block);
        memcpy(&block_info,data + offset_of_info,sizeof(SHT_block_info));   
        BF_UnpinBlock(block);
      } 
      
      num_of_records_per_bucket[i] += block_info.num_of_records;

      if (i == 0) {
        min_records = num_of_records_per_bucket[i];
        max_records = num_of_records_per_bucket[i];
      } else {
        if (num_of_records_per_bucket[i] < min_records) min_records = num_of_records_per_bucket[i];
        if (num_of_records_per_bucket[i] > max_records) max_records = num_of_records_per_bucket[i];
      }
      
      total_records += num_of_records_per_bucket[i];
      
    }
    printf("Total records: %d\n",total_records);
    printf("Minimum records per bucket: %d\n",min_records);
    printf("Maximum records per bucket: %d\n",max_records);
    printf("Mean records per bucket: %d\n",total_records/info->num_of_buckets);
    printf("Mean buckets per records: %d\n",info->num_of_blocks/info->num_of_buckets);
    
    int more_one = 0;
    for(int i = 0; i<info->num_of_buckets;i++) {
      if(blocks_per_buckets[i] > 0) more_one++;
    }
    printf("Number of buckets who have more one block: %d\n",more_one);
    for(int i = 0; i < info->num_of_buckets;i++) {
      printf("For bucket %d, used %d blocks more\n",i,blocks_per_buckets[i]);
    }
    free(num_of_records_per_bucket);
    free(blocks_per_buckets);
}