#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
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


int HT_CreateFile(char *fileName,  int buckets){
  // Δημιουργουμε το αρχειο , δημιουργουμε το μηδενικο μπλοκ του και το αρχικοποιουμε
  if(buckets > MAX_BUCKETS) return -1;

  CALL_OR_DIE(BF_CreateFile(fileName));
  int fptr;
  
  CALL_OR_DIE(BF_OpenFile(fileName,&fptr));
  
  BF_Block* zero_block;
  BF_Block_Init(&zero_block);
  
  CALL_OR_DIE(BF_AllocateBlock(fptr,zero_block));
  
  HT_info info;
  info.fptr = fptr;
  info.num_of_records = 0;
  info.num_of_records_per_block = (BF_BLOCK_SIZE - sizeof(HT_block_info))/sizeof(Record); // poses egrafes xwraei to kathe block
  info.num_of_records = 0;
  info.num_of_buckets = buckets;
  info.num_of_blocks = 1;

  BF_Block* bucket_block;
  BF_Block_Init(&bucket_block);
  
  // για ολους τους κουβαδες αρχικοποιουμε τα πρωτα μπλοκ τους και τα δεδομενα τους
  for(int i = 0 ; i < buckets; i++) {
    info.block_of_bucket[i] = info.last_insert[i] = i+1;
    HT_block_info block_info;
    block_info.num_of_records = 0;
    block_info.next_block_id = 0;
    CALL_OR_DIE(BF_AllocateBlock(fptr,bucket_block));
    info.num_of_blocks++;
    char* data;
    data = BF_Block_GetData(bucket_block);
    int offset_of_info = BF_BLOCK_SIZE - sizeof(HT_block_info);
    memcpy(data+offset_of_info,&block_info,sizeof(block_info));
    BF_Block_SetDirty(bucket_block);
    BF_UnpinBlock(bucket_block);
  }
  BF_Block_Destroy(&bucket_block);

  // Ανανεωνουμε τα δεδομενα του μηδενικου μπλοκ
  char* data;
  data = BF_Block_GetData(zero_block); 
  memcpy(data,&info,sizeof(HT_info));
  HT_info return_info;
  BF_Block_SetDirty(zero_block);
  CALL_OR_DIE(BF_UnpinBlock(zero_block));
  BF_Block_Destroy(&zero_block);
  

  CALL_OR_DIE(BF_CloseFile(fptr));
  return 0;
}

// Ανοιγουμε το αρχειο 
HT_info* HT_OpenFile(char *fileName){
  int fptr;
  HT_info *return_info = malloc(sizeof(HT_info));
  
  BF_Block *block_0;
  BF_Block_Init(&block_0);

  CALL_OR_DIE(BF_OpenFile(fileName,&fptr));

  CALL_OR_DIE(BF_GetBlock(fptr,0,block_0));

  memcpy(return_info,BF_Block_GetData(block_0),sizeof(HT_info)); 
  return_info->fptr = fptr;
  
  BF_UnpinBlock(block_0);
  
  BF_Block_Destroy(&block_0);
  return return_info;
}

// Κλεινουμε το αρχειο και αποδεσμευουμε την μνημη
int HT_CloseFile( HT_info* HT_info ){
  int fptr = HT_info->fptr;
  free(HT_info);    
  CALL_OR_DIE(BF_CloseFile(fptr));
  return 0;
}


int HT_InsertEntry(HT_info* ht_info, Record record){
  
  // Hassαρουμε το id
  int hash_value = record.id%ht_info->num_of_buckets;
  
  // Βρισκουμε το τελευταιο μπλοκ του κουβα
  int block_of_last_insert = ht_info->last_insert[hash_value];
  
  BF_Block *block;
  BF_Block_Init(&block);

  // Παιρνουμε τα δεδομενα του τελευταιου μπλοκ
  BF_GetBlock(ht_info->fptr,block_of_last_insert,block);
  
  char* data;
  data = BF_Block_GetData(block);
  
  HT_block_info block_info;
  int offset_of_info = BF_BLOCK_SIZE - sizeof(HT_block_info);
  memcpy(&block_info,data + offset_of_info, sizeof(block_info));
  
  // Αν το μπλοκ ειναι γεματο τοτε κανουμε καινουργιο και αποθηκευουμε εκει μεσα την εγγραφη μας
  if(block_info.num_of_records == ht_info->num_of_records_per_block) {
    
    BF_Block *new_block;
    BF_Block_Init(&new_block);
    BF_AllocateBlock(ht_info->fptr,new_block);
    ht_info->num_of_blocks++;

    int new_block_value;
    BF_GetBlockCounter(ht_info->fptr,&new_block_value);
    ht_info->last_insert[hash_value] = --new_block_value;
    
    HT_block_info new_block_info;
    new_block_info.num_of_records = 1;
    new_block_info.next_block_id = 0;
    
    char* data_new;
    data_new = BF_Block_GetData(new_block);
    memcpy(data_new + offset_of_info,&new_block_info,sizeof(HT_block_info));
    
    memcpy(data_new,&record,sizeof(Record));
    
    block_info.next_block_id = new_block_value;
    
    BF_Block_SetDirty(new_block);
    BF_UnpinBlock(new_block);
    BF_Block_Destroy(&new_block);
  } else {
    // Αποθηκευουμε την εγγραφη στο τελευταιο μπλοκ
    memcpy(data + (block_info.num_of_records++)*sizeof(Record),&record,sizeof(Record));
  }
  
  // +1 εγγραφή
  ht_info->num_of_records++;

  // Ανανεωνει τα δεδομενα του μπλοκ της προ τελευταιας εισαγωγης
  memcpy(data + offset_of_info,&block_info,sizeof(block_info));
  
  BF_Block_SetDirty(block);
  BF_UnpinBlock(block);

  // Ανανεωνουμε τα δεδομενα του μηδενικου μπλοκ
  BF_Block* zero_block;
  BF_Block_Init(&zero_block);
  BF_GetBlock(ht_info->fptr,0,zero_block);
  
  char* zero_data = BF_Block_GetData(zero_block);
 
  memcpy(zero_data,ht_info,sizeof(*ht_info));
  
  BF_Block_SetDirty(zero_block);
  BF_UnpinBlock(zero_block);
  return ht_info->last_insert[hash_value];
}

int HT_GetAllEntries(HT_info* ht_info, void *value ){

  // Hassαρουμε και βρισκουμε το πρωτο μπλοκ του κουβα
  int hash_value = *(int*)(value)%ht_info->num_of_buckets;
  int block_of_bucket = ht_info->block_of_bucket[hash_value];
  
  int num_of_block = 0; // τα μπλοκ που διαβασαμε
  BF_Block *block;
  BF_Block_Init(&block);
  
  // Παίρνουμε το πρώτο μπλοκ του κουβά
  BF_GetBlock(ht_info->fptr,block_of_bucket,block);
  char* data;
  data = BF_Block_GetData(block);

  // Παίρνουμε τις πληροφορίες του πρώτου μπλοκ του κουβά
  HT_block_info block_info;
  int offset_of_info = BF_BLOCK_SIZE - sizeof(HT_block_info);
  memcpy(&block_info,data + offset_of_info, sizeof(HT_block_info));

  // Όσο στο μπλοκ υπάρχουν εγγραφές
  while (block_info.num_of_records != 0) {

    num_of_block++; // καινούριο μπλοκ
    
    // Για όλες τις εγγραφές του μπλοκ αν ισχύει η συνθήκη τις printaroyme
    for(int i = 0; i<block_info.num_of_records;i++) {
      Record record;
      int offset = i * sizeof(Record);
      memcpy(&record,data + offset,sizeof(Record));
      if(record.id == *(int*)value) {
        printRecord(record);
      }
    }

    BF_UnpinBlock(block);

    // Αν δεν υπάρχει επόμενο μπλοκ σταματάμε
    if (block_info.next_block_id == 0) break;

    // Παιρνουμε τα δεδομενα του επομενου μπλοκ της αλυσιδας
    BF_GetBlock(ht_info->fptr,block_info.next_block_id,block);
    data = BF_Block_GetData(block); 
    memcpy(&block_info,data + offset_of_info,sizeof(block_info));
  }
  BF_UnpinBlock(block);
  return num_of_block;
}

// Εκτυπωνει τα στατιστικα για ενα HT αρχειο
int HT_HashStatistics(char* filename) {
    printf("Statistics for ht file: %s\n",filename);
    HT_info *info;
    info = HT_OpenFile(filename);
    
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
      
      HT_block_info block_info;
      int offset_of_info = BF_BLOCK_SIZE - sizeof(HT_block_info);
      memcpy(&block_info,data + offset_of_info,sizeof(HT_block_info));
      BF_UnpinBlock(block);
      while (block_info.next_block_id != 0) {

        num_of_records_per_bucket[i] += block_info.num_of_records;
        blocks_per_buckets[i]++;
        BF_GetBlock(info->fptr,block_info.next_block_id,block);
        data = BF_Block_GetData(block);
        memcpy(&block_info,data + offset_of_info,sizeof(HT_block_info));   
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
      printf("For bucket %d, used %d blocks\n",i,blocks_per_buckets[i]);
    }
    free(num_of_records_per_bucket);
    free(blocks_per_buckets);
}




