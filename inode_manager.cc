#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
  if (id < BLOCK_NUM)
    memcpy(buf, blocks[id], BLOCK_SIZE);
  else {
    printf("[DISK]\t[ERROR] blockid %u out of range\n", id);
    exit(0);
  }
}

void
disk::write_block(blockid_t id, const char *buf)
{
  if (id < BLOCK_NUM)
    memcpy(blocks[id], buf, BLOCK_SIZE);
  else {
    printf("[DISK]\t[ERROR] blockid %u out of range\n", id);
    exit(0);
  }
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
  const uint32_t nblocks = sb.nblocks;
  for (uint32_t i = IBLOCK(INODE_NUM, BLOCK_NUM) + 1; i < nblocks; i++) {
    char bitmap[BLOCK_SIZE];
    read_block(BBLOCK(i), bitmap);
    uint32_t pos = i % BPB;
    char bitmap_byte = bitmap[pos/8];
    bool bit = bitmap_byte & (0x80 >> (pos % 8));
    if (!bit) {
      bitmap[pos/8] = bitmap_byte | (0x80 >> (pos % 8));
      write_block(BBLOCK(i), bitmap);
      return i;
    }
  }

  printf("[BM]\t[ERROR] cannot find a free block\n");
  exit(0);
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */

   if (id < IBLOCK(INODE_NUM, BLOCK_NUM) || id >= BLOCK_NUM) {
    printf("[BM]\t[ERROR] block id out of range\n");
    exit(0);
  }

  char bitmap[BLOCK_SIZE];
  read_block(BBLOCK(id), bitmap);
  uint32_t pos = id % BPB;
  char bitmap_byte = bitmap[pos/8];
  bitmap[pos/8] = bitmap_byte & (~(0x80 >> (pos % 8)));
  write_block(BBLOCK(id), bitmap);
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("[IM]\t[ERROR] alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
  for (uint32_t i = 1; i <= INODE_NUM; ++ i) {
    inode_t *ino = get_inode(i);
    if (ino == NULL) {
      ino = (inode_t*)malloc(sizeof(inode_t));
      ino->type = (short)type;
      ino->size = 0;
      unsigned int current_time = (unsigned int)time(NULL);
      ino->ctime = current_time;
      ino->mtime = current_time;
      ino->atime = current_time;
      put_inode(i, ino);
      free(ino);
      printf("[IM]\t[INFO] allocate type %d to inode %u\n", type, i);
      return i;
    } else {
      free(ino);
    }
  }
  printf("[IM]\t[ERROR] cannot find a free inode\n");
  exit(0);
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
  inode_t *ino = get_inode(inum);
  if (ino == NULL) {
    printf("[IM]\t[WARN] inode %d has already been freed\n", inum);
  } else {
    memset(ino, 0, sizeof(inode_t));
    put_inode(inum, ino);
    free(ino);
  }
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);

  if (inum < 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode*)buf + inum%IPB;
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_out
   */

  printf("[IM]\t[INFO] read file inode = %u\n", inum);
  inode_t *ino = get_inode(inum);
  if (ino == NULL) {
    printf("[IM]\t[ERROR] fail to read file: inode %d does not exist\n", inum);
    exit(0);
  }
  const int i_size = ino->size;
  unsigned int current_time = (unsigned int)time(NULL);
  ino->atime = current_time;
  put_inode(inum, ino);

  if (i_size == 0) {
    *size = 0;
    *buf_out = NULL;
    put_inode(inum, ino);
    free(ino);
    return;
  }

  *buf_out = (char *)malloc(i_size);
  *size = i_size;
  char *buf_pos = *buf_out;

  if (i_size <= NDIRECT*BLOCK_SIZE) {
    // the file does not use indirect blocks
    const uint32_t block_num = (i_size - 1) / BLOCK_SIZE + 1;

    for (uint32_t i = 0; i < block_num - 1; i++) {
      char block_buf[BLOCK_SIZE];
      bm->read_block(ino->blocks[i], block_buf);
      memcpy(buf_pos, block_buf, BLOCK_SIZE);
      buf_pos += BLOCK_SIZE;
    }

    char block_buf[BLOCK_SIZE];
    bm->read_block(ino->blocks[block_num - 1], block_buf);
    uint32_t remaining_size = i_size - (block_num - 1) * BLOCK_SIZE;
    memcpy(buf_pos, block_buf, remaining_size);

  } else {
    // copy all the direct blocks
    for (uint32_t i = 0; i < NDIRECT; i++) {
      char block_buf[BLOCK_SIZE];
      bm->read_block(ino->blocks[i], block_buf);
      memcpy(buf_pos, block_buf, BLOCK_SIZE);
      buf_pos += BLOCK_SIZE;
    }

    // get the indirect block
    uint indirect_block[NINDIRECT];
    bm->read_block(ino->blocks[NDIRECT], (char *)indirect_block);
    uint32_t remaining_size = i_size - (NDIRECT) * BLOCK_SIZE;
    const uint32_t indirect_block_num = (remaining_size - 1) / BLOCK_SIZE + 1;
    
    for (uint32_t i = 0; i < indirect_block_num - 1; i++) {
      char block_buf[BLOCK_SIZE];
      bm->read_block(indirect_block[i], block_buf);
      memcpy(buf_pos, block_buf, BLOCK_SIZE);
      buf_pos += BLOCK_SIZE;
    }

    char block_buf[BLOCK_SIZE];
    bm->read_block(indirect_block[indirect_block_num - 1], block_buf);
    remaining_size = i_size - (NDIRECT + indirect_block_num - 1) * BLOCK_SIZE;
    memcpy(buf_pos, block_buf, remaining_size);
  }

  free(ino);
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */

  printf("[IM]\t[INFO] write file inode = %u size = %u\n", inum, size);
  if (size > (int)MAXFILE * BLOCK_SIZE || size < 0) {
    printf("[IM]\t[ERROR] unpermitted file size: %d\n", size);
    exit(0);
  }

  inode_t *ino = get_inode(inum);
  if (ino == NULL) {
    printf("[IM]\t[ERROR] fail to read file: inode %d does not exist\n", inum);
    exit(0);
  }

  const uint32_t i_size = ino->size;
  
  // free all old blocks
   if (i_size <= NDIRECT*BLOCK_SIZE) {
    // the old file does not use indirect blocks
    const uint32_t block_num = i_size == 0 ? 0 : (i_size - 1) / BLOCK_SIZE + 1;

    for (uint32_t i = 0; i < block_num; i++) {
      bm->free_block(ino->blocks[i]);
    }

  } else {
    // free all the direct blocks
    for (uint32_t i = 0; i < NDIRECT; i++) {
      bm->free_block(ino->blocks[i]);
    }

    // get the indirect block
    uint indirect_block[NINDIRECT];
    bm->read_block(ino->blocks[NDIRECT], (char *)indirect_block);
    uint32_t remaining_size = i_size - (NDIRECT) * BLOCK_SIZE;
    const uint32_t indirect_block_num = remaining_size == 0 ? 0 : (remaining_size - 1) / BLOCK_SIZE + 1;
    
    // free all the indirect blocks
    for (uint32_t i = 0; i < indirect_block_num; i++) {
      bm->free_block(indirect_block[i]);
    }
  }

  ino->size = size;
  unsigned int current_time = (unsigned int)time(NULL);
  ino->atime = current_time;
  ino->mtime = current_time;
  ino->ctime = current_time;

  if (size == 0) {
    put_inode(inum, ino);
    free(ino);
    return;
  }

  char *buf_pos = (char *)buf;

  if (size <= NDIRECT * BLOCK_SIZE) {
    const uint32_t block_num = (size - 1) / BLOCK_SIZE + 1;

    for (uint32_t i = 0; i < block_num - 1; i++) {
      blockid_t block_id = bm->alloc_block();
      ino->blocks[i] = block_id;
      bm->write_block(block_id, buf_pos);
      buf_pos += BLOCK_SIZE;
    }

    blockid_t block_id = bm->alloc_block();
    ino->blocks[block_num - 1] = block_id;
    char block_buf[BLOCK_SIZE];
    uint32_t remaining_size = size - (block_num - 1) * BLOCK_SIZE;
    memcpy(block_buf, buf_pos, remaining_size);
    bm->write_block(block_id, block_buf);

  } else {
    for (uint32_t i = 0; i < NDIRECT; i++) {
      blockid_t block_id = bm->alloc_block();
      ino->blocks[i] = block_id;
      bm->write_block(block_id, buf_pos);
      buf_pos += BLOCK_SIZE;
    }

    uint indirect_block[NINDIRECT];
    uint32_t remaining_size = size - (NDIRECT) * BLOCK_SIZE;
    const uint32_t indirect_block_num = (remaining_size - 1) / BLOCK_SIZE + 1;
    
    for (uint32_t i = 0; i < indirect_block_num - 1; i++) {
      blockid_t block_id = bm->alloc_block();
      indirect_block[i] = block_id;
      bm->write_block(block_id, buf_pos);
      buf_pos += BLOCK_SIZE;
    }

    blockid_t block_id = bm->alloc_block();
    indirect_block[indirect_block_num - 1] = block_id;
    char block_buf[BLOCK_SIZE];
    remaining_size = size - (NDIRECT + indirect_block_num - 1) * BLOCK_SIZE;
    
    memcpy(block_buf, buf_pos, remaining_size);
    bm->write_block(block_id, block_buf);

    // alloc block for indirect block
    block_id = bm->alloc_block();
    bm->write_block(block_id, (char *)indirect_block);
    ino->blocks[NDIRECT] = block_id;
  }

  put_inode(inum, ino);
  free(ino);
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  inode_t *ino = get_inode(inum);
  if (ino == NULL) {
    // printf("\tim: error! fail to read file attr: inode %d does not exist\n", inum);
    // exit(0);
    a.type = 0;
    return;
  }
  a.type = ino->type;
  a.size = ino->size;
  a.atime = ino->atime;
  a.mtime = ino->mtime;
  a.ctime = ino->ctime;

  free(ino);
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
  
  inode_t *ino = get_inode(inum);
  if (ino == NULL) {
    printf("[IM]\t[ERROR] fail to remove file: inode %d does not exist\n", inum);
    exit(0);
  }

  const uint32_t i_size = ino->size;
  // free all old blocks
   if (i_size <= NDIRECT*BLOCK_SIZE) {
    // the old file does not use indirect blocks
    const uint32_t block_num = i_size == 0 ? 0 : (i_size - 1) / BLOCK_SIZE + 1;

    for (uint32_t i = 0; i < block_num; i++) {
      bm->free_block(ino->blocks[i]);
    }

  } else {
    // free all the direct blocks
    for (uint32_t i = 0; i < NDIRECT; i++) {
      bm->free_block(ino->blocks[i]);
    }

    // get the indirect block
    uint indirect_block[NINDIRECT];
    bm->read_block(ino->blocks[NDIRECT], (char *)indirect_block);
    uint32_t remaining_size = i_size - (NDIRECT) * BLOCK_SIZE;
    const uint32_t indirect_block_num = remaining_size == 0 ? 0 : (remaining_size - 1) / BLOCK_SIZE + 1;
    
    // free all the indirect blocks
    for (uint32_t i = 0; i < indirect_block_num; i++) {
      bm->free_block(indirect_block[i]);
    }
  }

  free_inode(inum);
  free(ino);
}
