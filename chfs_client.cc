// chfs client.  implements FS operations using extent server
#include "chfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

chfs_client::chfs_client(std::string extent_dst)
{
    ec = new extent_client(extent_dst);
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

chfs_client::inum
chfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
chfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
chfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

bool
chfs_client::issymlink(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_SYMLINK) {
        printf("issymlink: %lld is a symlink\n", inum);
        return true;
    } 
    return false;
}

bool
chfs_client::isdir(inum inum)
{
    // Oops! is this still correct when you implement symlink?
    
    return ! (isfile(inum) || issymlink(inum));
}

extent_protocol::types
chfs_client::gettype(inum inum) {
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return extent_protocol::T_UNDEFINED;
    }

    return (extent_protocol::types)a.type;
}

int
chfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
chfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int
chfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    std::string buf;
    if (ec->get(ino, buf) != extent_protocol::OK) {
        return IOERR;
    }

    if (buf.length() != size) {
        buf.resize(size);

        if (ec->put(ino, buf) != extent_protocol::OK) {
            return IOERR;
        }
    }

    return r;
}

int
chfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    printf("[CC]\t[INFO] create %s at %llu, filename length: %lu\n",name, parent, strlen(name));
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    bool found = false;
    inum existed_ino;

    if (lookup(parent, name, found, existed_ino) != OK) {
        return IOERR;
    }
    if (found) {
        return EXIST;
    }

    if (ec->create(extent_protocol::T_FILE, ino_out) != extent_protocol::OK) {
        return IOERR;
    }

    std::string buf;
    if (ec->get(parent, buf) != extent_protocol::OK) {
        return IOERR;
    }

    buf.append(name);
    buf.append("/");
    buf.append(std::to_string(ino_out));
    buf.append("/");

    if (ec->put(parent, buf) != extent_protocol::OK) {
        return IOERR;
    }

    return r;
}

int
chfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    printf("[CC]\t[INFO] make dir %s at %llu, filename length: %lu\n",name, parent, strlen(name));
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    bool found = false;
    inum existed_ino;

    if (lookup(parent, name, found, existed_ino) != OK) {
        return IOERR;
    }
    if (found) {
        return EXIST;
    }

    if (ec->create(extent_protocol::T_DIR, ino_out) != extent_protocol::OK) {
        return IOERR;
    }

    std::string buf;
    if (ec->get(parent, buf) != extent_protocol::OK) {
        return IOERR;
    }
    
    buf.append(name);
    buf.append("/");
    buf.append(std::to_string(ino_out));
    buf.append("/");

    if (ec->put(parent, buf) != extent_protocol::OK) {
        return IOERR;
    }

    // buf.clear();
    // buf.append("./");
    // buf.append(std::to_string(parent));
    // buf.append("/../");
    // buf.append(std::to_string(ino_out));
    // buf.append("/");

    // if (ec->put(ino_out, buf) != extent_protocol::OK) {
    //     return IOERR;
    // }

    return r;
}

int
chfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    printf("[CC]\t[INFO] lookup dir %llu for %s\n", parent, name);
    int r = OK;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */

    std::string buf;
    found = false;
    if (ec->get(parent, buf) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    while (buf.length() > 0) {
        size_t pos = buf.find('/');
        std::string dir_name = buf.substr(0, pos);
        buf.erase(0, pos + 1);

        pos = buf.find('/');

        if (dir_name.compare(name) == 0) {
            ino_out = n2i(buf.substr(0, pos));
            found = true;
            return OK;
        }

        buf.erase(0, pos + 1);
    }

release:
    return r;
}

int
chfs_client::readdir(inum dir, std::list<dirent> &list)
{
    printf("[CC]\t[INFO] read dir %llu\n", dir);
    int r = OK;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */

    std::string buf;
    if (ec->get(dir, buf) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    while (buf.length() > 0) {
        size_t pos = buf.find('/');
        dirent dir;
        dir.name = buf.substr(0, pos);
        buf.erase(0, pos + 1);

        pos = buf.find('/');
        dir.inum = n2i(buf.substr(0, pos));
        list.push_back(dir);

        buf.erase(0, pos + 1);
    }

release:
    return r;
}

int
chfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;
    /*
     * your code goes here.
     * note: read using ec->get().
     */

    printf("read %016llx\n", ino);
    extent_protocol::attr a;
    std::string buf;
    if (ec->getattr(ino, a) != extent_protocol::OK || off > a.size) {
        r = IOERR;
        goto release;
    }
    printf("getfile %016llx -> sz %u\n", ino, a.size);

    if (ec->get(ino, buf) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    data = buf.substr(off, size);

release:
    return r;
}

int
chfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;

    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */

    printf("write %016llx\n", ino);
    bytes_written = 0;
    extent_protocol::attr a;
    std::string buf;
    std::string rewrite_data(data, size);
    if (ec->getattr(ino, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    printf("writefile %016llx -> sz %u\n", ino, a.size);

    if (ec->get(ino, buf) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    if (off > a.size) {
        buf.resize(off + size, '\0');
    }

    buf.replace(off, size, rewrite_data);


    if (ec->put(ino, buf) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    bytes_written = size;

release:
    return r;
}

int chfs_client::unlink(inum parent,const char *name)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */

    std::string buf;
    std::string writing;
    if (ec->get(parent, buf) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    while (buf.length() > 0) {
        size_t pos = buf.find('/');
        std::string dir_name = buf.substr(0, pos);
        buf.erase(0, pos + 1);

        pos = buf.find('/');

        if (dir_name.compare(name) != 0) {
            writing.append(dir_name);
            writing.append("/");
            writing.append(buf.substr(0, pos));
            writing.append("/");
        }

        buf.erase(0, pos + 1);
    }

    if (ec->put(parent, writing) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

release:
    return r;
}

int
chfs_client::symlink(const char *real_path, inum parent, const char * name, inum &ino_out)
{
    bool found = false;
    inum existed_ino;

    if (lookup(parent, name, found, existed_ino) != OK) {
        return IOERR;
    }
    if (found) {
        return EXIST;
    }

    if (ec->create(extent_protocol::T_SYMLINK, ino_out) != extent_protocol::OK) {
        return IOERR;
    }

    if (ec->put(ino_out, std::string(real_path)) != extent_protocol::OK) {
        return IOERR;
    }

    std::string buf;
    if (ec->get(parent, buf) != extent_protocol::OK) {
        return IOERR;
    }
    
    buf.append(name);
    buf.append("/");
    buf.append(std::to_string(ino_out));
    buf.append("/");

    if (ec->put(parent, buf) != extent_protocol::OK) {
        return IOERR;
    }

    return OK;
}

int
chfs_client::readlink(inum ino, std::string &real_path)
{
    if (ec->get(ino, real_path) != extent_protocol::OK) {
        return IOERR;
    }

    return OK;
}

