#include <cstdint>
#include <fcntl.h>
#include <sys/time.h>

#include "../hm/hm_manager.h"

const char* g_zns_dev = "/dev/nvme0n1";
constexpr size_t kZoneSize = 64*1024*1024;
constexpr size_t kBlockSize = 4096;
struct zbd_info g_zbd_info;
//zbd_zone * zone_info;
int g_zns_fd_dio;
int g_zns_fd;
int g_nr_zones;

namespace leveldb{
    static uint64_t get_now_micros(){
        struct timeval tv;
        gettimeofday(&tv, NULL);
        return (tv.tv_sec) * 1000000 + tv.tv_usec;
    }

    static size_t random_number(size_t size) {
        return rand() % size;
    }

    HMManager::HMManager(const Comparator *icmp)
        :icmp_(icmp) {
        ssize_t ret;

        //ret = zbc_open(smr_filename, O_RDWR, &dev_);  //Open device without O_DIRECT
        g_zns_fd =  zbd_open(g_zns_dev, O_RDWR, &dev_);
        g_zns_fd_dio = zbd_open(g_zns_dev, O_RDWR | O_DIRECT, nullptr);  //Open device with O_DIRECT; O_DIRECT means that Write directly to disk without cache
        
        if(g_zns_fd_dio == -1){
            perror("open dio dev");
            exit(1);
        }
        if(dev_.lblock_size != kBlockSize){
            fprintf(stderr,"block size is not %lu\n",kBlockSize);
            exit(1);
        }
        if(dev_.zone_size != kZoneSize){
            fprintf(stderr, "zone size is not %lu\n",kZoneSize);
            exit(1);
        }
        unsigned int nr_zones;
        if(zbd_list_zones(g_zns_fd_dio, 0, dev_.nr_zones * kZoneSize, ZBD_RO_ALL, &zone_, &nr_zones)){
            fprintf(stderr,"zbd list zones\n");
        }
        if(nr_zones  != dev_.nr_zones){
            fprintf(stderr, "zone count %du != %u\n",nr_zones, dev_.nr_zones);
        }
        g_nr_zones = nr_zones;
        zonenum_ = g_nr_zones;
        int rc = zbd_reset_zones(g_zns_fd, 0 ,g_nr_zones*kZoneSize);    //reset all zone
        
        zbd_list_zones(g_zns_fd_dio, 0, dev_.nr_zones * kZoneSize, ZBD_RO_ALL, &zone_, &nr_zones);
        
        if(rc == -1){
            perror("zbd reset");
            exit(1);
        }
        //zone_ is zone_info
        //zonenum_ is g_nr_zones
        bitmap_ = new BitMap(zonenum_);
        first_zonenum_ = set_first_zonenum();

        init_log_file();
        MyLog("\n  !!geardb!!  \n");
        MyLog("COM_WINDOW_SEQ:%d Verify_Table:%d Read_Whole_Table:%d Find_Table_Old:%d\n",COM_WINDOW_SEQ,Verify_Table,Read_Whole_Table,Find_Table_Old);
        //printf("the first_zonenum_:%d zone_num:%ld\n",first_zonenum_,zonenum_);
        //////statistics
        delete_zone_num=0;
        all_table_size=0;
        kv_store_sector=0;
        kv_read_sector=0;
        max_zone_num=0;
        move_file_size=0;
        read_time=0;
        write_time=0;
        //////end
    }

    HMManager::~HMManager(){
        //get_all_info();

        std::map<uint64_t, struct Ldbfile*>::iterator it=table_map_.begin();
        while(it!=table_map_.end()){
            delete it->second;
            it=table_map_.erase(it);
        }
        table_map_.clear();
        int i;
        for(i=0;i<config::kNumLevels;i++){
            std::vector<struct Zonefile*>::iterator iz=zone_info_[i].begin();
            while(iz!=zone_info_[i].end()){
                delete (*iz);
                iz=zone_info_[i].erase(iz);
            }
            zone_info_[i].clear();
        }
        if(zone_) {
            free(zone_);
        }
        if(&dev_){
            zbd_close(g_zns_fd);
        }
        if(bitmap_){
            free(bitmap_);
        }

    }

    int HMManager::set_first_zonenum(){
        int i;
        for(i=0;i<zonenum_;i++){
            if(zone_[i].type==2){
                return i;
            }
        }
        return 0;
    }

    ssize_t HMManager::hm_alloc_zone(){ //?????????????
        //printf("\nhm alloc zone\n");
        ssize_t i;
        for(i=first_zonenum_;i<zonenum_;i++){  //Traverse from the first sequential write zone
            if(bitmap_->get(i)==0){
                unsigned int num = 1;
                //enum zbd_report_option ro = ZBD_RO_ALL;
                //struct zbd_zone *zone=(struct zbd_zone *)malloc(sizeof(struct zbd_zone));
                //zbd_report_zones(g_zns_fd, zone_[i].start, kZoneSize , ro , zone , &num);
                if(zone_[i].wp != zone_[i].start){
                    printf("alloc fail: zone:%ld wp:%ld start:%ld\n",i,zone_[i].wp,zone_[i].start);
                    zone_[i].flags = ZBD_OP_RESET;
                    zbd_reset_zones(g_zns_fd,zone_[i].start,kZoneSize);
                    zone_[i].wp=zone_[i].start;
                  //  if(zone) free(zone);
                    continue;
                }
                //if(zone) free(zone);
                bitmap_->set(i);
                zone_[i].flags = ZBD_OP_RESET;
                zbd_reset_zones(g_zns_fd,zone_[i].start,kZoneSize);
                zone_[i].wp=zone_[i].start;      
                return i;
            }
        }
        printf("hm_alloc_zone failed! no remain space\n");
        exit(1);
        return -1;
    }
    
    void HMManager::hm_free_zone(uint64_t zone){
        //printf("hm free\n");
        ssize_t ret;
        zone_[zone].flags = ZBD_OP_RESET;
       
        ret =zbd_reset_zones(g_zns_fd,zone_[zone].start,kZoneSize);
        //printf("reset zone %ld\n", zone);
        if(ret!=0){
            MyLog("reset zone:%ld faild! error:%ld\n",zone,ret);
        }
        bitmap_->clr(zone);
        zone_[zone].wp=zone_[zone].start;
    }

    ssize_t HMManager::hm_alloc(int level,uint64_t size){ // size is byte
        //printf("hm alloc\n");
        uint64_t need_size=(size/PHYSICAL_BLOCK_SIZE+1)*(PHYSICAL_BLOCK_SIZE/512); // sector
        uint64_t write_zone=0;
        if(zone_info_[level].empty()){
            write_zone=hm_alloc_zone();
            struct Zonefile* zf=new Zonefile(write_zone);
            zone_info_[level].push_back(zf);

            if(get_zone_num()>max_zone_num){
                max_zone_num=get_zone_num();
            }
            return 1;
        }
        write_zone=zone_info_[level][zone_info_[level].size()-1]->zone;
        if(((zone_[write_zone].len -(zone_[write_zone].wp-zone_[write_zone].start)) / 512) < need_size 
            || zone_[write_zone].flags == ZBD_OP_FINISH) {//The current written zone can't write ???
             
            //sync_file_range(g_zns_fd, write_zone * kZoneSize, kZoneSize,
              //     SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WRITE | SYNC_FILE_RANGE_WAIT_AFTER);
                    
            zbd_finish_zones(g_zns_fd,zone_[write_zone].start,kZoneSize);
            zone_[write_zone].flags = ZBD_OP_FINISH;
           // printf("finish zone %ld\n",write_zone);
            write_zone=hm_alloc_zone();
            struct Zonefile* zf=new Zonefile(write_zone);
            zone_info_[level].push_back(zf);
            if(get_zone_num()>max_zone_num){
                max_zone_num=get_zone_num();
            }
        }
        return 1;
    }

    ssize_t HMManager::hm_write(int level,uint64_t filenum,const void *buf,uint64_t count){
        //printf("\nhm write\n");
        hm_alloc(level,count);
        void *w_buf=NULL;
        uint64_t write_zone=zone_info_[level][zone_info_[level].size()-1]->zone;
        uint64_t sector_count;
        uint64_t sector_ofst=zone_[write_zone].wp / 512;
        ssize_t ret;

        uint64_t write_time_begin=get_now_micros();
        if(0){
            sector_count=count/512;
        }
        else{
            sector_count=(count/PHYSICAL_BLOCK_SIZE+1)*(PHYSICAL_BLOCK_SIZE/512);  //Align with physical block
            //w_buf=(void *)malloc(sector_count*512);
            ret=posix_memalign(&w_buf,MEMALIGN_SIZE,sector_count*512);
            if(ret!=0){
                printf("error:%ld posix_memalign falid!\n",ret);
                return -1;
            }
            memset(w_buf,0,sector_count*512);
            memcpy(w_buf,buf,count);
           // printf("zone is %ld ",write_zone);
            ret = pwrite(g_zns_fd_dio, buf, sector_count* 512, sector_ofst* 512);
            //ret=zbc_pwrite(dev_, w_buf, sector_count, sector_ofst);
            free(w_buf);
        }
        if(ret<=0){
            printf("error:%ld hm_write falid! table:%ld\n",ret,filenum);
            return -1;
        }
        uint64_t write_time_end=get_now_micros();
        uint64_t ttt = (write_time_end-write_time_begin);
        write_time += (write_time_end-write_time_begin);

        zone_[write_zone].wp +=sector_count * 512;
        struct Ldbfile *ldb= new Ldbfile(filenum,write_zone,sector_ofst,count,level);
        table_map_.insert(std::pair<uint64_t, struct Ldbfile*>(filenum,ldb));
        zone_info_[level][zone_info_[level].size()-1]->add_table(ldb);
        all_table_size += ldb->size;
        kv_store_sector += sector_count;

        //printf("write table:%ld to level-%d zone:%ld of size:%ld bytes ofst:%ld sect:%ld next:%ld write time:%lld\n",filenum,level,write_zone,count,sector_ofst,sector_count,sector_ofst+sector_count,ttt);
        
        return ret*512;
    }

    ssize_t HMManager::hm_read(uint64_t filenum,void *buf,uint64_t count, uint64_t offset){
        //printf("hm read\n");
        void *r_buf=NULL;
        uint64_t sector_count;
        uint64_t sector_ofst;
        uint64_t de_ofst;
        ssize_t ret;
        uint64_t read_time_begin=get_now_micros();

        std::map<uint64_t, struct Ldbfile*>::iterator it;
        it=table_map_.find(filenum);
        if(it==table_map_.end()){
            printf(" table_map_ can't find table:%ld!\n",filenum);
            return -1;
        }
        
        sector_ofst=it->second->offset+(offset/LOGICAL_BLOCK_SIZE)*(LOGICAL_BLOCK_SIZE/512);
        de_ofst=offset - (offset/LOGICAL_BLOCK_SIZE)*LOGICAL_BLOCK_SIZE;

        sector_count=((count+de_ofst)%LOGICAL_BLOCK_SIZE) ? ((count+de_ofst)/LOGICAL_BLOCK_SIZE+1)*(LOGICAL_BLOCK_SIZE/512) : ((count+de_ofst)/LOGICAL_BLOCK_SIZE)*(LOGICAL_BLOCK_SIZE/512);   //Align with logical block

        //r_buf=(void *)malloc(sector_count*512);
        ret=posix_memalign(&r_buf,MEMALIGN_SIZE,sector_count*512);
        if(ret!=0){
            printf("error:%ld posix_memalign falid!\n",ret);
            return -1;
        }
        memset(r_buf,0,sector_count*512);
        ret=pread(g_zns_fd_dio, r_buf, sector_count * 512,sector_ofst * 512);
        //ret=zbc_pread(dev_, r_buf, sector_count,sector_ofst);
        memcpy(buf,((char *)r_buf)+de_ofst,count);
        free(r_buf);
        if(ret<=0){
            printf("error:%ld hm_read falid!\n",ret);
            return -1;
        }
        
        uint64_t read_time_end=get_now_micros();
        read_time +=(read_time_end-read_time_begin);
        kv_read_sector += sector_count;
        MyLog("read table:%ld of size:%ld bytes file:%ld bytes time is %ld sector is %ld sex is %ld\n",filenum,count,it->second->size, read_time, kv_read_sector, (sector_ofst + sector_count) / 32 - (sector_ofst)/32 + 1);
        return count;
    }

    ssize_t HMManager::hm_delete(uint64_t filenum){
        //printf("hm delete\n");
        std::map<uint64_t, struct Ldbfile*>::iterator it;
        it=table_map_.find(filenum);
        //printf("it is %d\n",filenum);
        if(it!=table_map_.end()){
            struct Ldbfile *ldb=it->second;
            table_map_.erase(it);
            int level=ldb->level;
            uint64_t zone_id=ldb->zone;
            std::vector<struct Zonefile*>::iterator iz=zone_info_[level].begin();
            
            for(;iz!=zone_info_[level].end();iz++){
                if(zone_id==(*iz)->zone){
                    (*iz)->delete_table(ldb);
                    if((*iz)->ldb.empty() && (zone_[zone_id].wp-zone_[zone_id].start)/512 > 32*2048){ //?
          
                        struct Zonefile* zf=(*iz);
                        zone_info_[level].erase(iz);
                        if(is_com_window(level,zone_id)){
                           
                            std::vector<struct Zonefile*>::iterator ic=com_window_[level].begin();
                            for(;ic!=com_window_[level].end();ic++){
                               
                                if((*ic)->zone==zone_id){
                                    com_window_[level].erase(ic);
                                    break;
                                }
                            }
                        }
                        delete zf;
                        hm_free_zone(zone_id);
                        MyLog("delete zone:%ld from level-%d\n",zone_id,level);
                        delete_zone_num++;
                        
                    }
                    break;
                }
            }
            //printf("delete table:%ld from level-%d zone:%ld of size:%ld MB\n",filenum,level,zone_id,ldb->size/1048576);
            all_table_size -= ldb->size;
            delete ldb;
        }
        return 1;
    }

    ssize_t HMManager::move_file(uint64_t filenum,int to_level){
        //printf("hm mf\n");
        void *r_buf=NULL;
        ssize_t ret;
        std::map<uint64_t, struct Ldbfile*>::iterator it;
        it=table_map_.find(filenum);
        if(it==table_map_.end()){
            printf("error:move file failed! no find file:%ld\n",filenum);
            return -1;
        }
        struct Ldbfile *ldb=it->second;
        uint64_t sector_count=((ldb->size+PHYSICAL_BLOCK_SIZE-1)/PHYSICAL_BLOCK_SIZE)*(PHYSICAL_BLOCK_SIZE/512);
        uint64_t sector_ofst=ldb->offset;
        uint64_t file_size=ldb->size;
        int old_level=ldb->level;
        
        uint64_t read_time_begin=get_now_micros();
        //r_buf=(void *)malloc(sector_count*512);
        ret=posix_memalign(&r_buf,MEMALIGN_SIZE,sector_count*512);
        if(ret!=0){
            printf("error:%ld posix_memalign falid!\n",ret);
            return -1;
        }
        memset(r_buf,0,sector_count*512);
        ret=pread(g_zns_fd_dio, r_buf, sector_count * 512,sector_ofst * 512);
        //ret=zbc_pread(dev_, r_buf, sector_count,sector_ofst);
        if(ret<=0){
            printf("error:%ld z_read falid!\n",ret);
            return -1;
        }
        uint64_t read_time_end=get_now_micros();
        read_time +=(read_time_end-read_time_begin);

        hm_delete(filenum);
        kv_read_sector += sector_count;

        uint64_t write_time_begin=get_now_micros();

        hm_alloc(to_level,file_size);
        uint64_t write_zone=zone_info_[to_level][zone_info_[to_level].size()-1]->zone;
        sector_ofst=zone_[write_zone].wp / 512;
        ret=pwrite(g_zns_fd_dio, r_buf, sector_count * 512,sector_ofst * 512);
        //ret=zbc_pwrite(dev_, r_buf, sector_count, sector_ofst);
        if(ret<=0){
            printf("error:%ld zbc_pwrite falid!\n",ret);
            return -1;
        }
        uint64_t write_time_end=get_now_micros();
        write_time += (write_time_end-write_time_begin);

        zone_[write_zone].wp +=sector_count * 512;
        ldb= new Ldbfile(filenum,write_zone,sector_ofst,file_size,to_level);
        table_map_.insert(std::pair<uint64_t, struct Ldbfile*>(filenum,ldb));
        zone_info_[to_level][zone_info_[to_level].size()-1]->add_table(ldb);
        
        free(r_buf);
        kv_store_sector += sector_count;
        move_file_size += file_size;
        all_table_size += file_size;

       // printf("move table:%ld from level-%d to level-%d zone:%ld of size:%ld MB write time: %lld read time: %lld\n",filenum,old_level,to_level,write_zone,file_size/1048576,
           // write_time_end - write_time_begin, read_time_end - read_time_begin);
        return 1;
    }

    struct Ldbfile* HMManager::get_one_table(uint64_t filenum){
        std::map<uint64_t, struct Ldbfile*>::iterator it;
        it=table_map_.find(filenum);
        if(it==table_map_.end()){
            printf("error:no find file:%ld\n",filenum);
            return NULL;
        }
        return it->second;
    }

    void HMManager::get_zone_table(uint64_t filenum,std::vector<struct Ldbfile*> **zone_table){
        //printf("gnt\r\n");
        std::map<uint64_t, struct Ldbfile*>::iterator it;
        it=table_map_.find(filenum);
        if(it==table_map_.end()){
            printf("error:no find file:%ld\n",filenum);
            return ;
        }

        int level=it->second->level;
        uint64_t zone_id=it->second->zone;
        std::vector<struct Zonefile*>::iterator iz;
        for(iz=zone_info_[level].begin();iz!=zone_info_[level].end();iz++){
            if((*iz)->zone==zone_id){
                *zone_table=&((*iz)->ldb);
                return;
            }
        }

    }

    bool HMManager::trivial_zone_size_move(uint64_t filenum){
        //printf("tzsm\r\n");
        std::map<uint64_t, struct Ldbfile*>::iterator it;
        it=table_map_.find(filenum);

        if(it==table_map_.end()){
            printf("error:no find file:%ld\n",filenum);
            return false;
        }
        int level=it->second->level;
        if(level == 7){
            //printf("nonono baba\n");
            return false;
        }
        uint64_t zone_id=it->second->zone;
        
        if(zone_[zone_id].flags == ZBD_OP_FINISH){            
        //if((zone_[zone_id].len-(zone_[zone_id].wp-zone_[zone_id].start)) < 16*1024*1024){ //The remaining free space is less than 64MB, triggering
            return true;
        }
        
        else{
            //printf("hoho baba\n");
            return false;
        } 
    }

    void HMManager::move_zone(uint64_t filenum){
        //printf("mz\r\n");
        std::map<uint64_t, struct Ldbfile*>::iterator it;
        it=table_map_.find(filenum);
        if(it==table_map_.end()){
            printf("error:no find file:%ld\n",filenum);
            return ;
        }

        int level=it->second->level;
        uint64_t zone_id=it->second->zone;
        std::vector<struct Zonefile*>::iterator iz;
        struct Zonefile* zf;
        if(level > 7) return;
        
        if(is_com_window(level,zone_id)){
            std::vector<struct Zonefile*>::iterator ic=com_window_[level].begin();
            for(;ic!=com_window_[level].end();ic++){
                if((*ic)->zone==zone_id){
                    com_window_[level].erase(ic);
                    break;
                }
            }
        }
        for(iz=zone_info_[level].begin();iz!=zone_info_[level].end();iz++){
            if((*iz)->zone==zone_id){
                zf=(*iz);
                zone_info_[level].erase(iz);
                break;
            }
        }
        MyLog("before move zone:[");
        for(int i=0;i<zone_info_[level+1].size();i++){
            MyLog("%ld ",zone_info_[level+1][i]->zone);
        }
        MyLog("]\n");

        int size=zone_info_[level+1].size();
        if(size==0) size=1;
        zone_info_[level+1].insert(zone_info_[level+1].begin()+(size-1),zf);

        for(int i=0;i<zf->ldb.size();i++){
            zf->ldb[i]->level=level+1;
        }

        MyLog("move zone:%d table:[",zone_id);
        for(int i=0;i<zf->ldb.size();i++){
            MyLog("%ld ",zf->ldb[i]->table);
        }
        MyLog("] to level:%d\n",level+1);

        MyLog("end move zone:[");
        for(int i=0;i<zone_info_[level+1].size();i++){
            MyLog("%ld ",zone_info_[level+1][i]->zone);
        }
        MyLog("]\n");
    }


    void HMManager::update_com_window(int level){
        //printf("ucw\r\n");
        ssize_t window_num=adjust_com_window_num(level);
        if(COM_WINDOW_SEQ) {
            set_com_window_seq(level,window_num);
        }
        else{
            set_com_window(level,window_num);
        }
        
    }

    ssize_t HMManager::adjust_com_window_num(int level){
        //printf("acw\r\n");
        ssize_t window_num=0;
        switch (level){
            case 0:
            case 1:
            case 2:
                window_num = zone_info_[level].size();   //1,2 level's compaction window number is all the level
                break;
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
            case 8:
                window_num = zone_info_[level].size()/COM_WINDOW_SCALE; //other level compaction window number is 1/COM_WINDOW_SCALE
                break;
            default:
                break;
        }
        return window_num;
    }

    void HMManager::set_com_window(int level,int num){
        //printf("scw\r\n");
        int i;
        if(level==1||level==2){
            com_window_[level].clear();
            for(i=0;i<zone_info_[level].size();i++){
                com_window_[level].push_back(zone_info_[level][i]);
            }
            return;
        }
        if(com_window_[level].size() >= num){
            return;
        }
        size_t ran_num;
        for(i=com_window_[level].size();i<num;i++){
            while(1){
                ran_num=random_number(zone_info_[level].size()-1);
                if(!is_com_window(level,zone_info_[level][ran_num]->zone)){
                    break;
                }
            }
            com_window_[level].push_back(zone_info_[level][ran_num]);
        }
    }

    void HMManager::set_com_window_seq(int level,int num){
        //printf("scws\r\n");
        int i;
        if(level==1||level==2){
            com_window_[level].clear();
            for(i=0;i<zone_info_[level].size();i++){
                com_window_[level].push_back(zone_info_[level][i]);
            }
            return;
        }
        if(com_window_[level].size() >= num){
            return;
        }
        com_window_[level].clear();
        for(i=0;i<num;i++){
            com_window_[level].push_back(zone_info_[level][i]);
        }

    }

    bool HMManager::is_com_window(int level,uint64_t zone){
        //printf("icw\r\n");
        std::vector<struct Zonefile*>::iterator it;
        for(it=com_window_[level].begin();it!=com_window_[level].end();it++){
            if((*it)->zone==zone){
                return true;
            }
        }
        return false;
    }

    void HMManager::get_com_window_table(int level,std::vector<struct Ldbfile*> *window_table){
        //printf("gcwt\r\n");
        std::vector<struct Zonefile*>::iterator iz;
        std::vector<struct Ldbfile*>::iterator it;
        for(iz=com_window_[level].begin();iz!=com_window_[level].end();iz++){
            for(it=(*iz)->ldb.begin();it!=(*iz)->ldb.end();it++){
                window_table->push_back((*it));
            }
        }

    }



    //////statistics
    uint64_t HMManager::get_zone_num(){
        //printf("gzn\r\n");
        uint64_t num=0;
        int i;
        for(i=0;i<config::kNumLevels;i++){
            num += zone_info_[i].size();
        }
        return num;
    }

    void HMManager::get_one_level(int level,uint64_t *table_num,uint64_t *table_size){
        //printf("gol\r\n");
        std::vector<struct Zonefile*>::iterator it;
        uint64_t num=0;
        uint64_t size=0;
        for(it=zone_info_[level].begin();it!=zone_info_[level].end();it++){
            num += (*it)->ldb.size();
            size += (*it)->get_all_file_size();
        }
        *table_num = num;
        *table_size = size;
    }

    void HMManager::get_per_level_info(){
        //printf("gpli\r\n");
        int i;
        uint64_t table_num=0;
        uint64_t table_size=0;
        float percent=0;
        int zone_num=0;
        uint64_t zone_id;

        for(i=0;i<config::kNumLevels;i++){
            get_one_level(i,&table_num,&table_size);
            if(table_size == 0){
                percent = 0;
            }
            else {
                zone_num=zone_info_[i].size();
                zone_id=zone_info_[i][zone_num-1]->zone;
                percent=100.0*table_size/((zone_num-1)*64.0*1024*1024+(zone_[zone_id].wp - zone_[zone_id].start)); //?
            }
            MyLog("Level-%d zone_num:%d table_num:%ld table_size:%ld MB percent:%.2f %%\n",i,zone_info_[i].size(),table_num,table_size/1048576,percent);
        }
    }
    void HMManager::get_split_info(){
        std::vector<struct Zonefile*>::iterator it;
        int i;
        uint64_t table_num;
        uint64_t table_size;
        uint64_t zone_id;
        uint64_t size[6] = {0};
        uint64_t count[6] = {0};
        for(i=0;i<config::kNumLevels;i++){
            if(zone_info_[i].size() != 0){
                for(it=zone_info_[i].begin();it!=zone_info_[i].end();it++){
                    std::vector<struct Ldbfile*>::iterator iter;
                    for(iter=(*it)->ldb.begin();iter!=(*it)->ldb.end();iter++){
                        if((*iter)->size > 4*1024*1024*0.9){
                            size[0] += (*iter)->size;
                            count[0] += 1;
                        }
                        else if ((*iter)->size > 4*1024*1024*0.7){
                            size[1] += (*iter)->size;
                            count[1] += 1;
                        }
                        else if((*iter)->size > 4*1024*1024*0.5)
                        {
                            size[2] += (*iter)->size;
                            count[2] += 1;
                        }
                         else if((*iter)->size > 4*1024*1024*0.3)
                         {
                            size[3] += (*iter)->size;
                            count[3] += 1;
                         }
                        else if((*iter)->size > 4*1024*1024*0.1){
                            size[4]+= (*iter)->size;
                            count[4] +=1;
                        }
                        else{
                            size[5] += (*iter)->size;
                            count[5] +=1;
                        }
                    }
                }
                
            }
        }
        MyLog("ZoneLSMcount is %lld %lld %lld %lld %lld %lld\n",size[0],size[1],size[2],size[3],size[4],size[5]);
        MyLog("ZoneLSMsize is %lld %lld %lld %lld %lld %lld\n",count[0],count[1],count[2],count[3],count[4],count[5]);
        get_one_level(i,&table_num,&table_size);
    }
    void HMManager::get_valid_info(){
        //printf("gvi\r\n");
        MyLog("write_zone:%ld delete_zone_num:%ld max_zone_num:%ld table_num:%ld table_size:%ld MB\n",get_zone_num(),delete_zone_num,max_zone_num,table_map_.size(),all_table_size/1048576);
        get_per_level_info();
        uint64_t table_num;
        uint64_t table_size;
        uint64_t zone_id;
        float percent;
        std::vector<struct Zonefile*>::iterator it;
        int i;
        for(i=0;i<config::kNumLevels;i++){
            if(zone_info_[i].size() != 0){
                for(it=zone_info_[i].begin();it!=zone_info_[i].end();it++){
                    zone_id=(*it)->zone;
                    table_num=(*it)->ldb.size();
                    table_size=(*it)->get_all_file_size();
                    (*it)->get_all_file_info();
                    percent=100.0*table_size/(64.0*1024*1024);
                    MyLog("Level-%d zone_id:%ld table_num:%ld valid_size:%ld MB percent:%.2f %% \n",i,zone_id,table_num,table_size/1048576,percent);
                }
                
            }
        }

    }

    void HMManager::get_all_info(){
        //printf("gai\r\n");
        uint64_t disk_size=(get_zone_num())*zone_[first_zonenum_].len;
        MyLog("\n GGM is %lld %lld %lld %lld\n",Get_Count, Get_File, MetaSearch, NoCache);
        MyLog("\n Time break down is %lf, %lf, %lf, %lf, %lf, %lf, %lf, %lf \n",total_get_time,find_file_time,table_cache_get_time,find_table_time,Internal_get_time,iiter_time, block_read_time, seek_time);
        MyLog("\nget all data!\n");
        MyLog("table_all_size:%ld MB kv_read_sector:%ld MB kv_store_sector:%ld MB disk_size:%ld MB \n",all_table_size/(1024*1024),\
            kv_read_sector/2048,kv_store_sector/2048,disk_size/2048);
        MyLog("read_time:%.1f s write_time:%.1f s read:%.1f MB/s write:%.1f MB/s\n",1.0*read_time*1e-6,1.0*write_time*1e-6,\
            (kv_read_sector/2048.0)/(read_time*1e-6),(kv_store_sector/2048.0)/(write_time*1e-6));
        get_valid_info();
        MyLog("\n");
        
    }

    void HMManager::get_valid_data(){
        //printf("gvd\r\n");
        MyLog2("level,zone_id,table_num,valid_size(MB),percent(%%)\n");
        uint64_t table_num;
        uint64_t table_size;
        uint64_t zone_id;
        float percent;
        std::vector<struct Zonefile*>::iterator it;
        int i;
        for(i=0;i<config::kNumLevels;i++){
            if(zone_info_[i].size() != 0){
                for(it=zone_info_[i].begin();it!=zone_info_[i].end();it++){
                    zone_id=(*it)->zone;
                    table_num=(*it)->ldb.size();
                    table_size=(*it)->get_all_file_size();
                    percent=100.0*table_size/(64.0*1024*1024);
                    MyLog2("%d,%ld,%ld,%ld,%.2f\n",i,zone_id,table_num,table_size/1048576,percent);
                }
                
            }
        }
    }

    void HMManager::get_my_info(int num){
        
        MyLog6("\nnum:%d table_size:%ld MB kv_read_sector:%ld MB kv_store_sector:%ld MB zone_num:%ld max_zone_num:%ld move_size:%ld MB\n",num,all_table_size/(1024*1024),\
            kv_read_sector/2048,kv_store_sector/2048,get_zone_num(),max_zone_num,move_file_size/(1024*1024));
        MyLog6("read_time:%.1f s write_time:%.1f s read:%.1f MB/s write:%.1f MB/s\n",1.0*read_time*1e-6,1.0*write_time*1e-6,\
            (kv_read_sector/2048.0)/(read_time*1e-6),(kv_store_sector/2048.0)/(write_time*1e-6));
        get_valid_all_data(num);
    }

    void HMManager::get_valid_all_data(int num){
        //printf("gvad\r\n");
        uint64_t disk_size=(get_zone_num())*zone_[first_zonenum_].len;

        MyLog3("\nnum:%d\n",num);
        MyLog3("table_all_size:%ld MB kv_read_sector:%ld MB kv_store_sector:%ld MB disk_size:%ld MB \n",all_table_size/(1024*1024),\
            kv_read_sector/2048,kv_store_sector/2048,disk_size/2048);
        MyLog3("read_time:%.1f s write_time:%.1f s read:%.1f MB/s write:%.1f MB/s\n",1.0*read_time*1e-6,1.0*write_time*1e-6,\
            (kv_read_sector/2048.0)/(read_time*1e-6),(kv_store_sector/2048.0)/(write_time*1e-6));
        MyLog3("write_zone:%ld delete_zone_num:%ld max_zone_num:%ld table_num:%ld table_size:%ld MB\n",get_zone_num(),delete_zone_num,max_zone_num,table_map_.size(),all_table_size/1048576);
        uint64_t table_num;
        uint64_t table_size;
        int zone_num=0;
        uint64_t zone_id;
        float percent;
        std::vector<struct Zonefile*>::iterator it;
        int i;
        for(i=0;i<config::kNumLevels;i++){
            get_one_level(i,&table_num,&table_size);
            if(table_size == 0){
                percent = 0;
            }
            else {
                zone_num=zone_info_[i].size();
                zone_id=zone_info_[i][zone_num-1]->zone;
                percent=100.0*table_size/((zone_num-1)*64.0*1024*1024+(zone_[zone_id].wp - zone_[zone_id].start)); //?
            }
            MyLog3("Level-%d zone_num:%d table_num:%ld table_size:%ld MB percent:%.2f %% \n",i,zone_info_[i].size(),table_num,table_size/1048576,percent);
        }
        MyLog3("level,zone_id,table_num,valid_size(MB),percent(%%)\n");
        for(i=0;i<config::kNumLevels;i++){
            if(zone_info_[i].size() != 0){
                for(it=zone_info_[i].begin();it!=zone_info_[i].end();it++){
                    zone_id=(*it)->zone;
                    table_num=(*it)->ldb.size();
                    table_size=(*it)->get_all_file_size();
                    percent=100.0*table_size/(64.0*1024*1024);
                    MyLog3("%d,%ld,%ld,%ld,%.2f\n",i,zone_id,table_num,table_size/1048576,percent);
                }
                
            }
        }
    }
    ///jeeyoon added
     uint64_t HMManager::get_victim_zone(int level){
        std::vector<struct Zonefile*>::iterator it;
        uint64_t min_num = 100000;
        //uint64_t max_size = 0;
        uint64_t num=0;
        int finish_flag = 0;
        uint64_t zone = -1;
        //uint64_t size=0;
        /*for(it=zone_info_[level].begin();it!=zone_info_[level].end();it++){
            num = (*it)->ldb.size();
            if(min_num > num || (finish_flag == 0 && zone_[zone].flags == ZBD_OP_FINISH)){
                if(finish_flag == 0 && zone_[zone].flags == ZBD_OP_FINISH)
                {
                    finish_flag =1;
                }
                min_num = num;
                zone = (*it)->zone;
            }
            //size += (*it)->get_all_file_size();
        }*/
        it = zone_info_[level].begin();
        zone = (*it) -> zone;
         return zone;
     }
    void HMManager::move_to_next_level(int level)
    {
        std::map<uint64_t, struct Ldbfile*>::iterator it;
        struct Zonefile* zf;
        std::vector<struct Zonefile*>::iterator iz;
        assert(zone_info_[level+1].size() == 0);
        
        for(iz=zone_info_[level].begin();iz!=zone_info_[level].end();iz++){
            int size=zone_info_[level+1].size();
            zf=(*iz);
            zone_info_[level+1].insert(zone_info_[level+1].begin()+(size-1),zf);   
        }
        for(iz=zone_info_[level].begin();iz!=zone_info_[level].end();iz++){
            zf=(*iz);
            zone_info_[level+1].erase(iz);   
        }
    }


    //////end

    

    
    




}