// tlv_client.cpp
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <vector>
#include <string>
#include <stdint.h>

static void die(const char* m){ perror(m); _exit(1); }

static void send_cmd(int fd, const std::vector<std::string>& args){
    std::vector<uint8_t> out;
    uint32_t n = args.size();
    out.insert(out.end(), (uint8_t*)&n, (uint8_t*)&n + 4);
    for (auto &s: args){
        uint32_t len = (uint32_t)s.size();
        out.insert(out.end(), (uint8_t*)&len, (uint8_t*)&len + 4);
        out.insert(out.end(), s.begin(), s.end());
    }
    uint32_t L = (uint32_t)out.size();
    if (write(fd, &L, 4) != 4) die("write len");
    if (write(fd, out.data(), out.size()) != (ssize_t)out.size()) die("write body");
}

static void readn(int fd, void* p, size_t n){
    uint8_t* b=(uint8_t*)p; size_t got=0;
    while (got<n){
        ssize_t r=read(fd,b+got,n-got);
        if(r<=0) die("read");
        got+=r;
    }
}

static void dump_tlv(const uint8_t* p, const uint8_t* end, int indent=0){
    auto pad=[&](){ for(int i=0;i<indent;i++) printf("  "); };
    if (p>=end){ pad(); printf("(empty)\n"); return; }
    while (p<end){
        uint8_t tag=*p++;
        switch(tag){
            case 0: pad(); printf("NIL\n"); break;
            case 1:{ // ERR
                if(p+4>end){ pad(); puts("ERR <truncated>"); return; }
                uint32_t len; memcpy(&len,p,4); p+=4;
                if(p+len>end){ pad(); puts("ERR <truncated str>"); return; }
                pad(); printf("ERR \"%.*s\"\n",(int)len,(const char*)p); p+=len;
            }break;
            case 2:{ // STR
                if(p+4>end){ pad(); puts("STR <truncated len>"); return; }
                uint32_t len; memcpy(&len,p,4); p+=4;
                if(p+len>end){ pad(); puts("STR <truncated str>"); return; }
                pad(); printf("STR \"%.*s\"\n",(int)len,(const char*)p); p+=len;
            }break;
            case 3:{ // INT64
                if(p+8>end){ pad(); puts("INT <truncated>"); return; }
                int64_t v; memcpy(&v,p,8); p+=8;
                pad(); printf("INT %lld\n",(long long)v);
            }break;
            case 5:{ // ARR
                if(p+4>end){ pad(); puts("ARR <truncated len>"); return; }
                uint32_t n; memcpy(&n,p,4); p+=4;
                pad(); printf("ARR[%u]\n", n);
                for(uint32_t i=0;i<n;i++){
                    // Each element is a TLV; dump one element
                    // Call recursively to print exactly one item at a time:
                    // We don't know sizes of items ahead of time, so parse by single element:
                    // Implement a sub-dumper that parses one element and returns new pointer.
                    // For simplicity here, call dump_tlv on the remaining buffer but indent+1
                    // and break after one element by tracking pointer before & after.
                    const uint8_t* before=p;
                    // Peek tag to decide how far to jump
                    if(p>=end){ pad(); puts("  <truncated array item>"); return; }
                    uint8_t t=*p++;
                    auto back=[&](const char* msg){ pad(); printf("  <bad item: %s>\n", msg); return; };
                    switch(t){
                        case 0: { for(int k=0;k<indent+1;k++) printf("  "); printf("NIL\n"); } break;
                        case 1:
                        case 2:{
                            if(p+4>end){ back("len"); return; }
                            uint32_t len; memcpy(&len,p,4); p+=4;
                            if(p+len>end){ back("str"); return; }
                            for(int k=0;k<indent+1;k++) printf("  ");
                            printf("%s \"%.*s\"\n", t==1?"ERR":"STR",(int)len,(const char*)p);
                            p+=len;
                        } break;
                        case 3:{
                            if(p+8>end){ back("int"); return; }
                            int64_t v; memcpy(&v,p,8); p+=8;
                            for(int k=0;k<indent+1;k++) printf("  ");
                            printf("INT %lld\n",(long long)v);
                        } break;
                        case 5:{
                            // nested array: recurse
                            if(p+4>end){ back("arrlen"); return; }
                            uint32_t m; memcpy(&m,p,4); p+=4;
                            for(int k=0;k<indent+1;k++) printf("  ");
                            printf("ARR[%u]\n", m);
                            // recurse m elements by calling the same inner loop
                            // Easiest: call dump_tlv for the rest at indent+2 and rely on counts? 
                            // For a simple tester, we'll print a placeholder and bail to keep code short.
                            for(int k=0;k<indent+2;k++) printf("  ");
                            printf("<nested array not expanded in demo>\n");
                        } break;
                        default:
                            for(int k=0;k<indent+1;k++) printf("  ");
                            printf("UNKNOWN_TAG %u\n", t);
                            // cannot advance safely; bail
                            return;
                    }
                    (void)before;
                }
            }break;
            default:
                pad(); printf("UNKNOWN_TAG %u\n", tag);
                return;
        }
    }
}

int main(int argc, char** argv){
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if(fd<0) die("socket");
    sockaddr_in a{}; a.sin_family=AF_INET; a.sin_port=htons(1234); a.sin_addr.s_addr=htonl(INADDR_LOOPBACK);
    if(connect(fd,(sockaddr*)&a,sizeof(a))<0) die("connect");

    auto roundtrip=[&](std::initializer_list<const char*> args){
        std::vector<std::string> v; for(auto s:args) v.emplace_back(s);
        send_cmd(fd, v);
        uint32_t L; readn(fd,&L,4);
        std::vector<uint8_t> buf(L); readn(fd,buf.data(),L);
        printf("Reply (%u bytes):\n", L);
        dump_tlv(buf.data(), buf.data()+buf.size());
        printf("----\n");
    };

    roundtrip({"set","foo","bar"});
    roundtrip({"get","foo"});
    roundtrip({"del","foo"});
    roundtrip({"get","foo"});
    roundtrip({"keys"});

    close(fd);
    return 0;
}
