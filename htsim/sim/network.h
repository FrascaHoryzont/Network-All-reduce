// -*- c-basic-offset: 4; indent-tabs-mode: nil -*- 
#ifndef NETWORK_H
#define NETWORK_H

#include <vector>
#include <iostream>
#include "config.h"
#include "loggertypes.h"
#include "route.h"

typedef uint32_t id_t;

class Packet;
class PacketFlow;
class PacketSink;
typedef uint32_t packetid_t;
typedef uint32_t flowid_t;

void print_route(const Route& route);

class DataReceiver : public Logged {
 public:
    DataReceiver(const string& name) : Logged(name) {};
    virtual ~DataReceiver(){};
    virtual uint64_t cumulative_ack()=0;
    //virtual uint32_t get_id()=0;
    virtual uint32_t drops()=0;
};

class PacketFlow : public Logged {
    friend class Packet;
 public:
    PacketFlow(TrafficLogger* logger);
    virtual ~PacketFlow() {};
    void set_logger(TrafficLogger* logger);
    void logTraffic(Packet& pkt, Logged& location, TrafficLogger::TrafficEvent ev);
    void set_flowid(flowid_t id);
    inline flowid_t flow_id() const {return _flow_id;}
    bool log_me() const {return _logger != NULL;}
 protected:
    static packetid_t _max_flow_id;
    flowid_t _flow_id;
    TrafficLogger* _logger;
};


typedef enum {IP, TCP, TCPACK, TCPNACK, SWIFT, SWIFTACK, STRACK, STRACKACK,
              NDP, NDPACK, NDPNACK, NDPPULL, NDPRTS,
              NDPLITE, NDPLITEACK, NDPLITEPULL, NDPLITERTS,
              ETH_PAUSE, TOFINO_TRIM,
              ROCE, ROCEACK, ROCENACK,
              HPCC, HPCCACK, HPCCNACK,
              CNP,
              INC_DATA,   // Pacchetto che porta dati da aggregare (All-Reduce)
              INC_ACK,     // (Opzionale) Risposta broadcast finale
              INC_RESULT,  // Pacchetto che riporta il risultato dell'aggregazione
              EQDSDATA, EQDSPULL, EQDSACK, EQDSNACK, EQDSRTS,
              UECDATA, UECPULL, UECACK, UECNACK, UECRTS} packet_type;

typedef enum {NONE, UP, DOWN} packet_direction;

class VirtualQueue {
 public:
    VirtualQueue() { }
    virtual ~VirtualQueue() {}
    virtual void completedService(Packet& pkt) = 0;
};

class LosslessInputQueue;

// See tcppacket.h to illustrate how Packet is typically used.
class Packet {
    friend class PacketFlow;
 public:

    // --- AGGIUNTA FLARE / CANARY ---
    // Identificano l'operazione di In-Network Computing
    bool _is_inc = false;       // È un pacchetto da aggregare?
    uint32_t _inc_job_id = 0;   // ID del Job (es. AllReduce #1)
    uint32_t _inc_block_id = 0; // ID del blocco dati
    uint32_t _inc_int_data = 0; // Payload semplice (intero) per testare la somma
                                // In una simulazione reale useresti un vector,
                                // ma per debug basta un int.

    // Campi specifici per Canary (gestione dinamica)
    bool _is_straggler = false; // Se è un pacchetto in ritardo gestito dal timeout
    
    int _inc_last_switch_id=-1;
    
    // use PRIO_NONE if the packet is never expected to encounter a priority queue, otherwise default to PRIO_LO
    typedef enum {PRIO_LO, PRIO_MID, PRIO_HI, PRIO_NONE} PktPriority;
    
    /* empty constructor; Packet::set must always be called as
       well. It's a separate method, for convenient reuse */
    Packet() {_is_header = false; _bounced = false; _type = IP; _flags = 0; _refcount = 0; _dst = UINT32_MAX; _pathid = UINT32_MAX; _direction = NONE; _ingressqueue = NULL; _route = 0; _nexthop = 0; _oldnexthop = 0; _is_inc = false; _inc_job_id = 0; _inc_block_id = 0; _inc_int_data = 0; _is_straggler = false; _inc_last_switch_id = -1; _next_routed_hop = 0;} 

    /* say "this packet is no longer wanted". (doesn't necessarily
       destroy it, so it can be reused) */
    virtual void free();

    static void set_packet_size(int packet_size) {
        // Use Packet::set_packet_size() to change the default packet
        // size for TCP or NDP data packets.  You MUST call this
        // before the value has been used to initialize anything else.
        // If someone has already read the value of packet size, no
        // longer allow it to be changed, or all hell will break
        // loose.
        assert(_packet_size_fixed == false);
        _data_packet_size = packet_size;
    }

    static int data_packet_size() {
        _packet_size_fixed = true;
        return _data_packet_size;
    }

    virtual PacketSink* sendOn(); // "go on to the next hop along your route"
                                  // returns what that hop is

    virtual PacketSink* previousHop() {if (_nexthop>=2) return _route->at(_nexthop-2); else return NULL;}
    virtual PacketSink* currentHop() {if (_nexthop>=1) return _route->at(_nexthop-1); else return NULL;}
    
    virtual PacketSink* sendOn2(VirtualQueue* crtSink);

    uint16_t size() const {return _size;}
    void set_size(int i) {_size = i;}
    packet_type type() const {return _type;};
    bool header_only() const {return _is_header;}
    bool bounced() const {return _bounced;}
    PacketFlow& flow() const {return *_flow;}
    virtual ~Packet() {};
    inline const packetid_t id() const {return _id;}
    inline uint32_t flow_id() const {return _flow->flow_id();}
    inline uint32_t dst() const {return _dst;}
    inline void set_dst(uint32_t dst) { _dst = dst;}
    inline uint32_t pathid() {
        assert(_pathid != UINT32_MAX);
        return _pathid;
    }

    inline void set_pathid(uint32_t p) { _pathid = p;}
    const Route* route() const {return _route;}
    const Route* reverse_route() const {return _route->reverse();}

    inline void set_next_hop(PacketSink* snk) { _next_routed_hop = snk;}

    virtual void strip_payload(uint16_t trim_size) {
        assert(trim_size >= 64);
        assert(!_is_header);
        _is_header = true;
        _size = trim_size;
    }
    virtual void bounce();
    virtual void unbounce(uint16_t pktsize);
    inline uint32_t path_len() const {return _path_len;}

    virtual void go_up(){ if (_direction == NONE) _direction = UP; else if (_direction == DOWN) abort();}
    virtual void go_down(){ if (_direction == UP) _direction = DOWN; else if (_direction == NONE) abort();}
    virtual void set_direction(packet_direction d){ 
        if (d==_direction) return; 
        if ((_direction == NONE) || (_direction == UP && d==DOWN)) 
            _direction = d; 
        else {
            cout << "Current direction is " << _direction << " trying to change it to " << d << endl;
            abort();
        }
    }

    virtual PktPriority priority() const = 0;

    virtual packet_direction get_direction() {return _direction;}

    void inc_ref_count() { _refcount++;};
    void dec_ref_count() { _refcount--;};
    int ref_count() {return _refcount;};
    
    inline uint32_t flags() const {return _flags;}
    inline void set_flags(uint32_t f) {_flags = f;}

    uint32_t nexthop() const {return _nexthop;} // only intended to be used for debugging
    virtual void set_route(const Route &route);
    virtual void set_route(const Route *route=nullptr);
    virtual void set_route(PacketFlow& flow, const Route &route, int pkt_size, packetid_t id);

    void set_ingress_queue(LosslessInputQueue* t){assert(!_ingressqueue); _ingressqueue = t;}
    LosslessInputQueue* get_ingress_queue(){assert(_ingressqueue); return _ingressqueue;}
    void clear_ingress_queue(){assert(_ingressqueue); _ingressqueue = NULL;}

    //    void set_detour(PacketSink* n, int rewind) {_detour = n;_nexthop -= rewind;}
    
    string str() const;
 protected:
    void set_attrs(PacketFlow& flow, int pkt_size, packetid_t id);

    static int _data_packet_size; // default size of a TCP or NDP data packet,
                                  // measured in bytes
    static bool _packet_size_fixed; //prevent foot-shooting
    
    packet_type _type;
    
    uint16_t _size,_oldsize;
    
    
    bool _is_header;
    bool _bounced; // packet has hit a full queue, and is being bounced back to the sender
    uint32_t _flags; // used for ECN & friends

    uint32_t _dst; //used for packets that do not have a route in switched networks.    
    uint32_t _pathid;  //used for ECMP hashing.
    packet_direction _direction; //used to avoid loop in FatTrees.   

    // A packet can contain a route or a routegraph, but not both.
    // Eventually switch over entirely to RouteGraph?
    const Route* _route;

    //PacketSink* _detour;
    uint32_t _nexthop,_oldnexthop;

    //used for tunneling purposes when one packet can be referenced by multiple classes
    uint8_t _refcount;

    //used when using routing tables in switches, i.e. the packet has no route.
    PacketSink* _next_routed_hop;

    packetid_t _id;
    PacketFlow* _flow{nullptr};
    static PacketFlow _defaultFlow;
    LosslessInputQueue* _ingressqueue;
    uint32_t _path_len; // length of the path in hops - used in BCube priority routing with NDP
};

class PacketSink {
 public:
    PacketSink() { _remoteEndpoint = NULL; }
    virtual ~PacketSink() {}
    virtual void receivePacket(Packet& pkt) =0;
    virtual void receivePacket(Packet& pkt,VirtualQueue* previousHop) {
        receivePacket(pkt);
    };

    virtual void setRemoteEndpoint(PacketSink* q) {_remoteEndpoint = q;};
    virtual void setRemoteEndpoint2(PacketSink* q) {_remoteEndpoint = q;q->setRemoteEndpoint(this);};
    PacketSink* getRemoteEndpoint() {return _remoteEndpoint;}

    virtual const string& nodename()=0;

    PacketSink* _remoteEndpoint;
};

// NIC mostly exists to enable logging of an EqdsNIC, particularly
// when we want aggregate stats for all the incoming flows from
// different sources.
class NIC {
public:
    NIC(id_t src_id);

    virtual const string& nodename() const =0;
    id_t _src_id;
    id_t get_id() const {return _src_id;}

    void logReceivedData(mem_b tot_bytes, mem_b new_bytes);
    void logReceivedTrim(mem_b bytes);
    void logReceivedCtrl(mem_b bytes);
    mem_b _total_data_received;  // stats collection - total amount of
                                 // data bytes received (inc pkt
                                 // headers)
    mem_b _new_data_received;  // stats collection - amount of new
                               // unique data bytes received (inc pkt
                               // headers, but not duplicates)
    mem_b _trim_received;  // stats collection
    mem_b _ctrl_received;  // stats collection, plus pull pacing reduction
};


// For speed, it may be useful to keep a database of all packets that
// have been allocated -- that way we don't need a malloc for every
// new packet, we can just reuse old packets. Care, though -- the set()
// method will need to be invoked properly for each new/reused packet

template<class P>
class PacketDB {
 public:
    PacketDB() : _alloc_count(0) {}
    ~PacketDB() {
        for (P* p : _freelist)
            delete p;
        //cout << "Pkt count: " << _alloc_count << endl;
        //cout << "Pkt mem used: " << _alloc_count * sizeof(P) << endl;
    }
    P* allocPacket() {
        if (_freelist.empty()) {
            P* p = new P();
            p->inc_ref_count();
            /*
            if (_alloc_count == 0) {
                cout << "Packet size: " << sizeof(P) << endl;
            }
            */
            _alloc_count++;
            return p;
        } else {
            P* p = _freelist.back();
            _freelist.pop_back();
            p->inc_ref_count();
            return p;
        }
    };
    void freePacket(P* pkt) {
        assert(pkt->ref_count()>=1);
        pkt->dec_ref_count();

        if (!pkt->ref_count())
            _freelist.push_back(pkt);
    };

 protected:
    vector<P*> _freelist; // Irek says it's faster with vector than with list
    int _alloc_count;
};
class IncPacket : public Packet {
public:
    // Database per il riciclo della memoria (standard del simulatore)
    static PacketDB<IncPacket> _packetdb;

    inline static IncPacket* newpkt(const Route& route, 
                                    uint32_t job_id, uint32_t block_id, 
                                    uint32_t int_data = 0,
                                    packet_type p_type = INC_DATA) {
        
        // 1. Alloca dal database invece che con 'new'
        IncPacket* p = _packetdb.allocPacket();
        
        // 2. Inizializzazione tramite base class (imposta flow, route, size, id)
        // Usiamo una dimensione standard (es. 1000 byte) o data_packet_size()
        p->set_route(_defaultFlow, route, 1000, 0); 

        // 3. Configurazione campi specifici INC
        p->_type = p_type;
        p->_is_inc = true;  // Campo che avevi aggiunto in Packet
        p->_inc_job_id = job_id;
        p->_inc_block_id = block_id;
        p->_inc_int_data = int_data;
        
        // 4. Reset stati di default (evita residui da pacchetti riutilizzati)
        p->_is_header = false;
        p->_bounced = false;
        p->_is_straggler = false;
        p->_inc_last_switch_id = -1;
        p->_direction = NONE;
        p->_path_len = route.size();

        return p;
    }

    // Distruttore virtuale
    virtual ~IncPacket() {}

    // Implementazione obbligatoria del metodo virtuale puro
    virtual PktPriority priority() const override { 
        // I pacchetti INC solitamente hanno priorità alta o media per ridurre la latenza di sincronizzazione
        return PRIO_MID; 
    }

    // Override di free per tornare nel database corretto
    virtual void free() override {
        _packetdb.freePacket(this);
    }

    // Metodo helper per cambiare il tipo durante il tragitto (es. da DATA a RESULT)
    void make_result() {
        _type = INC_RESULT;
    }
};


#endif
