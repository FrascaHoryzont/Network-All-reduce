// -*- c-basic-offset: 4; indent-tabs-mode: nil -*-        

#ifndef _SWITCH_H
#define _SWITCH_H
#include "queue.h"
/*
 * A switch to group together multiple ports (currently used in the
 * PAUSE implementation), and in generic_topology
 *
 * At the moment we don't normally build topologies where the switch
 * receives a packet and makes a forwarding decision - the route
 * already carries the forwarding path.  But we might revisit this to
 * simulate switches that make dynamic decisions.
 */

#include <list>
#include "config.h"
#include "eventlist.h"
#include "network.h"
#include "loggertypes.h"
#include "drawable.h"
#include "routetable.h"
#include <map> 
#include <vector>
#include <set>

class BaseQueue;
class LosslessQueue;
class LosslessInputQueue;
class RouteTable;

// [FLARE] Structure to track aggregation state
struct AggregationEntry {
    std::set<uint32_t> received_flows; // <--- USIAMO UN SET PER EVITARE DUPLICATI
    simtime_picosec first_arrival;
    uint32_t aggregated_data = 0;
};

class Switch : public EventSource, public Drawable, public PacketSink {
 public:
    Switch(EventList& eventlist) : EventSource(eventlist, "none") { _name = "none"; _id = id++;};
    Switch(EventList& eventlist, string s) : EventSource(eventlist, s) { _name= s; _id = id++;}
    virtual ~Switch() = default;

    virtual int addPort(BaseQueue* q);
    virtual void addHostPort(int addr, int flowid, PacketSink* transport) { abort();};

    uint32_t getID(){return _id;};
    virtual uint32_t getType() {return 0;}

    virtual void receivePacket(Packet& pkt); 

    virtual void receivePacket(Packet& pkt, VirtualQueue* prev) {abort();}
    
    virtual void doNextEvent() {abort();}

    // Routing standard (ECMP)
    virtual Route* getNextHop(Packet& pkt) { return getNextHop(pkt, NULL);}
    virtual Route* getNextHop(Packet& pkt, BaseQueue* ingress_port) {abort();};

    BaseQueue* getPort(int id) { assert(id >= 0); if ((unsigned int)id<_ports.size()) return _ports.at(id); else return NULL;}

    unsigned int portCount(){ return _ports.size();}

    void sendPause(LosslessQueue* problem, unsigned int wait);
    void sendPause(LosslessInputQueue* problem, unsigned int wait);

    void configureLossless();
    void configureLosslessInput();

    void add_logger(Logfile& log, simtime_picosec sample_period); 

    virtual const string& nodename() {return _name;}

protected:
    vector<BaseQueue*> _ports;
    uint32_t _id;
    string _name;
    RouteTable* _fib;
    static uint32_t id;

};
#endif
