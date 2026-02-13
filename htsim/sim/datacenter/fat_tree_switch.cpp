// -*- c-basic-offset: 4; indent-tabs-mode: nil -*-
#include "fat_tree_switch.h"
#include "routetable.h"
#include "fat_tree_topology.h"
#include "callback_pipe.h"
#include "queue_lossless.h"
#include "queue_lossless_output.h"
#include "network.h"

unordered_map<BaseQueue*,uint32_t> FatTreeSwitch::_port_flow_counts;
vector<uint32_t> FatTreeSwitch::_job_participants;

// Implementazione funzione
void FatTreeSwitch::add_job_participant(uint32_t host_id) {
    vector<uint32_t>& list = _job_participants;
    for (uint32_t existing : list) {
        if (existing == host_id) return;
    }
    list.push_back(host_id);
}

Route* FatTreeSwitch::build_route_core_to_host(uint32_t dest_id) {
    Route* r = new Route();
    uint32_t b = 0; // bundle index
    uint32_t agg_target;

    // --- 1. CORE -> AGG ---
    if (_ft->cfg().get_tiers() == 3) {
        uint32_t pod_id = _ft->cfg().HOST_POD(dest_id);
        agg_target = _ft->cfg().MIN_POD_AGG_SWITCH(pod_id) + (_id % _ft->cfg().agg_switches_per_pod());
        
        r->push_back(_ft->queues_nc_nup[_id][agg_target][b]);
        r->push_back(_ft->pipes_nc_nup[_id][agg_target][b]);
        r->push_back(_ft->queues_nc_nup[_id][agg_target][b]->getRemoteEndpoint());
    }
    else{
        agg_target=_id;
    }

    // --- 2. AGG -> TOR ---
    uint32_t tor_target = _ft->cfg().HOST_POD_SWITCH(dest_id);
    
    r->push_back(_ft->queues_nup_nlp[agg_target][tor_target][b]);
    r->push_back(_ft->pipes_nup_nlp[agg_target][tor_target][b]);
    r->push_back(_ft->queues_nup_nlp[agg_target][tor_target][b]->getRemoteEndpoint());

    // --- 3. TOR -> HOST ---
    BaseQueue* last_q = _ft->queues_nlp_ns[tor_target][dest_id][b];
    r->push_back(last_q);
    r->push_back(_ft->pipes_nlp_ns[tor_target][dest_id][b]);
    r->push_back(last_q->getRemoteEndpoint());

    return r;
}

FatTreeSwitch::FatTreeSwitch(EventList& eventlist, string s, switch_type t, uint32_t id,simtime_picosec delay, FatTreeTopology* ft): Switch(eventlist, s) {
    _id = id;
    _type = t;
    _pipe = new CallbackPipe(delay,eventlist, this);
    _uproutes = NULL;
    _ft = ft;
    _crt_route = 0;
    _hash_salt = random();
    _last_choice = eventlist.now();
    _fib = new RouteTable();
}

FatTreeSwitch::~FatTreeSwitch() {
    delete _pipe;
    delete _fib;
}

void FatTreeSwitch::receivePacket(Packet& pkt){
    //cout << "SWITCH " << _id << " DIAG_FAT_TREE_SWITCH: Entered receivePacket for Pkt " << pkt.id() << endl;
    if (pkt.type()==ETH_PAUSE){
        EthPausePacket* p = (EthPausePacket*)&pkt;
        //I must be in lossless mode!
        //find the egress queue that should process this, and pass it over for processing. 
        for (size_t i = 0;i < _ports.size();i++){
            LosslessQueue* q = (LosslessQueue*)_ports.at(i);
            if (q->getRemoteEndpoint() && ((Switch*)q->getRemoteEndpoint())->getID() == p->senderID()){
                q->receivePacket(pkt);
                break;
            }
        }
        
        return;
    }
    if(pkt.type()==INC_RESULT){
        pkt.sendOn();
        return;
    }
    if (pkt._is_inc) {
        handle_inc_packet(&pkt); 
        return; 
    }

    if (_packets.find(&pkt)==_packets.end()){
        //ingress pipeline processing.

        _packets[&pkt] = true;

        const Route * nh = getNextHop(pkt,NULL);
        if (nh == NULL) {
            pkt.free();
            return;
        }
        //set next hop which is peer switch.
        pkt.set_route(*nh);

        //emulate the switching latency between ingress and packet arriving at the egress queue.
        _pipe->receivePacket(pkt); 
    }
    else {
        if (pkt.nexthop() < pkt.route()->size()) {
            pkt.sendOn();
        } else {
            pkt.free();
        }
    }
};

void FatTreeSwitch::addHostPort(int addr, int flowid, PacketSink* transport_port){
    Route* rt = new Route();
    BaseQueue* q = _ft->queues_nlp_ns[_ft->cfg().HOST_POD_SWITCH(addr)][addr][0];
    
    rt->push_back(q);
    rt->push_back(_ft->pipes_nlp_ns[_ft->cfg().HOST_POD_SWITCH(addr)][addr][0]);
    rt->push_back(transport_port);
    _fib->addHostRoute(addr,rt,flowid);
    q->setRemoteEndpoint(transport_port);
}

uint32_t mhash(uint32_t x) {
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = (x >> 16) ^ x;
    return x;
}

uint32_t FatTreeSwitch::adaptive_route_p2c(vector<FibEntry*>* ecmp_set, int8_t (*cmp)(FibEntry*,FibEntry*)){
    uint32_t choice = 0, min = UINT32_MAX;
    uint32_t start, i = 0;
    static const uint16_t nr_choices = 2;
    
    do {
        start = random()%ecmp_set->size();

        Route * r= (*ecmp_set)[start]->getEgressPort();
        assert(r && r->size()>1);
        BaseQueue* q = (BaseQueue*)(r->at(0));
        assert(q);
        if (q->queuesize()<min){
            choice = start;
            min = q->queuesize();
        }
        i++;
    } while (i<nr_choices);
    return choice;
}

uint32_t FatTreeSwitch::adaptive_route(vector<FibEntry*>* ecmp_set, int8_t (*cmp)(FibEntry*,FibEntry*)){
    //cout << "adaptive_route" << endl;
    uint32_t choice = 0;

    uint32_t best_choices[256];
    uint32_t best_choices_count = 0;
  
    FibEntry* min = (*ecmp_set)[choice];
    best_choices[best_choices_count++] = choice;

    for (uint32_t i = 1; i< ecmp_set->size(); i++){
        int8_t c = cmp(min,(*ecmp_set)[i]);

        if (c < 0){
            choice = i;
            min = (*ecmp_set)[choice];
            best_choices_count = 0;
            best_choices[best_choices_count++] = choice;
        }
        else if (c==0){
            assert(best_choices_count<255);
            best_choices[best_choices_count++] = i;
        }        
    }

    assert (best_choices_count>=1);
    uint32_t choiceindex = random()%best_choices_count;
    choice = best_choices[choiceindex];
    //cout << "ECMP set choices " << ecmp_set->size() << " Choice count " << best_choices_count << " chosen entry " << choiceindex << " chosen path " << choice << " ";

    if (cmp==compare_flow_count){
        //for (uint32_t i = 0; i<best_choices_count;i++)
          //  cout << "pathcnt " << best_choices[i] << "="<< _port_flow_counts[(BaseQueue*)( (*ecmp_set)[best_choices[i]]->getEgressPort()->at(0))]<< " ";
        
        _port_flow_counts[(BaseQueue*)((*ecmp_set)[choice]->getEgressPort()->at(0))]++;
    }

    return choice;
}

uint32_t FatTreeSwitch::replace_worst_choice(vector<FibEntry*>* ecmp_set, int8_t (*cmp)(FibEntry*,FibEntry*),uint32_t my_choice){
    uint32_t best_choice = 0;
    uint32_t worst_choice = 0;

    uint32_t best_choices[256];
    uint32_t best_choices_count = 0;

    FibEntry* min = (*ecmp_set)[best_choice];
    FibEntry* max = (*ecmp_set)[worst_choice];
    best_choices[best_choices_count++] = best_choice;

    for (uint32_t i = 1; i< ecmp_set->size(); i++){
        int8_t c = cmp(min,(*ecmp_set)[i]);

        if (c < 0){
            best_choice = i;
            min = (*ecmp_set)[best_choice];
            best_choices_count = 0;
            best_choices[best_choices_count++] = best_choice;
        }
        else if (c==0){
            assert(best_choices_count<256);
            best_choices[best_choices_count++] = i;
        }        

        if (cmp(max,(*ecmp_set)[i])>0){
            worst_choice = i;
            max = (*ecmp_set)[worst_choice];
        }
    }

    //might need to play with different alternatives here, compare to worst rather than just to worst index.
    int8_t r = cmp((*ecmp_set)[my_choice],(*ecmp_set)[worst_choice]);
    assert(r>=0);

    if (r==0){
        assert (best_choices_count>=1);
        return best_choices[random()%best_choices_count];
    }
    else return my_choice;
}


int8_t FatTreeSwitch::compare_pause(FibEntry* left, FibEntry* right){
    Route * r1= left->getEgressPort();
    assert(r1 && r1->size()>1);
    LosslessOutputQueue* q1 = dynamic_cast<LosslessOutputQueue*>(r1->at(0));
    Route * r2= right->getEgressPort();
    assert(r2 && r2->size()>1);
    LosslessOutputQueue* q2 = dynamic_cast<LosslessOutputQueue*>(r2->at(0));

    if (!q1->is_paused()&&q2->is_paused())
        return 1;
    else if (q1->is_paused()&&!q2->is_paused())
        return -1;
    else 
        return 0;
}

int8_t FatTreeSwitch::compare_flow_count(FibEntry* left, FibEntry* right){
    Route * r1= left->getEgressPort();
    assert(r1 && r1->size()>1);
    BaseQueue* q1 = (BaseQueue*)(r1->at(0));
    Route * r2= right->getEgressPort();
    assert(r2 && r2->size()>1);
    BaseQueue* q2 = (BaseQueue*)(r2->at(0));

    if (_port_flow_counts.find(q1)==_port_flow_counts.end())
        _port_flow_counts[q1] = 0;

    if (_port_flow_counts.find(q2)==_port_flow_counts.end())
        _port_flow_counts[q2] = 0;

    //cout << "CMP q1 " << q1 << "=" << _port_flow_counts[q1] << " q2 " << q2 << "=" << _port_flow_counts[q2] << endl; 

    if (_port_flow_counts[q1] < _port_flow_counts[q2])
        return 1;
    else if (_port_flow_counts[q1] > _port_flow_counts[q2] )
        return -1;
    else 
        return 0;
}

int8_t FatTreeSwitch::compare_queuesize(FibEntry* left, FibEntry* right){
    Route * r1= left->getEgressPort();
    assert(r1 && r1->size()>1);
    BaseQueue* q1 = dynamic_cast<BaseQueue*>(r1->at(0));
    Route * r2= right->getEgressPort();
    assert(r2 && r2->size()>1);
    BaseQueue* q2 = dynamic_cast<BaseQueue*>(r2->at(0));

    if (q1->quantized_queuesize() < q2->quantized_queuesize())
        return 1;
    else if (q1->quantized_queuesize() > q2->quantized_queuesize())
        return -1;
    else 
        return 0;
}

int8_t FatTreeSwitch::compare_bandwidth(FibEntry* left, FibEntry* right){
    Route * r1= left->getEgressPort();
    assert(r1 && r1->size()>1);
    BaseQueue* q1 = dynamic_cast<BaseQueue*>(r1->at(0));
    Route * r2= right->getEgressPort();
    assert(r2 && r2->size()>1);
    BaseQueue* q2 = dynamic_cast<BaseQueue*>(r2->at(0));

    if (q1->quantized_utilization() < q2->quantized_utilization())
        return 1;
    else if (q1->quantized_utilization() > q2->quantized_utilization())
        return -1;
    else 
        return 0;

    /*if (q1->average_utilization() < q2->average_utilization())
        return 1;
    else if (q1->average_utilization() > q2->average_utilization())
        return -1;
    else 
        return 0;        */
}

int8_t FatTreeSwitch::compare_pqb(FibEntry* left, FibEntry* right){
    //compare pause, queuesize, bandwidth.
    int8_t p = compare_pause(left, right);

    if (p!=0)
        return p;
    
    p = compare_queuesize(left,right);

    if (p!=0)
        return p;

    return compare_bandwidth(left,right);
}

int8_t FatTreeSwitch::compare_pq(FibEntry* left, FibEntry* right){
    //compare pause, queuesize, bandwidth.
    int8_t p = compare_pause(left, right);

    if (p!=0)
        return p;
    
    return compare_queuesize(left,right);
}

int8_t FatTreeSwitch::compare_qb(FibEntry* left, FibEntry* right){
    //compare pause, queuesize, bandwidth.
    int8_t p = compare_queuesize(left, right);

    if (p!=0)
        return p;
    
    return compare_bandwidth(left,right);
}

int8_t FatTreeSwitch::compare_pb(FibEntry* left, FibEntry* right){
    //compare pause, queuesize, bandwidth.
    int8_t p = compare_pause(left, right);

    if (p!=0)
        return p;
    
    return compare_bandwidth(left,right);
}

void FatTreeSwitch::permute_paths(vector<FibEntry *>* uproutes) {
    int len = uproutes->size();
    for (int i = 0; i < len; i++) {
        int ix = random() % (len - i);
        FibEntry* tmppath = (*uproutes)[ix];
        (*uproutes)[ix] = (*uproutes)[len-1-i];
        (*uproutes)[len-1-i] = tmppath;
    }
}

FatTreeSwitch::routing_strategy FatTreeSwitch::_strategy = FatTreeSwitch::NIX;
uint16_t FatTreeSwitch::_ar_fraction = 0;
uint16_t FatTreeSwitch::_ar_sticky = FatTreeSwitch::PER_PACKET;
simtime_picosec FatTreeSwitch::_sticky_delta = timeFromUs((uint32_t)10);
double FatTreeSwitch::_ecn_threshold_fraction = 0.2;
double FatTreeSwitch::_speculative_threshold_fraction = 0.2;
int8_t (*FatTreeSwitch::fn)(FibEntry*,FibEntry*)= &FatTreeSwitch::compare_queuesize;
uint16_t FatTreeSwitch::_trim_size = 64;
bool FatTreeSwitch::_disable_trim = false;

Route* FatTreeSwitch::getNextHop(Packet& pkt, BaseQueue* ingress_port){
    //cout<<pkt.dst()<<" "<<_type<<endl;
    if (pkt.dst() == UINT32_MAX) {
        // Se siamo un ToR, mandiamo sempre SU verso un Aggregation Switch.
        // Non usiamo la FIB per evitare di allocare vettori enormi.
        if (_type == TOR) {
            uint32_t podid;
            uint32_t agg_min, agg_max;

            // Logica per trovare gli switch sopra di noi (copiata dalla logica standard)
            if (_ft->cfg().get_tiers() == 3) {
                podid = _id / _ft->cfg().tor_switches_per_pod();
                agg_min = _ft->cfg().MIN_POD_AGG_SWITCH(podid);
                agg_max = _ft->cfg().MAX_POD_AGG_SWITCH(podid);
            } else {
                agg_min = 0;
                agg_max = _ft->cfg().getNAGG() - 1;
            }

            // Scegliamo un Aggregation Switch a caso (ECMP semplice) per bilanciare
            // Usiamo l'hash del flusso per coerenza
            uint32_t num_agg = agg_max - agg_min + 1;
            uint32_t choice = freeBSDHash(pkt.flow_id(), pkt.pathid(), _hash_salt) % num_agg;
            uint32_t target_agg = agg_min + choice;
            uint32_t b = 0; // bundle 0

            // Costruiamo la rotta al volo
            Route* r = new Route();
            r->push_back(_ft->queues_nlp_nup[_id][target_agg][b]);
            r->push_back(_ft->pipes_nlp_nup[_id][target_agg][b]);
            r->push_back(_ft->queues_nlp_nup[_id][target_agg][b]->getRemoteEndpoint());
            
            pkt.set_direction(UP);
            return r;
        }
        else if (_type == AGG) {
            
            // Se siamo in una rete a 3 livelli, dobbiamo salire ancora verso i CORE
            if (_ft->cfg().get_tiers() == 3) {
                // Calcoli presi dalla logica standard AGG->CORE
                uint32_t podpos = _id % _ft->cfg().agg_switches_per_pod();
                // Numero di uplink disponibili verso i Core
                uint32_t uplink_bundles = _ft->cfg().radix_up(AGG_TIER) / _ft->cfg().bundlesize(CORE_TIER);
                
                if (uplink_bundles == 0) return NULL;

                // Scegliamo un uplink a caso (ECMP)
                uint32_t l = freeBSDHash(pkt.flow_id(), pkt.pathid(), _hash_salt) % uplink_bundles;
                
                // Calcola l'ID del Core switch target
                uint32_t core = l * _ft->cfg().agg_switches_per_pod() + podpos;
                uint32_t b = 0; 

                Route* r = new Route();
                // Nota: qui si usa queues_nup_nc (Up to Core)
                r->push_back(_ft->queues_nup_nc[_id][core][b]);
                r->push_back(_ft->pipes_nup_nc[_id][core][b]);
                r->push_back(_ft->queues_nup_nc[_id][core][b]->getRemoteEndpoint());

                pkt.set_direction(UP);
                return r;
            } 
            else {
                // Se siamo in una rete a 2 livelli (come small_fat_tree),
                // l'Aggregation Switch è la cima (Root/Spine).
                // Non deve inoltrare oltre, il pacchetto muore qui (processato da handle_inc_packet).
                return NULL;
            }
        }
        abort();
    }
    vector<FibEntry*> * available_hops = _fib->getRoutes(pkt.dst());

    if (available_hops){
        //implement a form of ECMP hashing; might need to revisit based on measured performance.
        uint32_t ecmp_choice = 0;
        if (available_hops->size()>1)
            switch(_strategy){
            case NIX:
                abort();
            case ECMP:
                ecmp_choice = freeBSDHash(pkt.flow_id(),pkt.pathid(),_hash_salt) % available_hops->size();
                break;
            case ADAPTIVE_ROUTING:
                if (pkt.size() < 100) {
                    // don't bother adaptive routing the small packets - don't want to pollute the tables
                    ecmp_choice = freeBSDHash(pkt.flow_id(),pkt.pathid(),_hash_salt) % available_hops->size();
                    break;
                }
                if (_ar_sticky==FatTreeSwitch::PER_PACKET){
                    ecmp_choice = adaptive_route(available_hops,fn); 
                } 
                else if (_ar_sticky==FatTreeSwitch::PER_FLOWLET){     
                    if (_flowlet_maps.find(pkt.flow_id())!=_flowlet_maps.end()){
                        FlowletInfo* f = _flowlet_maps[pkt.flow_id()];
                        
                        // only reroute an existing flow if its inter packet time is larger than _sticky_delta and
                        // and
                        // 50% chance happens. 
                        // and (commented out) if the switch has not taken any other placement decision that we've not seen the effects of.
                        if (eventlist().now() - f->_last > _sticky_delta && /*eventlist().now() - _last_choice > _pipe->delay() + BaseQueue::_update_period  &&*/ random()%2==0){ 
                            //cout << "AR 1 " << timeAsUs(eventlist().now()) << endl;
                            uint32_t new_route = adaptive_route(available_hops,fn); 
                            if (fn(available_hops->at(f->_egress),available_hops->at(new_route)) < 0){
                                f->_egress = new_route;
                                _last_choice = eventlist().now();
                                //cout << "Switch " << _type << ":" << _id << " choosing new path "<<  f->_egress << " for " << pkt.flow_id() << " at " << timeAsUs(eventlist().now()) << " last is " << timeAsUs(f->_last) << endl;
                            }
                        }
                        ecmp_choice = f->_egress;

                        f->_last = eventlist().now();
                    }
                    else {
                        //cout << "AR 2 " << timeAsUs(eventlist().now()) << endl;
                        ecmp_choice = adaptive_route(available_hops,fn); 
                        _last_choice = eventlist().now();

                        _flowlet_maps[pkt.flow_id()] = new FlowletInfo(ecmp_choice,eventlist().now());
                    }
                }

                break;
            case ECMP_ADAPTIVE:
                ecmp_choice = freeBSDHash(pkt.flow_id(),pkt.pathid(),_hash_salt) % available_hops->size();
                if (random()%100 < 50)
                    ecmp_choice = replace_worst_choice(available_hops,fn, ecmp_choice);
                break;
            case RR:
                if (pkt.size()<128)
                    ecmp_choice = freeBSDHash(pkt.flow_id(),pkt.pathid(),_hash_salt) % available_hops->size();
                else {
                    if (_crt_route>=1*available_hops->size()){
                        _crt_route = 0;
                        permute_paths(available_hops);
                    }
                    ecmp_choice = _crt_route % available_hops->size();
                    _crt_route ++;
                }
                break;
            case RR_ECMP:
                if (_type == TOR){
                    if (_crt_route>=5 * available_hops->size()){
                        _crt_route = 0;
                        permute_paths(available_hops);
                    }
                    ecmp_choice = _crt_route % available_hops->size();
                    _crt_route ++;
                }
                else ecmp_choice = freeBSDHash(pkt.flow_id(),pkt.pathid(),_hash_salt) % available_hops->size();
                
                break;
            }
        
        FibEntry* e = (*available_hops)[ecmp_choice];
        pkt.set_direction(e->getDirection());
        
        return e->getEgressPort();
    }

    //no route table entries for this destination. Add them to FIB or fail. 
    if (_type == TOR){
        if ( _ft->cfg().HOST_POD_SWITCH(pkt.dst()) == _id) { 
            //this host is directly connected!
            HostFibEntry* fe = _fib->getHostRoute(pkt.dst(),pkt.flow_id());
            assert(fe);
            pkt.set_direction(DOWN);
            return fe->getEgressPort();
        } else {
            //route packet up!
            if (_uproutes)
                _fib->setRoutes(pkt.dst(),_uproutes);
            else {
                uint32_t podid,agg_min,agg_max;

                if (_ft->cfg().get_tiers()==3) {
                    podid = _id / _ft->cfg().tor_switches_per_pod();
                    agg_min = _ft->cfg().MIN_POD_AGG_SWITCH(podid);
                    agg_max = _ft->cfg().MAX_POD_AGG_SWITCH(podid);
                }
                else {
                    agg_min = 0;
                    agg_max = _ft->cfg().getNAGG()-1;
                }

                for (uint32_t k=agg_min; k<=agg_max;k++){
                    for (uint32_t b = 0; b < _ft->cfg().bundlesize(AGG_TIER); b++) {
                        Route * r = new Route();
                        r->push_back(_ft->queues_nlp_nup[_id][k][b]);
                        assert(((BaseQueue*)r->at(0))->getSwitch() == this);

                        r->push_back(_ft->pipes_nlp_nup[_id][k][b]);
                        r->push_back(_ft->queues_nlp_nup[_id][k][b]->getRemoteEndpoint());
                        _fib->addRoute(pkt.dst(),r,1,UP);
                    }

                    /*
                      FatTreeSwitch* next = (FatTreeSwitch*)_ft->queues_nlp_nup[_id][k]->getRemoteEndpoint();
                      assert (next->getType()==AGG && next->getID() == k);
                    */
                }
                _uproutes = _fib->getRoutes(pkt.dst());
                permute_paths(_uproutes);
            }
        }
    } else if (_type == AGG) {
        if (_ft->cfg().get_tiers()==2 || _ft->cfg().HOST_POD(pkt.dst()) == _ft->cfg().AGG_SWITCH_POD_ID(_id)) {
            //must go down!
            //target NLP id is 2 * pkt.dst()/K
            uint32_t target_tor = _ft->cfg().HOST_POD_SWITCH(pkt.dst());
            for (uint32_t b = 0; b < _ft->cfg().bundlesize(AGG_TIER); b++) {
                Route * r = new Route();
                r->push_back(_ft->queues_nup_nlp[_id][target_tor][b]);
                assert(((BaseQueue*)r->at(0))->getSwitch() == this);

                r->push_back(_ft->pipes_nup_nlp[_id][target_tor][b]);          
                r->push_back(_ft->queues_nup_nlp[_id][target_tor][b]->getRemoteEndpoint());

                _fib->addRoute(pkt.dst(),r,1, DOWN);
            }
        } else {
            //go up!
            if (_uproutes)
                _fib->setRoutes(pkt.dst(),_uproutes);
            else {
                uint32_t podpos = _id % _ft->cfg().agg_switches_per_pod();
                uint32_t uplink_bundles = _ft->cfg().radix_up(AGG_TIER) / _ft->cfg().bundlesize(CORE_TIER);
                for (uint32_t l = 0; l <  uplink_bundles ; l++) {
                    uint32_t core = l * _ft->cfg().agg_switches_per_pod() + podpos;
                    for (uint32_t b = 0; b < _ft->cfg().bundlesize(CORE_TIER); b++) {
                        Route *r = new Route();
                        r->push_back(_ft->queues_nup_nc[_id][core][b]);
                        assert(((BaseQueue*)r->at(0))->getSwitch() == this);

                        r->push_back(_ft->pipes_nup_nc[_id][core][b]);
                        r->push_back(_ft->queues_nup_nc[_id][core][b]->getRemoteEndpoint());

                        /*
                          FatTreeSwitch* next = (FatTreeSwitch*)_ft->queues_nup_nc[_id][k]->getRemoteEndpoint();
                          assert (next->getType()==CORE && next->getID() == k);
                        */
                    
                        _fib->addRoute(pkt.dst(),r,1,UP);

                        //cout << "AGG switch " << _id << " adding route to " << pkt.dst() << " via CORE " << k << " bundle_id " << b << endl;
                    }
                }
                //_uproutes = _fib->getRoutes(pkt.dst());
                permute_paths(_fib->getRoutes(pkt.dst()));
            }
        }
    } else if (_type == CORE) {
        uint32_t nup = _ft->cfg().MIN_POD_AGG_SWITCH(_ft->cfg().HOST_POD(pkt.dst())) + (_id % _ft->cfg().agg_switches_per_pod());
        for (uint32_t b = 0; b < _ft->cfg().bundlesize(CORE_TIER); b++) {
            Route *r = new Route();
            //cout << "CORE switch " << _id << " adding route to " << pkt.dst() << " via AGG " << nup << endl;

            assert (_ft->queues_nc_nup[_id][nup][b]);
            r->push_back(_ft->queues_nc_nup[_id][nup][b]);
            assert(((BaseQueue*)r->at(0))->getSwitch() == this);

            assert (_ft->pipes_nc_nup[_id][nup][b]);
            r->push_back(_ft->pipes_nc_nup[_id][nup][b]);

            r->push_back(_ft->queues_nc_nup[_id][nup][b]->getRemoteEndpoint());
            _fib->addRoute(pkt.dst(),r,1,DOWN);
        }
    }
    else {
        cerr << "Route lookup on switch with no proper type: " << _type << endl;
        abort();
    }
    assert(_fib->getRoutes(pkt.dst()));

    //FIB has been filled in; return choice. 
    return getNextHop(pkt, ingress_port);
};
bool FatTreeSwitch::hasUpLinks(){  
    if (_type == CORE) {
        return false;
    }
    if (_type == TOR) {
        return true;
    }
    if (_type == AGG) {
        return (_ft->cfg().get_tiers() > 2);
    }
    return false;
}
void FatTreeSwitch::handle_inc_packet(Packet* p) {
    // GESTIONE PACCHETTI IN SALITA (AGGREGAZIONE)
    uint32_t job_id = p->_inc_job_id;
    uint32_t block_id = p->_inc_block_id;
    
    // Identifica chi ha mandato il contributo (Host o Switch sotto)
    uint32_t contributor_id = (p->_inc_last_switch_id != -1) ? (uint32_t)p->_inc_last_switch_id : p->flow_id();

    auto key = std::make_pair(job_id, block_id);
    
    // Se abbiamo già completato questo blocco, ignora i duplicati
    if (_completed_blocks.find(key) != _completed_blocks.end()) {
        p->free();
        return;
    }

    if (_aggregation_table.find(key) == _aggregation_table.end()) {
        AggregationEntry entry;
        entry.first_arrival = eventlist().now();
        entry.aggregated_data = 0;
        _aggregation_table[key] = entry;
    }
    _aggregation_table[key].aggregated_data += p->_inc_int_data;
    _aggregation_table[key].received_flows.insert(contributor_id);
    
    /// Calcolo dinamico basato sulla configurazione caricata dal file .topo
    int expected_children = 0;

    if (_type == TOR) {
        // Il ToR aspetta pacchetti dagli HOST collegati sotto di lui.
        expected_children = _ft->cfg().radix_down(TOR_TIER); 
    } 
    else if (_type == AGG) {
        // L'Aggregation aspetta pacchetti dai ToR collegati sotto di lui.
        expected_children = _ft->cfg().radix_down(AGG_TIER);
    }
    else if (_type == CORE) {
        // Il Core aspetta pacchetti dagli Aggregation switches (uno per Pod).
        expected_children = _ft->cfg().radix_down(CORE_TIER);
    } 

    if (_aggregation_table[key].received_flows.size() >= expected_children) {
        uint32_t final_sum = _aggregation_table[key].aggregated_data;
        // Segna come completato ed elimina dalla tabella attiva
        _completed_blocks.insert(key);
        _aggregation_table.erase(key);

        if (hasUpLinks()) {
            cout << "!!! AGGREGATION COMPLETE !!! Switch " << _id << " (Intermediate) -> Sending UP" << endl;
            send_aggregated_packet(job_id, block_id, final_sum); 
        } else {
            cout << "!!! AGGREGATION COMPLETE !!! Switch " << _id << " (ROOT) -> Broadcasting DOWN" << endl;
            send_inc_result_down(p, final_sum);
            return; 
        }
    } 
    
    p->free();
}

void FatTreeSwitch::send_aggregated_packet(uint32_t job_id, uint32_t block_id,uint32_t aggregated_data) {
    int best_port = select_best_port_towards_spine();
    if (best_port == -1) {
        // cout << "DEBUG_SWITCH: Switch " << _id << " is Root (or isolated). Aggregation finished." << endl;
        return;
    }
    BaseQueue* q = _ports.at(best_port);
    PacketSink* next_hop_sink = q->getRemoteEndpoint();
    if (!next_hop_sink) {
        cerr << "CRITICAL ERROR: Switch " << _id << " selected Port " << best_port 
             << " but endpoint is NULL (Link disconnected)!" << endl;
        return;
    }

    Route* route = new Route();
    route->push_back(next_hop_sink);

    IncPacket* p = IncPacket::newpkt(*route, job_id, block_id, aggregated_data, INC_DATA);

    p->_inc_last_switch_id = getID();
    
    p->set_next_hop(next_hop_sink);

    // Debug Log
    cout << "DEBUG_SWITCH: Switch " << _id << " Sending Aggregated Block " << block_id 
         << " UP via Port " << best_port 
         << " to Node " << next_hop_sink->nodename() << endl;

    q->receivePacket(*p);
}

int FatTreeSwitch::select_best_port_towards_spine() {
    int best_port = -1;
    uint64_t min_size = UINT64_MAX;

    // Itera sulle porte UP (da 2 in poi per Small Fat Tree)
    for (size_t i = 2; i < _ports.size(); i++) {
        BaseQueue* q = _ports.at(i);
        
        // CHECK CRUCIALE: La porta DEVE avere un cavo collegato
        if (q->getRemoteEndpoint() != NULL) { 
            if (q->queuesize() < min_size) {
                min_size = q->queuesize();
                best_port = i;
            }
        }
    }
    return best_port;
}
void FatTreeSwitch::send_inc_result_down(Packet* p, uint32_t aggregated_data) {
    vector<uint32_t> destinations = _job_participants; 

    IncPacket* original = (IncPacket*)p;
    
    cout << "DEBUG_SWITCH: Root " << _id << " distributing RESULT to participants" << endl;
    for (uint32_t dest_id : destinations) {
        // Costruiamo la rotta completa al volo (Core -> AGG -> ToR -> Host)
        Route* complete_route = build_route_core_to_host(dest_id);

        // Creiamo il pacchetto con la rotta già impostata
        IncPacket* copy = IncPacket::newpkt(*complete_route, original->_inc_job_id, original->_inc_block_id, aggregated_data);
        copy->make_result();
        copy->set_direction(DOWN);
        copy->set_dst(dest_id);
        
        // FONDAMENTALE: Firma il pacchetto come già processato da te (Core)
        copy->_inc_last_switch_id = getID();

        copy->set_route(*complete_route);

        // Passiamo alla pipe. Quando uscirà, FatTreeSwitch::receivePacket chiamerà sendOn().
        // Poiché la rotta è completa, sendOn() lo manderà all'AGG.
        // L'AGG riceverà il pacchetto, vedrà che ha già una rotta e NON lo intercetterà
        _pipe->receivePacket(*copy);
    }
}