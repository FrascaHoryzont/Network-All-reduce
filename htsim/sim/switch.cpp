// -*- c-basic-offset: 4; indent-tabs-mode: nil -*-        

#include "queue.h"
#include "switch.h"
#include "eth_pause_packet.h"
#include "queue_lossless.h"
#include "queue_lossless_input.h"
#include "loggers.h"

uint32_t Switch::id = 0;

int Switch::addPort(BaseQueue* q){
    _ports.push_back(q);
    q->setSwitch(this);
    return _ports.size()-1;
}

void Switch::sendPause(LosslessQueue* problem, unsigned int wait){
    for (size_t i = 0;i < _ports.size();i++){
        LosslessQueue* q = (LosslessQueue*)_ports.at(i);
        if (q==problem || !(q->getRemoteEndpoint())) continue;
        EthPausePacket* pkt = EthPausePacket::newpkt(wait,_id);
        q->getRemoteEndpoint()->receivePacket(*pkt);
    }
};

void Switch::configureLossless(){
    for (size_t i = 0;i < _ports.size();i++){
        LosslessQueue* q = (LosslessQueue*)_ports.at(i);    
        q->setSwitch(this);
        q->initThresholds();
    }
};

void Switch::add_logger(Logfile& log, simtime_picosec sample_period) {
    assert(_ports.size() > 0);
    MultiQueueLoggerSampling* queue_logger = new MultiQueueLoggerSampling(get_id(), sample_period,_ports.at(0)->eventlist());
    log.addLogger(*queue_logger);
    for (size_t i = 0; i < _ports.size(); i++) {
        _ports.at(i)->setLogger(queue_logger);
    }
}

void Switch::receivePacket(Packet& pkt) {
    // Check if this is an In-Network Computing packet
    cerr << "DIAG_SWITCH: Entered receivePacket for Pkt " << pkt.id() << endl;

    if (pkt._is_inc) {
        cerr << "DIAG_SWITCH: Handling INC packet." << endl;
        handle_inc_packet(&pkt);
        return; 
    }

    // Standard Forwarding Logic
    if (pkt.nexthop() < pkt.route()->size()) {
        pkt.sendOn();
    } else {
        pkt.free();
    }
}

void Switch::handle_inc_packet(Packet* p) {
    uint32_t job_id = p->_inc_job_id;
    uint32_t block_id = p->_inc_block_id;
    
    // Identifichiamo il flusso (o lo switch precedente)
    uint32_t contributor_id = (p->_inc_last_switch_id != -1) ? (uint32_t)p->_inc_last_switch_id : p->flow_id();

    auto key = std::make_pair(job_id, block_id);
    
    if (_aggregation_table.find(key) == _aggregation_table.end()) {
        AggregationEntry entry;
        entry.first_arrival = eventlist().now();
        _aggregation_table[key] = entry;
    }

    // Inseriamo nel SET per evitare di contare i duplicati (Retransmission Storm)
    _aggregation_table[key].received_flows.insert(contributor_id);
    
    int current_count = _aggregation_table[key].received_flows.size();
    
    // --- SOGLIA ---
    // Imposta a 2 perché nel tuo test hai Node 0 e Node 1 che inviano.
    int expected_children = 2; 

    // STAMPA SOLO SE CAMBIA LO STATO (Riduce spam)
    // o se siamo pronti ad inviare
    if (current_count >= expected_children) {
        // Usa CERR per essere sicuro di vederlo anche se crasha subito dopo
        cerr << "!!! AGGREGATION COMPLETE !!! Switch " << _id << " Block " << block_id << endl;
        
        send_aggregated_packet(job_id, block_id);
        _aggregation_table.erase(key);
    } 
    
    // Consuma sempre il pacchetto in ingresso
    p->free();
}

void Switch::send_aggregated_packet(uint32_t job_id, uint32_t block_id) {
    Packet* p = new IncPacket(job_id, block_id);
    p->_inc_last_switch_id = getID(); 
    p->set_size(1000); 

    int best_port = select_best_port_towards_spine();

    if (best_port != -1) {
        BaseQueue* q = _ports.at(best_port);
        PacketSink* next_hop_sink = q->getRemoteEndpoint();

        // --- FIX CRASH: Se il link è morto, NON INVIARE ---
        if (next_hop_sink) {
             p->set_next_hop(next_hop_sink);
        } else {
             cout << "CRITICAL ERROR: Switch " << _id << " Port " << best_port << " is disconnected!" << endl;
             p->free(); // Distruggi il pacchetto
             return;    // <--- QUESTO RETURN È VITALE!
        }
        // --------------------------------------------------

        cout << "DEBUG_SWITCH: Switch " << _id << " Sending Block " << block_id 
             << " UP via Port " << best_port << endl;
             
        q->receivePacket(*p);
    } else {
        cout << "DEBUG_SWITCH: Root reached or no link." << endl;
        p->free();
    }
}

int Switch::select_best_port_towards_spine() {
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