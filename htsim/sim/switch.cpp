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
        //handle_inc_packet(&pkt);
        return; 
    }

    // Standard Forwarding Logic
    if (pkt.nexthop() < pkt.route()->size()) {
        pkt.sendOn();
    } else {
        pkt.free();
    }
}
