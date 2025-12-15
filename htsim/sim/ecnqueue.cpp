// -*- c-basic-offset: 4; indent-tabs-mode: nil -*-        
#include "ecnqueue.h"
#include <math.h>
#include "ecn.h"
#include "queue_lossless.h"
#include <iostream>

ECNQueue::ECNQueue(linkspeed_bps bitrate, mem_b maxsize, 
                   EventList& eventlist, QueueLogger* logger, mem_b  K)
    : Queue(bitrate,maxsize,eventlist,logger), 
      _K(K)
{
    _state_send = LosslessQueue::READY;
}


void
ECNQueue::receivePacket(Packet & pkt)
{
    //is this a PAUSE packet?
    if (pkt.type()==ETH_PAUSE){
        EthPausePacket* p = (EthPausePacket*)&pkt;
        
        if (p->sleepTime()>0){
            //remote end is telling us to shut up.
            //assert(_state_send == LosslessQueue::READY);
            if (queuesize()>0)
                //we have a packet in flight
                _state_send = LosslessQueue::PAUSE_RECEIVED;
            else
                _state_send = LosslessQueue::PAUSED;
            
            //cout << timeAsMs(eventlist().now()) << " " << _name << " PAUSED "<<endl;
        }
        else {
            //we are allowed to send!
            _state_send = LosslessQueue::READY;
            //cout << timeAsMs(eventlist().now()) << " " << _name << " GO "<<endl;
            
            //start transmission if we have packets to send!
            if(queuesize()>0)
                beginService();
        }
        
        pkt.free();
        return;
    }

    // --- FLARE/CANARY INTERCEPTION START ---
    if (pkt._is_inc) {
        // Se c'è uno switch attaccato a questa coda...
        if (_switch) {
            // ...e non è lo switch che ha appena generato il pacchetto...
            if (pkt._inc_last_switch_id != (int)_switch->getID()) {
                cout << "!!! ECN SWITCH INTERCEPTION !!! Queue " << _nodename 
                     << " handing over to Switch " << _switch->getID() << endl;
                
                _switch->receivePacket(pkt);
                return; // STOP! Lo switch prende il controllo.
            }
        }
    }
    // --- FLARE/CANARY INTERCEPTION END ---


    if (_queuesize+pkt.size() > _maxsize) {
        /* if the packet doesn't fit in the queue, drop it */
        if (_logger) 
            _logger->logQueue(*this, QueueLogger::PKT_DROP, pkt);
        pkt.flow().logTraffic(pkt, *this, TrafficLogger::PKT_DROP);
        pkt.free();
        _num_drops++;
        return;
    }
    pkt.flow().logTraffic(pkt, *this, TrafficLogger::PKT_ARRIVE);

    //mark on enqueue
    //    if (_queuesize > _K)
    //  pkt.set_flags(pkt.flags() | ECN_CE);

    /* enqueue the packet */
    bool queueWasEmpty = _enqueued.empty();
    Packet* pkt_p = &pkt;
    _enqueued.push(pkt_p);
    _queuesize += pkt.size();
    if (_logger) _logger->logQueue(*this, QueueLogger::PKT_ENQUEUE, pkt);

    if (queueWasEmpty && _state_send==LosslessQueue::READY) {
        /* schedule the dequeue event */
        assert(_enqueued.size() == 1);
        beginService();
    }
    
}

void
ECNQueue::completeService()
{
    /* dequeue the packet */
    assert(!_enqueued.empty());
    Packet* pkt = _enqueued.pop();

    if (_state_send==LosslessQueue::PAUSE_RECEIVED)
        _state_send = LosslessQueue::PAUSED;
    
    //mark on deque
    if (_queuesize > _K)
        pkt->set_flags(pkt->flags() | ECN_CE);

    _queuesize -= pkt->size();
    pkt->flow().logTraffic(*pkt, *this, TrafficLogger::PKT_DEPART);
    if (_logger) _logger->logQueue(*this, QueueLogger::PKT_SERVICE, *pkt);

    /* tell the packet to move on to the next pipe */
    pkt->sendOn();

    //_virtual_time += drainTime(pkt);

    if (!_enqueued.empty() && _state_send==LosslessQueue::READY) {
        /* schedule the next dequeue event */
        beginService();
    }
}
