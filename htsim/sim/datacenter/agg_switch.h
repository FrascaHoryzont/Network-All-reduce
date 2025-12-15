// -*- c-basic-offset: 4; indent-tabs-mode: nil -*-

#ifndef _AGG_SWITCH_H
#define _AGG_SWITCH_H

#include "fat_tree_switch.h"
#include "uec.h"

// Uno switch che estende FatTreeSwitch ma aggiunge Network All-Reduce
class AggSwitch : public FatTreeSwitch {
public:
    AggSwitch(EventList& ev,
              const string& name,
              uint32_t id,
              simtime_picosec delay,
              FatTreeTopology* ft);

    void receivePacket(Packet& pkt) override;

    struct Stats {
        uint64_t groups_done = 0;
        uint64_t pkts_in = 0;
        uint64_t pkts_out = 0;
        uint64_t bytes_saved = 0;
    } stats;

private:
    // Chiave per identificare un gruppo di aggregazione
    struct Key {
        uint32_t coll;
        uint32_t block;
        bool operator==(const Key& o) const {
            return coll == o.coll && block == o.block;
        }
    };

    struct KeyHash {
        size_t operator()(const Key& k) const {
            return (size_t(k.coll) << 32) ^ k.block;
        }
    };

    // Stato parziale aggregazione
    struct AggState {
        uint32_t expected = 0;
        uint32_t received = 0;
        vector<double> acc;
        simtime_picosec t_first = 0;
    };

    unordered_map<Key, AggState, KeyHash> table;

    void finalize_and_emit(const Key& key, AggState& st, uint8_t op);
};

#endif
