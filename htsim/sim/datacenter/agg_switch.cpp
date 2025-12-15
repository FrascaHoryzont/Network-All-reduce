// -*- c-basic-offset: 4; indent-tabs-mode: nil -*-

#include "agg_switch.h"
#include "uec.h"
#include "eventlist.h"

// Costruttore: eredita fat-tree switch
AggSwitch::AggSwitch(EventList& ev,
                     const string& name,
                     uint32_t id,
                     simtime_picosec delay,
                     FatTreeTopology* ft)
    : FatTreeSwitch(ev, name, FatTreeSwitch::AGG, id, delay, ft) {
}


// ----------------------------------------------------------------------
// RECEIVEPACKET OVERRIDE
// ----------------------------------------------------------------------
void AggSwitch::receivePacket(Packet& pkt) {
    stats.pkts_in++;
    cout << "pkt type = " << int(pkt.type()) << endl;


    // Se NON è un collective → usa il comportamento normale
    if (pkt.type() != UEC_COLLECTIVE) {
        FatTreeSwitch::receivePacket(pkt);
        return;
    }

    // Cast sicuro
    auto* cp = static_cast<UecCollectivePacket*>(&pkt);
    const auto& h = cp->hdr();

    // Chiave aggregazione
    Key key{h.collective_id, h.block_id};
    auto& st = table[key];

    // Prima volta?
    if (st.received == 0) {
        st.expected = h.expected;
        st.acc.assign(h.elems, 0.0);
        st.t_first = eventlist().now();
    }

    // Accumula
    double* p = cp->payload();
    for (uint16_t i = 0; i < h.elems; ++i) {
        st.acc[i] += p[i];
    }

    st.received++;
    stats.bytes_saved += pkt.size();

    // Questo pacchetto non va inoltrato
    pkt.free();

    // Se completo → fai emit
    if (st.received == st.expected) {
        finalize_and_emit(key, st, h.op);
        table.erase(key);
        stats.groups_done++;
    }
}


// ----------------------------------------------------------------------
// EMISSIONE PACCHETTO RIDOTTO
// ----------------------------------------------------------------------
void AggSwitch::finalize_and_emit(const Key& key, AggState& st, uint8_t op) {

    // Header out
    UecCollectiveHdr out_h{};
    out_h.collective_id = key.coll;
    out_h.block_id = key.block;
    out_h.elems = (uint16_t)st.acc.size();
    out_h.expected = 1;
    out_h.op = op;

    auto* out = UecCollectivePacket::newpkt(out_h,
                                            st.acc.data(),
                                            st.acc.size() * sizeof(double));

    // NOTA IMPORTANTE:
    // NON devi riciclare la route: lascia decidere al FatTreeSwitch
    FatTreeSwitch::receivePacket(*out);

    stats.pkts_out++;
}
