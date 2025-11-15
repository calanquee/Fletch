/* Ingress Processing (Normal Operation) */

/* Egress Processing */


control netcacheEgress(    /* User */
    inout headers                          hdr,
    inout egress_metadata                         meta,
    /* Intrinsic */    
    in    egress_intrinsic_metadata_t                  eg_intr_md,
    in    egress_intrinsic_metadata_from_parser_t      eg_prsr_md,
    inout egress_intrinsic_metadata_for_deparser_t     eg_dprsr_md,
    inout egress_intrinsic_metadata_for_output_port_t  eg_oport_md){ 
	#include "regs/cm.p4"
	#include "regs/cache_frequency.p4"
	#include "regs/latest.p4"
	// #include "regs/deleted.p4"
	// #include "regs/seq.p4"
	#include "regs/val.p4"

	// action NoAction() {
	// }	
	// Stage 0
	action save_client_udpport() {
		hdr.clone_hdr.client_udpport = hdr.udp_hdr.srcPort;
	}

	@pragma stage 2
	table save_client_udpport_tbl {
		key = {
			hdr.op_hdr.optype: exact;
		}
		actions = {
			save_client_udpport;
			NoAction;
		}
		default_action = NoAction();
		size = 8;
	}

	// Stage 1

	action set_server_sid_and_port(bit<10> server_sid) {
		// hdr.clone_hdr.setValid();
		hdr.clone_hdr.server_sid = server_sid;
		hdr.clone_hdr.server_udpport = hdr.udp_hdr.dstPort; // dstport is serverport for GETREQ_INSWITCH
	}

	action reset_server_sid() {
		hdr.clone_hdr.server_sid = 0;
	}

	action alais_server_sid_and_port(){
		// for recirculation
		hdr.clone_hdr.server_sid = hdr.inswitch_hdr.server_sid;
		hdr.clone_hdr.server_udpport = hdr.udp_hdr.dstPort; // dstport is serverport for GETREQ_INSWITCH
	}
	@pragma stage 2
	table prepare_for_cachepop_tbl {
		key = {
			hdr.op_hdr.optype: exact;
			eg_intr_md.egress_port: exact;
		}
		actions = {
			alais_server_sid_and_port;
			set_server_sid_and_port;
			reset_server_sid;
			NoAction;
		}
		default_action = reset_server_sid();
		size = 32;
	}

	// Stage 2

	action set_is_hot() {
		meta.is_hot = 1;
	}

	action reset_is_hot() {
		meta.is_hot = 0;
	}

	@pragma stage 2
	table is_hot_tbl {
		key = {
			meta.cm1_predicate: exact;
			meta.cm2_predicate: exact;
			meta.cm3_predicate: exact;

		}
		actions = {
			set_is_hot;
			reset_is_hot;
		}
		default_action = reset_is_hot();
		size = 1;
	}


	// Stage 8

	action set_is_lastclone() {
		meta.is_lastclone_for_pktloss = 1;
		//hdr.debug_hdr.is_lastclone_for_pktloss = 1;
	}

	action reset_is_lastclone_lastscansplit() {
		meta.is_lastclone_for_pktloss = 0;
		//hdr.debug_hdr.is_lastclone_for_pktloss = 0;
	}

	@pragma stage 2
	table lastclone_lastscansplit_tbl {
		key = {
			hdr.op_hdr.optype: exact;
			hdr.clone_hdr.clonenum_for_pktloss: exact;
		}
		actions = {
			set_is_lastclone;
			reset_is_lastclone_lastscansplit;
		}
		default_action = reset_is_lastclone_lastscansplit();
		size = 8;
	}

	// Stage 9
	// action clone_e2e(MirrorId_t mir_ses){
    //     eg_dprsr_md.mirror_type = MIRROR_TYPE_E2E;
    //     meta.egr_mir_ses = mir_ses;
	// 	meta.pkt_type = PKT_TYPE_MIRROR;
	// }
#define clone_e2e(mir_ses)                      \
    eg_dprsr_md.mirror_type = MIRROR_TYPE_E2E;  \
	meta.egr_mir_ses = ##mir_ses;  				\
	meta.pkt_type = PKT_TYPE_MIRROR;			

	action update_netcache_warmupreq_inswitch_to_netcache_warmupreq_inswitch_pop_clone_for_pktloss_and_warmupack(bit<10> switchos_sid,bit<16> reflector_port) {
		hdr.op_hdr.optype = NETCACHE_WARMUPREQ_INSWITCH_POP;
		hdr.shadowtype_hdr.shadowtype = NETCACHE_WARMUPREQ_INSWITCH_POP;
		hdr.udp_hdr.dstPort = reflector_port;
		hdr.clone_hdr.clonenum_for_pktloss = 3; // 3 ACKs (drop w/ 3 -> clone w/ 2 -> clone w/ 1 -> clone w/ 0 -> drop and clone for WARMUPACK to client)

		hdr.clone_hdr.setValid(); // NOTE: clone_hdr.server_sid is reset as 0 in process_scanreq_split_tbl and prepare_for_cachepop_tbl

		//hdr.eg_intr_md.egress_port = hdr.port; // set eport to switchos
		clone_e2e(switchos_sid);
		// commemt for debug
		eg_dprsr_md.drop_ctl = 1; // Disable unicast, but enable mirroring
		// clone to switchos
		// clone(CloneType.E2E, (bit<32>)switchos_sid);
	}

	action forward_netcache_warmupreq_inswitch_pop_clone_for_pktloss_and_warmupack(bit<10> switchos_sid) {
		hdr.clone_hdr.clonenum_for_pktloss = hdr.clone_hdr.clonenum_for_pktloss - 1;
		hdr.udp_hdr.hdrlen = hdr.udp_hdr.hdrlen - INSWITCH_HEADER_OFFSET;
		clone_e2e(switchos_sid); // clone to switchos
		// // clone(CloneType.E2E, (bit<32>)switchos_sid);
	}

	action clone_get_response_to_client(bit<10> client_sid) {
		hdr.op_hdr.optype = GETRES_FINAL;
		hdr.shadowtype_hdr.shadowtype = GETRES_FINAL;
		hdr.clone_hdr.clonenum_for_pktloss = 1;
		hdr.clone_hdr.setValid();
		clone_e2e(client_sid); // clone to client
		eg_dprsr_md.drop_ctl = 1;
	}

	action forward_get_response_to_client(bit<10> client_sid) {
		hdr.clone_hdr.clonenum_for_pktloss = hdr.clone_hdr.clonenum_for_pktloss - 1;
		clone_e2e(client_sid); // clone to client
	}

	action update_getres_final_to_getres_ack_by_mirroring(bit<10> server_sid,bit<16> client_port) {
		hdr.op_hdr.optype = GETRES_ACK;
		// DEPRECATED: udp.srcport will be set as server_worker_port_start in update_ipmac_srcport_tbl
		// NOTE: we must set udp.srcPort now, otherwise it will dropped by parser/deparser due to NO reserved udp ports (current pkt will NOT access update_ipmac_srcport_tbl for server2client as current devport is server instead of client)
		hdr.udp_hdr.srcPort = client_port;
		hdr.udp_hdr.dstPort = hdr.clone_hdr.client_udpport;
		hdr.clone_hdr.setInvalid();
		clone_e2e(server_sid);
		eg_dprsr_md.drop_ctl = 1; // Disable unicast, but enable mirroring
	}

	action update_getres_no_unlock_to_getres_ack_pkt_loss_by_mirroring(bit<10> server_sid) {
		hdr.op_hdr.optype = GETRES_PKT_LOSS_ACK;
		// DEPRECATED: udp.srcport will be set as server_worker_port_start in update_ipmac_srcport_tbl
		// NOTE: we must set udp.srcPort now, otherwise it will dropped by parser/deparser due to NO reserved udp ports (current pkt will NOT access update_ipmac_srcport_tbl for server2client as current devport is server instead of client)
		// hdr.udp_hdr.srcPort = client_port;
		// hdr.udp_hdr.dstPort = hdr.clone_hdr.client_udpport;
		// hdr.clone_hdr.setInvalid();
		clone_e2e(server_sid);
		eg_dprsr_md.drop_ctl = 1; // Disable unicast, but enable mirroring
	}

	action update_netcache_warmupreq_inswitch_pop_to_warmupack_by_mirroring(bit<10> client_sid,bit<16> server_port) {
		hdr.op_hdr.optype = WARMUPACK;
		// DEPRECATED: udp.srcport will be set as server_worker_port_start in update_ipmac_srcport_tbl
		// NOTE: we must set udp.srcPort now, otherwise it will dropped by parser/deparser due to NO reserved udp ports (current pkt will NOT access update_ipmac_srcport_tbl for server2client as current devport is server instead of client)
		hdr.udp_hdr.srcPort = server_port;
		hdr.udp_hdr.dstPort = hdr.clone_hdr.client_udpport;
		hdr.udp_hdr.hdrlen = hdr.udp_hdr.hdrlen - INSWITCH_HEADER_OFFSET;
		hdr.shadowtype_hdr.setInvalid();
		hdr.inswitch_hdr.setInvalid();
		hdr.clone_hdr.setInvalid();
		clone_e2e(client_sid);
		eg_dprsr_md.drop_ctl = 1; // Disable unicast, but enable mirroring
		 // clone to client (hdr.inswitch_hdr.client_sid)
		// clone(CloneType.E2E, (bit<32>)client_sid);
	}

	action update_getreq_inswitch_to_getreq() {
		hdr.op_hdr.optype = GETREQ;

		hdr.shadowtype_hdr.setInvalid();
		hdr.inswitch_hdr.setInvalid();
		hdr.clone_hdr.setInvalid();
	}

	action update_getreq_inswitch_to_getres_by_mirroring(bit<10> client_sid,bit<16> server_port,bit<8> stat) {

		meta.is_hit = 1;
			
		// hdr.op_hdr.optype = GETRES;
		// hdr.shadowtype_hdr.shadowtype = GETRES;
		hdr.stat_hdr.stat = stat;
		// hdr.stat_hdr.nodeidx_foreval = SWITCHIDX_FOREVAL;
		// // NOTE: we must set udp.srcPort now, otherwise it will dropped by parser/deparser due to NO reserved udp ports (current pkt will NOT access update_ipmac_srcport_tbl for server2client as current devport is server instead of client)

		
		// hdr.inswitch_hdr.need_clone = 1;
		// hdr.inswitch_hdr.setInvalid();
		// hdr.stat_hdr.setValid();
		// hdr.clone_hdr.setInvalid();
		
		// clone_e2e(client_sid);
		// eg_dprsr_md.drop_ctl = 1; // Disable unicast, but enable mirroring
		 // clone to client (hdr.inswitch_hdr.client_sid)

	}


	action update_cache_pop_inswitch_to_cache_pop_inswitch_ack_drop_and_clone(bit<10>switchos_sid,bit<16> reflector_port) {
		hdr.op_hdr.optype = CACHE_POP_INSWITCH_ACK;
		hdr.udp_hdr.dstPort = reflector_port;

		// NOTE: we add/remove vallen and value headers in add_remove_value_header_tbl
		hdr.shadowtype_hdr.setInvalid();
		hdr.seq_hdr.setInvalid();
		hdr.inswitch_hdr.setInvalid();
		hdr.stat_hdr.setInvalid();

		clone_e2e(switchos_sid);
		eg_dprsr_md.drop_ctl = 1; // Disable unicast, but enable mirroring
		 // clone to switchos
		// clone(CloneType.E2E, (bit<32>)switchos_sid);
	}

	//action forward_cache_pop_inswitch_ack() {
	//}

	action update_putreq_inswitch_to_putreq_seq() {
		hdr.op_hdr.optype = PUTREQ_SEQ;
		hdr.shadowtype_hdr.shadowtype = PUTREQ_SEQ;

		hdr.inswitch_hdr.setInvalid();
		hdr.seq_hdr.setValid();

		//hdr.eg_intr_md.egress_port = hdr.eport;
	}

	action update_putreq_inswitch_to_netcache_putreq_seq_cached() {
		hdr.op_hdr.optype = NETCACHE_PUTREQ_SEQ_CACHED;
		hdr.shadowtype_hdr.shadowtype = NETCACHE_PUTREQ_SEQ_CACHED;

		hdr.inswitch_hdr.setInvalid();
		hdr.seq_hdr.setValid();

		//hdr.eg_intr_md.egress_port = hdr.eport;
	}

	action update_delreq_inswitch_to_delreq_seq() {
		hdr.op_hdr.optype = DELREQ_SEQ;
		hdr.shadowtype_hdr.shadowtype = DELREQ_SEQ;

		hdr.inswitch_hdr.setInvalid();
		hdr.seq_hdr.setValid();

		//hdr.eg_intr_md.egress_port = hdr.eport;
	}

	action update_delreq_inswitch_to_netcache_delreq_seq_cached() {
		hdr.op_hdr.optype = NETCACHE_DELREQ_SEQ_CACHED;
		hdr.shadowtype_hdr.shadowtype = NETCACHE_DELREQ_SEQ_CACHED;

		hdr.inswitch_hdr.setInvalid();
		hdr.seq_hdr.setValid();

		//hdr.eg_intr_md.egress_port = hdr.eport;
	}


	action update_cache_evict_loadfreq_inswitch_to_cache_evict_loadfreq_inswitch_ack_drop_and_clone(bit<16> optype ,bit<10> switchos_sid,bit<16> reflector_port) {
		hdr.op_hdr.optype = optype;
		hdr.udp_hdr.dstPort = reflector_port;

		hdr.shadowtype_hdr.setInvalid();
		hdr.inswitch_hdr.setInvalid();
		hdr.frequency_hdr.setValid();

		clone_e2e(switchos_sid);
		eg_dprsr_md.drop_ctl = 1; // Disable unicast, but enable mirroring
		 // clone to switchos
		// clone(CloneType.E2E, (bit<32>)switchos_sid);
	}

	//action forward_cache_evict_loadfreq_inswitch_ack() {
	//}

	action update_netcache_valueupdate_inswitch_to_netcache_valueupdate_ack() {
		hdr.op_hdr.optype = NETCACHE_VALUEUPDATE_ACK;

		hdr.shadowtype_hdr.setInvalid();
		hdr.seq_hdr.setInvalid();
		hdr.inswitch_hdr.setInvalid();
		hdr.stat_hdr.setInvalid();

		// NOTE: egress_port has already been set in ig_port_forward_tbl at ingress pipeline
	}

	action update_netcache_cache_pop_inswitch_nlatest_to_cache_pop_inswitch_ack_drop_and_clone(bit<10> switchos_sid,bit<16> reflector_port) {
		hdr.op_hdr.optype = CACHE_POP_INSWITCH_ACK;
		hdr.udp_hdr.dstPort = reflector_port;

		// NOTE: we add/remove vallen and value headers in add_remove_value_header_tbl
		hdr.shadowtype_hdr.setInvalid();
		hdr.seq_hdr.setInvalid();
		hdr.inswitch_hdr.setInvalid();
		hdr.stat_hdr.setInvalid();
		
		clone_e2e(switchos_sid);
		eg_dprsr_md.drop_ctl = 1; // Disable unicast, but enable mirroring
		 // clone to switchos
		// clone(CloneType.E2E, (bit<32>)switchos_sid);
	}

	action update_getreq_inswitch_to_netcache_getreq_pop_clone_for_pktloss_and_getreq(bit<10> switchos_sid,bit<16> reflector_port) {
		hdr.op_hdr.optype = NETCACHE_GETREQ_POP;
		hdr.udp_hdr.dstPort = reflector_port;
		hdr.clone_hdr.clonenum_for_pktloss = 3; // 3 ACKs (drop w/ 3 -> clone w/ 2 -> clone w/ 1 -> clone w/ 0 -> drop and clone for GETREQ to server)

		hdr.shadowtype_hdr.setInvalid();
		hdr.inswitch_hdr.setInvalid();
		hdr.clone_hdr.setValid(); // NOTE: clone_hdr.server_sid has been set in prepare_for_cachepop_tbl
		hdr.udp_hdr.hdrlen = hdr.udp_hdr.hdrlen + 8; 

		clone_e2e(switchos_sid);
		//hdr.eg_intr_md.egress_port = hdr.port; // set eport to switchos
		eg_dprsr_md.drop_ctl = 1; // Disable unicast, but enable mirroring
		 // clone to switchos
		// clone(CloneType.E2E, (bit<32>)switchos_sid);
	}

	action forward_netcache_getreq_pop_clone_for_pktloss_and_getreq(bit<10> switchos_sid) {
		hdr.clone_hdr.clonenum_for_pktloss = hdr.clone_hdr.clonenum_for_pktloss - 1;

		clone_e2e(switchos_sid); // clone to switchos
		// clone(CloneType.E2E, (bit<32>)switchos_sid);
	}

	action update_netcache_getreq_pop_to_getreq_by_mirroring(bit<10> server_sid) {
		hdr.op_hdr.optype = GETREQ;
		// Keep original udp.srcport (aka client udp port)
		hdr.udp_hdr.dstPort = hdr.clone_hdr.server_udpport;

		hdr.clone_hdr.setInvalid();

		hdr.udp_hdr.hdrlen = hdr.udp_hdr.hdrlen - 8;

		clone_e2e(server_sid);
		eg_dprsr_md.drop_ctl = 1; // Disable unicast, but enable mirroring
		 // clone to client (hdr.inswitch_hdr.client_sid)
		// clone(CloneType.E2E, (bit<32>)server_sid);
	}
	action update_getreq_inswitch_invalid_to_getreq() {
		hdr.op_hdr.optype = GETREQ;

		hdr.shadowtype_hdr.setInvalid();
		hdr.inswitch_hdr.setInvalid();
		hdr.clone_hdr.setInvalid();
		clone_e2e(hdr.clone_hdr.server_sid);
		eg_dprsr_md.drop_ctl = 1; // Disable unicast, but enable mirroring
	}
	action update_setvalid_inswitch_to_setvalid_inswitch_ack() {
		hdr.op_hdr.optype = SETVALID_INSWITCH_ACK;
		hdr.udp_hdr.dstPort = hdr.clone_hdr.client_udpport;

		hdr.shadowtype_hdr.setInvalid();
		hdr.inswitch_hdr.setInvalid();
	}
	action drop_for_debug(){
		eg_dprsr_md.drop_ctl = 1;
	}
	@pragma stage 6
	table eg_port_forward_tbl {
		key = {
			hdr.op_hdr.optype: exact;
			hdr.inswitch_hdr.is_cached: exact;
			meta.is_latest: exact;
			meta.is_hot:exact;
			meta.is_deleted: exact;
			hdr.inswitch_hdr.client_sid: exact;
			meta.is_lastclone_for_pktloss: exact;
			hdr.clone_hdr.server_sid: exact;
		}
		actions = {
			drop_for_debug;
			update_netcache_warmupreq_inswitch_to_netcache_warmupreq_inswitch_pop_clone_for_pktloss_and_warmupack;
			forward_netcache_warmupreq_inswitch_pop_clone_for_pktloss_and_warmupack;
			update_netcache_warmupreq_inswitch_pop_to_warmupack_by_mirroring;
			update_getreq_inswitch_to_getreq;
			update_getreq_inswitch_to_getres_by_mirroring;

			update_getreq_inswitch_to_netcache_getreq_pop_clone_for_pktloss_and_getreq;
			forward_netcache_getreq_pop_clone_for_pktloss_and_getreq;
			update_netcache_getreq_pop_to_getreq_by_mirroring;
			
			//update_cache_pop_inswitch_to_cache_pop_inswitch_ack_clone_for_pktloss; // clone for first CACHE_POP_INSWITCH_ACK
			//forward_cache_pop_inswitch_ack_clone_for_pktloss; // not last clone of CACHE_POP_INSWITCH_ACK
			update_cache_pop_inswitch_to_cache_pop_inswitch_ack_drop_and_clone; // clone for first CACHE_POP_INSWITCH_ACK (not need to clone for duplication due to switchos-side timeout-and-retry)
			//forward_cache_pop_inswitch_ack; // last clone of CACHE_POP_INSWITCH_ACK
			update_putreq_inswitch_to_putreq_seq;
			update_putreq_inswitch_to_netcache_putreq_seq_cached;
			update_delreq_inswitch_to_delreq_seq;
			update_delreq_inswitch_to_netcache_delreq_seq_cached;
			update_getreq_inswitch_invalid_to_getreq;

			update_cache_evict_loadfreq_inswitch_to_cache_evict_loadfreq_inswitch_ack_drop_and_clone; // clone to reflector and hence switchos; but not need clone for pktloss due to switchos-side timeout-and-retry
			//forward_cache_evict_loadfreq_inswitch_ack;
			update_netcache_valueupdate_inswitch_to_netcache_valueupdate_ack;
			update_netcache_cache_pop_inswitch_nlatest_to_cache_pop_inswitch_ack_drop_and_clone; // clone for first CACHE_POP_INSWITCH_ACK (not need to clone for duplication due to switchos-side timeout-and-retry)
			
			update_setvalid_inswitch_to_setvalid_inswitch_ack;
			clone_get_response_to_client;
			forward_get_response_to_client;
			update_getres_final_to_getres_ack_by_mirroring;
			update_getres_no_unlock_to_getres_ack_pkt_loss_by_mirroring;
			NoAction;
		}
		default_action = NoAction();
		size = 4096;
	}

	// TODO
	action update_getres_back_to_getreq(){
		hdr.op_hdr.optype = GETREQ;

		hdr.shadowtype_hdr.setInvalid();
		hdr.inswitch_hdr.setInvalid();
		hdr.clone_hdr.setInvalid();
		// clone to client for permission check fails
		clone_e2e(hdr.inswitch_hdr.client_sid);
		eg_dprsr_md.drop_ctl = 1; // Disable unicast, but enable mirroring
	}
	action real_update_getreq_inswitch_to_getres_by_mirroring(bit<10> recir_sid){
		// keydepth = 1, meta.is_hit = 1 , meta.is_permission = 1 , hdr.inswitch_hdr.need_recirculation = 1
		// it will be send to client
		hdr.op_hdr.optype = GETRES;
		hdr.shadowtype_hdr.shadowtype = GETRES;
		// hdr.stat_hdr.stat = stat;
		hdr.stat_hdr.nodeidx_foreval = SWITCHIDX_FOREVAL;
		// // NOTE: we must set udp.srcPort now, otherwise it will dropped by parser/deparser due to NO reserved udp ports (current pkt will NOT access update_ipmac_srcport_tbl for server2client as current devport is server instead of client)
		// // hdr.udp_hdr.srcPort = server_port;
		// hdr.udp_hdr.dstPort = hdr.clone_hdr.client_udpport;
		hdr.udp_hdr.srcPort = 1152;
		hdr.udp_hdr.dstPort = hdr.clone_hdr.client_udpport;
		// hdr.inswitch_hdr.need_clone = 1;
		hdr.inswitch_hdr.setInvalid();
		hdr.stat_hdr.setValid();
		hdr.clone_hdr.setInvalid();
		
		// clone_e2e(hdr.inswitch_hdr.client_sid);
		clone_e2e(recir_sid);

		eg_dprsr_md.drop_ctl = 1; // Disable unicast, but enable mirroring
	}
	action recirculate_getres(){
		// keydepth = 2~10, meta.is_hit = 1 , meta.is_permission = 1 , hdr.inswitch_hdr.need_recirculation = 1
		// it will be recirculated
		hdr.op_hdr.optype = GETREQ_RECIR;

		hdr.shadowtype_hdr.setValid(); // for unlock
		hdr.seq_hdr.setValid(); // for unlock
		hdr.inswitch_hdr.setInvalid();
		
		hdr.clone_hdr.setInvalid();
		
		// remove key 1
		hdr.op_hdr.keydepth = hdr.op_hdr.keydepth - 1; 
		hdr.udp_hdr.hdrlen = hdr.udp_hdr.hdrlen - 9;
		
		// store key1_hdr in shadowtype_hdr
		hdr.shadowtype_hdr.shadowtype = GETREQ_RECIR;
		hdr.seq_hdr.unlock_keyindex = hdr.key1_hdr.keyhihihi;

		hdr.key1_hdr.setInvalid();
		hdr.token_hdr.setInvalid();
		// eg_dprsr_md.drop_ctl = 1; // debug
	}
	@pragma stage 8
	table special_port_forward_for_recirculation_tbl {
		key = {
			hdr.op_hdr.optype: exact;
			hdr.op_hdr.keydepth:range;
			meta.is_hit:exact;
			hdr.inswitch_hdr.is_permission:exact;
			hdr.inswitch_hdr.need_recirculation:exact;
		}
		actions = {
			drop_for_debug;
			update_getres_back_to_getreq;
			real_update_getreq_inswitch_to_getres_by_mirroring;
			recirculate_getres;
			NoAction;
		}
		default_action = NoAction();
		size = 128;
	}
	
	// NOTE: we need to set dstip/mac for pkt from server to client especially for those w/ cache hit
	action update_ipmac_srcport_server2client(bit<48> client_mac,bit<48> server_mac, bit<32> client_ip,bit<32> server_ip, bit<16> server_port) {
		hdr.ethernet_hdr.srcAddr = server_mac;
		hdr.ethernet_hdr.dstAddr = client_mac;
		hdr.ipv4_hdr.srcAddr = server_ip;
		hdr.ipv4_hdr.dstAddr = client_ip;
		hdr.udp_hdr.srcPort = server_port;
	}

	action update_ipmac_srcport_server2client_for_getres_final(bit<16> server_port) {
		hdr.udp_hdr.srcPort = server_port;
	}

	// NOTE: as we use software link, switch_mac/ip = reflector_mac/ip
	// NOTE: although we use client_port to update srcport here, reflector does not care about the specific value of srcport
	// NOTE: we must reset srcip/mac for pkt from reflector.cp2dpserver, otherwise the ACK pkt will be dropped by reflector.NIC as srcip/mac = NIC.ip/mac
	action update_ipmac_srcport_switch2switchos(bit<48> client_mac,bit<48> switch_mac, bit<32> client_ip,bit<32> switch_ip, bit<16> client_port) {
		hdr.ethernet_hdr.srcAddr = client_mac;
		hdr.ethernet_hdr.dstAddr = switch_mac;
		hdr.ipv4_hdr.srcAddr = client_ip;
		hdr.ipv4_hdr.dstAddr = switch_ip;
		hdr.udp_hdr.srcPort = client_port;
	}
	// NOTE: for NETCACHE_GETREQ_POP, we need to keep the original srcip/mac, i.e., client ip/mac, which will be used by server for GETRES and by switch for ipv4_forward_tbl
	// NOTE: for NETCACHE_WARMUPREQ_INSWITCH_POP, both keep or reset original srcip/mac are ok as src/dst.ip/mac of WARMUPACK will be set by switch based on client_sid/egress_port
	action update_dstipmac_switch2switchos(bit<48> switch_mac, bit<32> switch_ip) {
		hdr.ethernet_hdr.dstAddr = switch_mac;
		hdr.ipv4_hdr.dstAddr = switch_ip;
	}

	// NOTE: for pkt from client, we should keep original srcip/mac, i.e., client ip/mac, which will be used by server later for response and by switch for ipv4_forward_tbl
	action update_dstipmac_client2server(bit<48> server_mac, bit<32> server_ip) {
		hdr.ethernet_hdr.dstAddr = server_mac;
		hdr.ipv4_hdr.dstAddr = server_ip;
	}
	// NOTE: for NETCACHE_VALUEUPDATE_ACK, we must reset srcip/mac, i.e., server ip/mac, otherwise it will be ignored by server NIC
	action update_ipmac_srcport_client2server(bit<48> client_mac,bit<48> server_mac, bit<32> client_ip,bit<32> server_ip, bit<16> client_port) {
		hdr.ethernet_hdr.srcAddr = client_mac;
		hdr.ethernet_hdr.dstAddr = server_mac;
		hdr.ipv4_hdr.srcAddr = client_ip;
		hdr.ipv4_hdr.dstAddr = server_ip;
		hdr.udp_hdr.srcPort = client_port;
	}

	action update_ipmac_srcport_dstport_client2server(bit<48> client_mac,bit<48> server_mac, bit<32> client_ip,bit<32> server_ip, bit<16> client_port, bit<16> server_port) {
		hdr.ethernet_hdr.srcAddr = client_mac;
		hdr.ethernet_hdr.dstAddr = server_mac;
		hdr.ipv4_hdr.srcAddr = client_ip;
		hdr.ipv4_hdr.dstAddr = server_ip;
		hdr.udp_hdr.srcPort = client_port;
		hdr.udp_hdr.dstPort = server_port;
	}

	// NOTE: dstport of REQ, RES, and notification has been updated in partition_tbl, server, and eg_port_forward_tbl
	@pragma stage 7
	table update_ipmac_srcport_tbl {
		key = {
			hdr.op_hdr.optype: exact;
			eg_intr_md.egress_port: exact;
		}
		actions = {
			update_ipmac_srcport_server2client; // focus on dstip and dstmac to corresponding client; use server[0] as srcip and srcmac; use server_worker_port_start as srcport
			update_ipmac_srcport_switch2switchos; // focus on dstip and dstmac to reflector; use client[0] as srcip and srcmac; use constant (123) as srcport
			update_dstipmac_switch2switchos; // keep original srcip/mac for NETCACHE_GETREQ_POP
			update_dstipmac_client2server; // focus on dstip and dstmac to corresponding server; NOT change srcip, srcmac, and srcport
			update_ipmac_srcport_client2server; // reset srcip/mac for NETCACHE_VALUEUPDATE_ACK
			update_ipmac_srcport_dstport_client2server; // for GETRES_PKT_LOSS_ACK
			update_ipmac_srcport_server2client_for_getres_final; // for GETRES_FINAL
			NoAction;
		}
		default_action = NoAction();
		size = 256;
	}

	action assign_val1_to_permission() {
		meta.tmp_permission = hdr.val1_hdr.vallo;
	}
	action assign_val2_to_permission() {
		meta.tmp_permission = hdr.val2_hdr.vallo;
	}
	action assign_val3_to_permission() {
		meta.tmp_permission = hdr.val3_hdr.vallo;
	}
	action assign_val4_to_permission() {
		meta.tmp_permission = hdr.val4_hdr.vallo;
	}
	action assign_val5_to_permission() {
		meta.tmp_permission = hdr.val5_hdr.vallo;
	}
	@pragma stage 6
	table assgn_permission_tbl {
		key = {
			hdr.inswitch_hdr.is_cached: exact;
			hdr.inswitch_hdr.val_idx_for_permission: exact;
		}
		actions = {
			assign_val1_to_permission;
			assign_val2_to_permission;
			assign_val3_to_permission;
			assign_val4_to_permission;
			assign_val5_to_permission;
			NoAction;
		}
		default_action = NoAction();
		size = 8;
	}

	action update_is_permissioned_as_1(){
		hdr.inswitch_hdr.is_permission = 1;
	}
	action update_is_permissioned_as_0(){
		hdr.inswitch_hdr.is_permission = 0;
	}
	@pragma stage 7
	table permission_check_tbl {
		key = {
			hdr.inswitch_hdr.is_cached: exact;
			hdr.inswitch_hdr.client_uid_gid[31:16]: exact; // uid
			hdr.inswitch_hdr.client_uid_gid[15:0]: exact; // gid
			meta.tmp_permission[24:24]: exact;
			meta.tmp_permission[21:21]: exact;
			meta.tmp_permission[18:18]: exact;
		}
		actions = {
			update_is_permissioned_as_1;
			update_is_permissioned_as_0;
		}
		default_action = update_is_permissioned_as_0();
		size = 16;
	}

	action update_pktlen(bit<16> udplen) {
		hdr.udp_hdr.hdrlen = udplen;
		// hdr.ipv4_hdr.totalLen = iplen;
	}
	action add_pktlen(bit<16> udplen_delta) {
		hdr.udp_hdr.hdrlen = hdr.udp_hdr.hdrlen + udplen_delta;
		meta.subtract_or_add = 0;
		// meta.hdrlen_delta = 0;
		// hdr.ipv4_hdr.totalLen = hdr.ipv4_hdr.totalLen + iplen_delta;
	}
	action subtract_pktlen(bit<16> udplen_delta) {
		// bit<16> tmp_res;
		// tmp_res = hdr.udp_hdr.hdrlen;
		// tmp_res = tmp_res |-| udplen_delta;
		// hdr.udp_hdr.hdrlen = hdr.udp_hdr.hdrlen - 
		meta.hdrlen_delta = udplen_delta;
		meta.subtract_or_add = 1;
		// hdr.udp_hdr.hdrlen = tmp_res;
		// hdr.ipv4_hdr.totalLen = hdr.ipv4_hdr.totalLen - iplen_delta;
	}
	// Qingxiu: reset meta.subtract_or_add and meta.hdrlen_delta since Tofino sets them as 1 by default
	action reset_metadata_for_update_pktlen(){
		meta.subtract_or_add = 0;
		meta.hdrlen_delta = 0;
	}

	@pragma stage 7
	table update_pktlen_tbl {
		key = {
			hdr.op_hdr.optype: exact;
			hdr.vallen_hdr.vallen: range;
			// meta.is_hit: exact;
			// hdr.inswitch_hdr.is_cached: exact;
		}
		actions = {
			update_pktlen;
			add_pktlen;
			subtract_pktlen;
			// Qingxiu: modify it from NoAction to the following
			reset_metadata_for_update_pktlen;
		}
		default_action = reset_metadata_for_update_pktlen(); // not change udp_hdr.hdrlen (GETREQ/GETREQ_POP/GETREQ_NLATEST)
		size = 256;
	}


	// Qingxiu: used by VALUEUPDATE
	action set_vallo1_to_meta() {
		meta.tmp_permission = hdr.val1_hdr.vallo;
	}
	@pragma stage 1
	table set_permission_to_meta_for_update_cache_tbl {
		key = {
			hdr.op_hdr.optype: exact;
			hdr.inswitch_hdr.is_cached: exact;
		}
		actions = {
			set_vallo1_to_meta;
			NoAction;
		}
		default_action = NoAction();
		size = 2;
	}

	action remove_all() {
		hdr.vallen_hdr.setInvalid();
		hdr.val1_hdr.setInvalid();
		hdr.val2_hdr.setInvalid();
		hdr.val3_hdr.setInvalid();
		hdr.val4_hdr.setInvalid();
		hdr.val5_hdr.setInvalid();
		hdr.val6_hdr.setInvalid();
		hdr.val7_hdr.setInvalid();
		hdr.val8_hdr.setInvalid();
		hdr.val9_hdr.setInvalid();
		hdr.val10_hdr.setInvalid();
		hdr.val11_hdr.setInvalid();
		hdr.val12_hdr.setInvalid();
		hdr.val13_hdr.setInvalid();
		hdr.val14_hdr.setInvalid();
		hdr.val15_hdr.setInvalid();
		hdr.val16_hdr.setInvalid();
		// Qingxiu: reset bitmap
		hdr.inswitch_hdr.bitmap = 0;
		// Qingxiu: debug pktlen
		// hdr.key1_hdr.keyhihilo = hdr.udp_hdr.hdrlen;
	}

	@pragma stage 9
	table add_and_remove_value_header_tbl {
		key = {
			hdr.op_hdr.optype: exact;
		}
		actions = {
			remove_all;
			NoAction;
		}
		default_action = remove_all();
		size = 64;
	}

	action set_requested_uid_gid(bit<32> client_uid_gid) {
		hdr.inswitch_hdr.client_uid_gid = client_uid_gid;
	}
	action debug(){
		hdr.inswitch_hdr.client_uid_gid = 0xFFFFFFFF;
	}
	@pragma stage 0
	table set_requested_uid_gid_tbl {
		key =  {
			hdr.op_hdr.optype: exact;
			hdr.inswitch_hdr.is_cached: exact;
			hdr.ipv4_hdr.srcAddr: lpm;
		}
		actions = {
			set_requested_uid_gid;
			debug;
		}
		default_action = debug();
		size = 2;
	}


	action update_ip_mac(bit<48> srcmac, bit<48> dstmac, bit<32> srcip, bit<32> dstip) {
		hdr.ethernet_hdr.srcAddr = srcmac;
		hdr.ethernet_hdr.dstAddr = dstmac;
		hdr.ipv4_hdr.srcAddr = srcip;
		hdr.ipv4_hdr.dstAddr = dstip;
	}
	@pragma stage 11
	table set_ip_mac_for_cachehit_response_tbl {
		key = {
			hdr.op_hdr.optype: exact;
			hdr.inswitch_hdr.client_sid: exact;
		}
		actions = {
			update_ip_mac;
			NoAction;
		}
		default_action = NoAction();
		size = 8;
	}

	action res_from_server1() {
		meta.is_deleted = 1;
	}
	action res_from_server2() {
		meta.is_deleted = 0;
	}
	@pragma stage 3
	table get_eport_for_getres_for_pkt_loss_tbl {
		key = {
			hdr.op_hdr.optype: exact;
			hdr.ipv4_hdr.srcAddr: lpm;
		}
		actions = {
			res_from_server1;
			res_from_server2;
			NoAction;
		}
		default_action = NoAction();
		size = 32;
	}


	apply{
		// [IMPORTANT]
		// Only prepare_for_cachepop_tbl will reset clone_hdr.server_sid as 0 by default, while process_scanreq_split_tbl only resets meta.remain_scannum by default (it ony modifies clone_hdr.server_sid for SCANREQ_SPLIT) -> MUST be very careful for all pkt types which will use clone_hdr.server_sid
		// For GETREQ_INSWITCH, clone_hdr.server_sid is NOT reset at process_scanreq_split_tbl, and is only set based on eport at prepare_for_cachepop_tbl -> OK
		// For SCANREQ_SPLIT, after setting server_sid based on split_hdr.globalserveridx at process_scanreq_split_tbl, it needs to invoke NoAction() explicitly in prepare_for_cachepop_tbl to avoid reset server_sid
		// For NETCACHE_GETREQ_POP, after inheriting clone_hdr.server_sid from GETREQ_INSWITCH, process_scanreq_split does NOT reset clone_hdr.server_sid by default, and it needs to invoke NoAction() explicitly in prepare_for_cachepop_tbl to avoid reset server_sid


		// stage 0
		if (hdr.key1_hdr.isValid()) {
			set_requested_uid_gid_tbl.apply();
			access_latest_tbl.apply(); // NOTE: latest_reg corresponds to stats.validity in NetCache paper, which will be used to *invalidate* the value by PUT/DELREQ
			access_cm1_tbl.apply();
			access_cm2_tbl.apply();
			
			// stage 1
			set_permission_to_meta_for_update_cache_tbl.apply();
			update_vallen_tbl.apply();
			access_cm3_tbl.apply();
			access_cache_frequency_tbl.apply();
			
			// stage 2
			prepare_for_cachepop_tbl.apply(); // reset clone_hdr.server_sid by default here
			save_client_udpport_tbl.apply(); // save udp.dstport (client port) for cache hit response of GETREQ/PUTREQ/DELREQ and PUTREQ/DELREQ_CASE1
			is_hot_tbl.apply();
			lastclone_lastscansplit_tbl.apply(); // Qingxiu: may need to configure for cloning
			update_vallo1_tbl.apply(); // NOTE: value registers do not reply on op_hdr.optype, they only rely on meta.access_val_mode, which is set by update_vallen_tbl in stage 1
			update_valhi1_tbl.apply();
			update_vallo9_tbl.apply();
			update_valhi9_tbl.apply();

			// stage 3
			update_vallo2_tbl.apply();
			update_valhi2_tbl.apply();
			update_vallo5_tbl.apply();

			get_eport_for_getres_for_pkt_loss_tbl.apply();
			
			// stage 4
			update_vallo3_tbl.apply();
			update_valhi3_tbl.apply();
			update_valhi13_tbl.apply();
			
			// stage 5
			update_vallo4_tbl.apply();
			update_valhi4_tbl.apply();
			update_vallo13_tbl.apply();
			

			// stage 6
			assgn_permission_tbl.apply();
			eg_port_forward_tbl.apply(); // including scan forwarding
			update_valhi5_tbl.apply();
			update_vallo14_tbl.apply();
			update_valhi14_tbl.apply();

			// stage 7
			update_ipmac_srcport_tbl.apply(); // Update ip, mac, and srcport for RES to client and notification to switchos
			update_pktlen_tbl.apply();
			permission_check_tbl.apply();
			update_vallo10_tbl.apply();
			update_valhi10_tbl.apply();
			update_vallo6_tbl.apply();
			update_valhi6_tbl.apply();
			
			
			// stage 8
			special_port_forward_for_recirculation_tbl.apply(); // including scan forwarding
			update_vallo11_tbl.apply();
			update_valhi11_tbl.apply();
			update_vallo12_tbl.apply();
			update_valhi12_tbl.apply();

			// Stage 9
			if(meta.subtract_or_add == 1){
				hdr.udp_hdr.hdrlen = hdr.udp_hdr.hdrlen - meta.hdrlen_delta;
			}
			add_and_remove_value_header_tbl.apply(); // Add or remove vallen and val according to optype and vallen
			

			// stage 10
			if(hdr.udp_hdr.isValid()) {
				hdr.ipv4_hdr.totalLen = hdr.udp_hdr.hdrlen + 20;
			}
			update_vallo15_tbl.apply();
			update_valhi15_tbl.apply();
			update_vallo16_tbl.apply();
			update_valhi16_tbl.apply();

			// stage 11
			update_vallo7_tbl.apply();
			update_valhi7_tbl.apply();
			update_vallo8_tbl.apply();
			update_valhi8_tbl.apply();

			set_ip_mac_for_cachehit_response_tbl.apply();
		}
	}
}