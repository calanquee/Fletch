/* Ingress Processing (Normal Operation) */
/* Ingress Processing */

typedef bit<9>  egressSpec_t;
control netcacheIngress(  
	/* User */
    inout headers                       hdr,
    inout ingress_metadata                      meta,
    /* Intrinsic */
    in    ingress_intrinsic_metadata_t               ig_intr_md,
    in    ingress_intrinsic_metadata_from_parser_t   ig_prsr_md,
    inout ingress_intrinsic_metadata_for_deparser_t  ig_dprsr_md,
    inout ingress_intrinsic_metadata_for_tm_t        ig_tm_md){
	
	#include "regs/lock.p4"
	#include "regs/counter.p4"
	

	// Stage 0
	action set_hot_threshold(bit<16> hot_threshold) {
		hdr.inswitch_hdr.hot_threshold = hot_threshold;
	}

	@pragma stage 0
	table set_hot_threshold_tbl {
		actions = {
			set_hot_threshold;
		}
		default_action = set_hot_threshold(DEFAULT_HH_THRESHOLD);
		size = 1;
	}
	Hash<bit<16>>(HashAlgorithm_t.IDENTITY) hash_cm1_calc;
	Hash<bit<16>>(HashAlgorithm_t.CRC16) hash_cm2_calc;
	action hash_for_cm12() {
		hdr.inswitch_hdr.hashval_for_cm1 = hash_cm1_calc.get({
			meta.keyhilo,
			meta.keyhihilo,
			meta.keyhihihi
		});
		hdr.inswitch_hdr.hashval_for_cm2 =  hash_cm2_calc.get({
			meta.keyhilo,
			meta.keyhihilo,
			meta.keyhihihi
		});
	}
	@pragma stage 4
	table hash_for_cm12_tbl {
		key =  {
			hdr.op_hdr.optype: exact;
		}
		actions = {
			hash_for_cm12;
			NoAction;
		}
		default_action = NoAction();
		size = 2;
	}


	action l2l3_forward(egressSpec_t eport) {
		ig_tm_md.ucast_egress_port = eport;
	}

	@pragma stage 0
	table l2l3_forward_tbl {
		key = {
			hdr.ethernet_hdr.dstAddr: exact;
			hdr.ipv4_hdr.dstAddr: lpm;
		}
		actions = {
			l2l3_forward;
			NoAction;
		}
		default_action = NoAction();
		size = 16;
	}



	Hash<bit<16>>(HashAlgorithm_t.CRC32) hash_partition_calc1;
	Hash<bit<16>>(HashAlgorithm_t.CRC32) hash_partition_calc2;
	action hash_for_partition_cached() {
		hdr.inswitch_hdr.hashval_for_seq = hash_partition_calc1.get({
			hdr.key1_hdr.keyhilo,
			hdr.key1_hdr.keyhihilo,
			hdr.key1_hdr.keyhihihi
		});
	}
	action hash_for_partition() {
		hdr.inswitch_hdr.hashval_for_seq = hash_partition_calc2.get({
			meta.keyhilo,
			meta.keyhihilo,
			meta.keyhihihi
		});
	}
	@pragma stage 5
	table hash_for_partition_tbl {
		key = {
			hdr.op_hdr.optype: exact;
			hdr.inswitch_hdr.is_cached: exact;
		}
		actions = {
			hash_for_partition_cached;
			hash_for_partition;
			NoAction;
		}
		default_action = NoAction();
		size = 32;
	}

	action hash_partition(egressSpec_t eport) {
		// hdr.udp_hdr.dstPort = udpport;
		ig_tm_md.ucast_egress_port = eport;
	}
	// for dir
	@pragma stage 6
	table hash_partition_tbl {
		key = {
			hdr.op_hdr.optype: exact;
			hdr.op_hdr.keydepth: exact;
			hdr.inswitch_hdr.is_cached: exact;
			// RANGE_SURPPORT
			hdr.inswitch_hdr.hashval_for_seq[14:0]: range;
		}
		actions = {
			hash_partition;
			NoAction;
		}
		default_action = NoAction();
		size = 256;
	}

	action recirculation_partition(egressSpec_t eport,bit<10> server_sid) {
		ig_tm_md.ucast_egress_port = eport;
		hdr.inswitch_hdr.need_recirculation = 1;
		hdr.inswitch_hdr.server_sid = server_sid;
	}
	@pragma stage 10
	table prepare_for_recirculation_tbl {
		key = {
			hdr.op_hdr.optype: exact;
			ig_tm_md.ucast_egress_port: exact;
			hdr.inswitch_hdr.is_cached: exact;
			// meta.is_acquire_lock_8: exact;
		}
		actions = {
			recirculation_partition;
			NoAction;
		}
		default_action = NoAction();
		size = 16;
	}

	action set_is_acquire_lock() {
		meta.is_acquire_lock = 1;
	}
	action reset_is_acquire_lock() {
		meta.is_acquire_lock = 0;
	}
	@pragma stage 10
	table is_acquire_lock_tbl {
		key = {
			hdr.op_hdr.optype: exact;
			meta.is_cached_for_lock: exact;
			meta.is_acquire_lock_23456: exact;
			meta.is_acquire_lock_7: exact;
			meta.is_acquire_lock_8: exact;
			meta.is_acquire_lock_9: exact;
		}
		actions = {
			set_is_acquire_lock;
			reset_is_acquire_lock;
		}
		default_action = reset_is_acquire_lock();
		size = 32;
	}


	action cached_action_tbl1(bit<16> idx_for_latest_deleted_frequency_reg, bit<16> idx, bit<16> bitmap) {
		hdr.inswitch_hdr.idx_for_latest_deleted_frequency_reg = idx_for_latest_deleted_frequency_reg;
		hdr.inswitch_hdr.idx = idx;
		hdr.inswitch_hdr.is_cached = 1;
		hdr.inswitch_hdr.bitmap = bitmap;
	}
	@pragma stage 0
	table cache_lookup1_tbl {
		key =  {
			hdr.token_hdr.token: exact;
			hdr.key1_hdr.keyhilo: exact;
			hdr.key1_hdr.keyhihilo: exact;
			hdr.key1_hdr.keyhihihi: exact;
		}
		actions = {
			cached_action_tbl1;
			NoAction;
		}
		default_action = NoAction();
		size = LOOKUP_ENTRY_COUNT;
	}

	action cached_action_tbl2(bit<16> idx_for_latest_deleted_frequency_reg, bit<16> idx, bit<16> bitmap) {
		hdr.inswitch_hdr.idx_for_latest_deleted_frequency_reg = idx_for_latest_deleted_frequency_reg;
		hdr.inswitch_hdr.idx = idx;
		hdr.inswitch_hdr.is_cached = 1;
		hdr.inswitch_hdr.bitmap = bitmap;
	}
	@pragma stage 3
	table cache_lookup2_1_tbl {
		key =  {
			hdr.token_hdr.token: exact;
			hdr.key1_hdr.keyhilo: exact;
			hdr.key1_hdr.keyhihilo: exact;
			hdr.key1_hdr.keyhihihi: exact;
		}
		actions = {
			cached_action_tbl2;
			NoAction;
		}
		default_action = NoAction();
		size = 32768;
	}
	
	action cached_action_tbl3() {
		meta.is_cached_for_lock = 1;
	}
	@pragma stage 1
	table cache_lookup3_tbl {
		key =  {
			meta.keyhilo: exact;
			meta.keyhihilo: exact;
			meta.keyhihihi: exact;
		}
		actions = {
			cached_action_tbl3;
			NoAction;
		}
		default_action = NoAction();
		size = LOOKUP_ENTRY_COUNT;
	}

	action cached_action_tbl4() {
		meta.is_cached_for_lock = 1;
	}
	@pragma stage 2
	table cache_lookup4_tbl {
		key =  {
			meta.keyhilo: exact;
			meta.keyhihilo: exact;
			meta.keyhihihi: exact;
		}
		actions = {
			cached_action_tbl4;
			NoAction;
		}
		default_action = NoAction();
		size = LOOKUP_ENTRY_COUNT*2;
	}

	action set_client_sid(bit<10> client_sid) {
		hdr.inswitch_hdr.client_sid = client_sid;
		// ig_dprsr_md.drop_ctl = 1;
	}
	@pragma stage 2
	table prepare_for_cachehit_tbl {
		key =  {
			hdr.op_hdr.optype: exact;
			// ig_intr_md.ingress_port: exact;
			hdr.ipv4_hdr.srcAddr: lpm;
		}
		actions = {
			set_client_sid;
			NoAction;
		}
		default_action = set_client_sid(0); // deprecated: configured as set_client_sid(sids[0]) in ptf
		size = 32;
	}
	
	action set_val1_idx_for_permission(bit<4> val_id){
		hdr.inswitch_hdr.val_idx_for_permission = val_id; // valid value: 0, 1, 2, 3, 4 --> val1, val2, val3, val4, val5
		hdr.inswitch_hdr.is_val1_permission = 1;
	}
	action set_val2_idx_for_permission(bit<4> val_id){
		hdr.inswitch_hdr.val_idx_for_permission = val_id; // valid value: 0, 1, 2, 3, 4 --> val1, val2, val3, val4, val5
		hdr.inswitch_hdr.is_val2_permission = 1;
	}
	action set_val3_idx_for_permission(bit<4> val_id){
		hdr.inswitch_hdr.val_idx_for_permission = val_id; // valid value: 0, 1, 2, 3, 4 --> val1, val2, val3, val4, val5
		hdr.inswitch_hdr.is_val3_permission = 1;
	}
	action set_val4_idx_for_permission(bit<4> val_id){
		hdr.inswitch_hdr.val_idx_for_permission = val_id; // valid value: 0, 1, 2, 3, 4 --> val1, val2, val3, val4, val5
		hdr.inswitch_hdr.is_val4_permission = 1;
	}
	action set_val5_idx_for_permission(bit<4> val_id){
		hdr.inswitch_hdr.val_idx_for_permission = val_id; // valid value: 0, 1, 2, 3, 4 --> val1, val2, val3, val4, val5
		hdr.inswitch_hdr.is_val5_permission = 1;
	}
	@pragma stage 5
	table set_val_idx_for_permission_tbl {
		key =  {
			hdr.op_hdr.optype: exact;
			hdr.inswitch_hdr.is_cached: exact;
			hdr.inswitch_hdr.bitmap[4:0]: exact;
		}
		actions = {
			set_val1_idx_for_permission;
			set_val2_idx_for_permission;
			set_val3_idx_for_permission;
			set_val4_idx_for_permission;
			set_val5_idx_for_permission;
			NoAction;
		}
		default_action = NoAction();
		size = 96;
	}


	action forward_normal_response(egressSpec_t eport) {
		ig_tm_md.ucast_egress_port = eport;
	}
	// set server_sid for getreq 
	// if internal path hit, it will be used
	// if not, it do nothing
	action forward_special_response(egressSpec_t eport,bit<10> server_sid) {
		ig_tm_md.ucast_egress_port = eport;
		hdr.inswitch_hdr.server_sid = server_sid;
	}
	@pragma stage 7
	table ipv4_forward_tbl {
		key =  {
			hdr.op_hdr.optype: exact;
			hdr.ipv4_hdr.dstAddr: lpm;
		}
		actions = {
			forward_normal_response;
			forward_special_response;
			NoAction;
		}
		default_action = NoAction();
		size = 64;
	}

	action update_getreq_to_getreq_inswitch() {
		hdr.op_hdr.optype = GETREQ_INSWITCH;
		hdr.shadowtype_hdr.shadowtype = GETREQ_INSWITCH;
		hdr.shadowtype_hdr.setValid();
		hdr.inswitch_hdr.setValid();
	}
	action update_putreq_to_putreq_inswitch() {
		hdr.op_hdr.optype = PUTREQ_INSWITCH;
		hdr.shadowtype_hdr.shadowtype = PUTREQ_INSWITCH;
		hdr.shadowtype_hdr.setValid();
		hdr.inswitch_hdr.setValid();
	}
	action update_putreq_touch_to_putreq_inswitch() {
		hdr.op_hdr.optype = PUTREQ_INSWITCH;
		hdr.inswitch_hdr.is_cached = 0;
		hdr.shadowtype_hdr.shadowtype = PUTREQ_INSWITCH;
		hdr.shadowtype_hdr.setValid();
		hdr.inswitch_hdr.setValid();
	}
	action update_getreq_recir_to_getreq_inswitch() {
		hdr.op_hdr.optype = GETREQ_INSWITCH;
		hdr.shadowtype_hdr.shadowtype = GETREQ_INSWITCH;
		hdr.shadowtype_hdr.setValid();
		hdr.inswitch_hdr.setValid();
		hdr.seq_hdr.setInvalid(); // seq hdr is used for unlock
	}
	action update_warmupreq_to_netcache_warmupreq_inswitch() {
		hdr.op_hdr.optype = NETCACHE_WARMUPREQ_INSWITCH;
		hdr.shadowtype_hdr.shadowtype = NETCACHE_WARMUPREQ_INSWITCH;
		hdr.shadowtype_hdr.setValid();
		hdr.inswitch_hdr.setValid();
	}
	action update_netcache_valueupdate_to_netcache_valueupdate_inswitch() {
		hdr.op_hdr.optype = NETCACHE_VALUEUPDATE_INSWITCH;
		hdr.shadowtype_hdr.shadowtype = NETCACHE_VALUEUPDATE_INSWITCH;

		hdr.inswitch_hdr.setValid();

		// NOTE: NETCACHE_VALUEUPDATE does not need partition_tbl, as in-switch record must in the same pipeline of the ingress port, which can also send NETCACHE_VALUEUPDATE_ACK back to corresponding server
		// ig_tm_md.ucast_egress_port = standard_metadata.ingress_port;
		ig_tm_md.ucast_egress_port = ig_intr_md.ingress_port;
		// swap to set dstport as corresponding server.valueupdateserver port
		
		bit<16> tmp_port = hdr.udp_hdr.srcPort;
		hdr.udp_hdr.srcPort = hdr.udp_hdr.dstPort;
		hdr.udp_hdr.dstPort = tmp_port;
	}
	action recir_for_acquire_lock_failed(bit<9> recir_port) {
		ig_tm_md.ucast_egress_port = recir_port;
		ig_tm_md.bypass_egress = 1;
	}
	action recir_for_pre_getres(bit<9> recir_port) {
		hdr.op_hdr.optype = GETRES_UNLOCK;
		hdr.shadowtype_hdr.shadowtype = GETRES_UNLOCK;
		hdr.shadowtype_hdr.setValid();
		ig_tm_md.ucast_egress_port = recir_port;
		ig_tm_md.bypass_egress = 1;
	}
	action recir_for_pre_getres_wo_unlock(bit<9> recir_port) {
		hdr.op_hdr.optype = GETRES_NO_UNLOCK;
		hdr.shadowtype_hdr.shadowtype = GETRES_NO_UNLOCK;
		hdr.shadowtype_hdr.setValid();
		ig_tm_md.ucast_egress_port = recir_port;
		ig_tm_md.bypass_egress = 1;
	}
	@pragma stage 11
	table ig_port_forward_tbl {
		key =  {
			hdr.op_hdr.optype: exact;
			meta.is_acquire_lock: exact;
			meta.is_counter_equal_to_sequence: exact;
		}
		actions = {
			update_getreq_to_getreq_inswitch;
			update_getreq_recir_to_getreq_inswitch;
			update_putreq_to_putreq_inswitch;
			update_warmupreq_to_netcache_warmupreq_inswitch;
			update_netcache_valueupdate_to_netcache_valueupdate_inswitch;
			update_putreq_touch_to_putreq_inswitch;
			recir_for_acquire_lock_failed;
			recir_for_pre_getres;
			recir_for_pre_getres_wo_unlock;
			NoAction;
		}
		default_action = NoAction();
		size = 32;
	}

	action reset_is_cached() {
		hdr.inswitch_hdr.is_cached = 0;
	}
	@pragma stage 4
	table set_is_cached_tbl {
		key = {
			hdr.op_hdr.optype: exact;
			meta.is_cached_for_lock: exact;
		}
		actions = {
			reset_is_cached;
			NoAction;
		}
		default_action = NoAction();
		size = 4;
	}

	action is_key1() {
		meta.keyhilo = hdr.key1_hdr.keyhilo;
		meta.keyhihilo = hdr.key1_hdr.keyhihilo;
		meta.keyhihihi = hdr.key1_hdr.keyhihihi;
	}
	action is_key2() {
		meta.keyhilo = hdr.key2_hdr.keyhilo;
		meta.keyhihilo = hdr.key2_hdr.keyhihilo;
		meta.keyhihihi = hdr.key2_hdr.keyhihihi;
		meta.tmp_key2_keyhihihi = hdr.key2_hdr.keyhihihi;
	}
	action is_key3() {
		meta.keyhilo = hdr.key3_hdr.keyhilo;
		meta.keyhihilo = hdr.key3_hdr.keyhihilo;
		meta.keyhihihi = hdr.key3_hdr.keyhihihi;
		meta.tmp_key2_keyhihihi = hdr.key2_hdr.keyhihihi;
		meta.tmp_key3_keyhihihi = hdr.key3_hdr.keyhihihi;
	}
	action is_key4() {
		meta.keyhilo = hdr.key4_hdr.keyhilo;
		meta.keyhihilo = hdr.key4_hdr.keyhihilo;
		meta.keyhihihi = hdr.key4_hdr.keyhihihi;
		meta.tmp_key2_keyhihihi = hdr.key2_hdr.keyhihihi;
		meta.tmp_key3_keyhihihi = hdr.key3_hdr.keyhihihi;
		meta.tmp_key4_keyhihihi = hdr.key4_hdr.keyhihihi;
	}
	action is_key5() {
		meta.keyhilo = hdr.key5_hdr.keyhilo;
		meta.keyhihilo = hdr.key5_hdr.keyhihilo;
		meta.keyhihihi = hdr.key5_hdr.keyhihihi;
		meta.tmp_key2_keyhihihi = hdr.key2_hdr.keyhihihi;
		meta.tmp_key3_keyhihihi = hdr.key3_hdr.keyhihihi;
		meta.tmp_key4_keyhihihi = hdr.key4_hdr.keyhihihi;
		meta.tmp_key5_keyhihihi = hdr.key5_hdr.keyhihihi;
	}
	action is_key6() {
		meta.keyhilo = hdr.key6_hdr.keyhilo;
		meta.keyhihilo = hdr.key6_hdr.keyhihilo;
		meta.keyhihihi = hdr.key6_hdr.keyhihihi;
		meta.tmp_key2_keyhihihi = hdr.key2_hdr.keyhihihi;
		meta.tmp_key3_keyhihihi = hdr.key3_hdr.keyhihihi;
		meta.tmp_key4_keyhihihi = hdr.key4_hdr.keyhihihi;
		meta.tmp_key5_keyhihihi = hdr.key5_hdr.keyhihihi;
		meta.tmp_key6_keyhihihi = hdr.key6_hdr.keyhihihi;
	}
	action is_key7() {
		meta.keyhilo = hdr.key7_hdr.keyhilo;
		meta.keyhihilo = hdr.key7_hdr.keyhihilo;
		meta.keyhihihi = hdr.key7_hdr.keyhihihi;
		meta.tmp_key2_keyhihihi = hdr.key2_hdr.keyhihihi;
		meta.tmp_key3_keyhihihi = hdr.key3_hdr.keyhihihi;
		meta.tmp_key4_keyhihihi = hdr.key4_hdr.keyhihihi;
		meta.tmp_key5_keyhihihi = hdr.key5_hdr.keyhihihi;
		meta.tmp_key6_keyhihihi = hdr.key6_hdr.keyhihihi;
		meta.tmp_key7_keyhihihi = hdr.key7_hdr.keyhihihi;
	}
	action is_key8() {
		meta.keyhilo = hdr.key8_hdr.keyhilo;
		meta.keyhihilo = hdr.key8_hdr.keyhihilo;
		meta.keyhihihi = hdr.key8_hdr.keyhihihi;
		meta.tmp_key2_keyhihihi = hdr.key2_hdr.keyhihihi;
		meta.tmp_key3_keyhihihi = hdr.key3_hdr.keyhihihi;
		meta.tmp_key4_keyhihihi = hdr.key4_hdr.keyhihihi;
		meta.tmp_key5_keyhihihi = hdr.key5_hdr.keyhihihi;
		meta.tmp_key6_keyhihihi = hdr.key6_hdr.keyhihihi;
		meta.tmp_key7_keyhihihi = hdr.key7_hdr.keyhihihi;
		meta.tmp_key8_keyhihihi = hdr.key8_hdr.keyhihihi;
	}
	action is_key9() {
		meta.keyhilo = hdr.key9_hdr.keyhilo;
		meta.keyhihilo = hdr.key9_hdr.keyhihilo;
		meta.keyhihihi = hdr.key9_hdr.keyhihihi;
		meta.tmp_offset_for_lock9 = 1;
		meta.tmp_key2_keyhihihi = hdr.key2_hdr.keyhihihi;
		meta.tmp_key3_keyhihihi = hdr.key3_hdr.keyhihihi;
		meta.tmp_key4_keyhihihi = hdr.key4_hdr.keyhihihi;
		meta.tmp_key5_keyhihihi = hdr.key5_hdr.keyhihihi;
		meta.tmp_key6_keyhihihi = hdr.key6_hdr.keyhihihi;
		meta.tmp_key7_keyhihihi = hdr.key7_hdr.keyhihihi;
		meta.tmp_key8_keyhihihi = hdr.key8_hdr.keyhihihi;
		meta.tmp_key9_keyhihihi = hdr.key9_hdr.keyhihihi;
	}
	action is_key10() {
		meta.keyhilo = hdr.key10_hdr.keyhilo;
		meta.keyhihilo = hdr.key10_hdr.keyhihilo;
		meta.keyhihihi = hdr.key10_hdr.keyhihihi;
		meta.tmp_offset_for_lock9 = 2;
		meta.tmp_key2_keyhihihi = hdr.key2_hdr.keyhihihi;
		meta.tmp_key3_keyhihihi = hdr.key3_hdr.keyhihihi;
		meta.tmp_key4_keyhihihi = hdr.key4_hdr.keyhihihi;
		meta.tmp_key5_keyhihihi = hdr.key5_hdr.keyhihihi;
		meta.tmp_key6_keyhihihi = hdr.key6_hdr.keyhihihi;
		meta.tmp_key7_keyhihihi = hdr.key7_hdr.keyhihihi;
		meta.tmp_key8_keyhihihi = hdr.key8_hdr.keyhihihi;
		meta.tmp_key9_keyhihihi = hdr.key9_hdr.keyhihihi;
	}
	@pragma stage 0
	table get_last_key_tbl {
		key = {
			hdr.op_hdr.optype: exact;
			hdr.op_hdr.keydepth: exact;
		}
		actions = {
			is_key1;
			is_key2;
			is_key3;
			is_key4;
			is_key5;
			is_key6;
			is_key7;
			is_key8;
			is_key9;
			is_key10;
			NoAction;
		}
		default_action = NoAction();
		size = 64;
	}
	
	// if ig_intr_md.ingress_port belongs to pipe0 and we want it to pipe1 in the ingress phase
	// set the recir_port to recir_port of pipe0
	action recir_for_error_ingress(bit<9> recir_port){
		ig_tm_md.ucast_egress_port = recir_port;
		ig_tm_md.bypass_egress = 1;
		exit;
	}

	@pragma stage 0
	table recir_for_error_ingress_tbl {
		key = {
			hdr.op_hdr.optype: exact;
			ig_intr_md.ingress_port: exact;
		}
		actions = {
			recir_for_error_ingress;
			NoAction;
		}
		default_action = NoAction();
		size = 16;
	}

	action is_key2_unlock() {
		meta.tmp_key2_keyhihihi = hdr.seq_hdr.unlock_keyindex;
	}
	action is_key3_unlock() {
		meta.tmp_key3_keyhihihi = hdr.seq_hdr.unlock_keyindex;
	}
	action is_key4_unlock() {
		meta.tmp_key4_keyhihihi = hdr.seq_hdr.unlock_keyindex;
	}
	action is_key5_unlock() {
		meta.tmp_key5_keyhihihi = hdr.seq_hdr.unlock_keyindex;
	}
	action is_key6_unlock() {
		meta.tmp_key6_keyhihihi = hdr.seq_hdr.unlock_keyindex;
	}
	action is_key7_unlock() {
		meta.tmp_key7_keyhihihi = hdr.seq_hdr.unlock_keyindex;
	}
	action is_key8_unlock() {
		meta.tmp_key8_keyhihihi = hdr.seq_hdr.unlock_keyindex;
	}
	action is_key9_unlock() {
		meta.tmp_key9_keyhihihi = hdr.seq_hdr.unlock_keyindex;
	}
	action is_key2_unlock_res() {
		meta.tmp_key2_keyhihihi = hdr.key1_hdr.keyhihihi;
	}
	action is_key3_unlock_res() {
		meta.tmp_key3_keyhihihi = hdr.key1_hdr.keyhihihi;
		meta.tmp_key2_keyhihihi = hdr.key2_hdr.keyhihihi;
	}
	action is_key4_unlock_res() {
		meta.tmp_key4_keyhihihi = hdr.key1_hdr.keyhihihi;
		meta.tmp_key3_keyhihihi = hdr.key2_hdr.keyhihihi;
		meta.tmp_key2_keyhihihi = hdr.key3_hdr.keyhihihi;
	}
	action is_key5_unlock_res() {
		meta.tmp_key5_keyhihihi = hdr.key1_hdr.keyhihihi;
		meta.tmp_key4_keyhihihi = hdr.key2_hdr.keyhihihi;
		meta.tmp_key3_keyhihihi = hdr.key3_hdr.keyhihihi;
		meta.tmp_key2_keyhihihi = hdr.key4_hdr.keyhihihi;
	}
	action is_key6_unlock_res() {
		meta.tmp_key6_keyhihihi = hdr.key1_hdr.keyhihihi;
		meta.tmp_key5_keyhihihi = hdr.key2_hdr.keyhihihi;
		meta.tmp_key4_keyhihihi = hdr.key3_hdr.keyhihihi;
		meta.tmp_key3_keyhihihi = hdr.key4_hdr.keyhihihi;
		meta.tmp_key2_keyhihihi = hdr.key5_hdr.keyhihihi;
	}
	action is_key7_unlock_res() {
		meta.tmp_key7_keyhihihi = hdr.key1_hdr.keyhihihi;
		meta.tmp_key6_keyhihihi = hdr.key2_hdr.keyhihihi;
		meta.tmp_key5_keyhihihi = hdr.key3_hdr.keyhihihi;
		meta.tmp_key4_keyhihihi = hdr.key4_hdr.keyhihihi;
		meta.tmp_key3_keyhihihi = hdr.key5_hdr.keyhihihi;
		meta.tmp_key2_keyhihihi = hdr.key6_hdr.keyhihihi;
	}
	action is_key8_unlock_res() {
		meta.tmp_key8_keyhihihi = hdr.key1_hdr.keyhihihi;
		meta.tmp_key7_keyhihihi = hdr.key2_hdr.keyhihihi;
		meta.tmp_key6_keyhihihi = hdr.key3_hdr.keyhihihi;
		meta.tmp_key5_keyhihihi = hdr.key4_hdr.keyhihihi;
		meta.tmp_key4_keyhihihi = hdr.key5_hdr.keyhihihi;
		meta.tmp_key3_keyhihihi = hdr.key6_hdr.keyhihihi;
		meta.tmp_key2_keyhihihi = hdr.key7_hdr.keyhihihi;
	}
	action is_key9_unlock_res() {
		meta.tmp_key9_keyhihihi = hdr.key1_hdr.keyhihihi;
		meta.tmp_key8_keyhihihi = hdr.key2_hdr.keyhihihi;
		meta.tmp_key7_keyhihihi = hdr.key3_hdr.keyhihihi;
		meta.tmp_key6_keyhihihi = hdr.key4_hdr.keyhihihi;
		meta.tmp_key5_keyhihihi = hdr.key5_hdr.keyhihihi;
		meta.tmp_key4_keyhihihi = hdr.key6_hdr.keyhihihi;
		meta.tmp_key3_keyhihihi = hdr.key7_hdr.keyhihihi;
		meta.tmp_key2_keyhihihi = hdr.key8_hdr.keyhihihi;
	}
	action is_key10_unlock_res() {
		meta.tmp_key9_keyhihihi = hdr.key2_hdr.keyhihihi;
		meta.tmp_key8_keyhihihi = hdr.key3_hdr.keyhihihi;
		meta.tmp_key7_keyhihihi = hdr.key4_hdr.keyhihihi;
		meta.tmp_key6_keyhihihi = hdr.key5_hdr.keyhihihi;
		meta.tmp_key5_keyhihihi = hdr.key6_hdr.keyhihihi;
		meta.tmp_key4_keyhihihi = hdr.key7_hdr.keyhihihi;
		meta.tmp_key3_keyhihihi = hdr.key8_hdr.keyhihihi;
		meta.tmp_key2_keyhihihi = hdr.key9_hdr.keyhihihi;
	}
	@pragma stage 1
	table get_key_for_unlock_tbl {
		key = {
			hdr.op_hdr.optype: exact;
			hdr.op_hdr.keydepth: exact;
			hdr.op_hdr.real_keydepth: exact;
		}
		actions = {
			is_key2_unlock;
			is_key3_unlock;
			is_key4_unlock;
			is_key5_unlock;
			is_key6_unlock;
			is_key7_unlock;
			is_key8_unlock;
			is_key9_unlock;
			is_key2_unlock_res;
			is_key3_unlock_res;
			is_key4_unlock_res;
			is_key5_unlock_res;
			is_key6_unlock_res;
			is_key7_unlock_res;
			is_key8_unlock_res;
			is_key9_unlock_res;
			is_key10_unlock_res;
			NoAction;
		}
		default_action = NoAction();
		size = 256;
	}

	action set_meta_offset_for_key9() {
		meta.tmp_offset_for_lock9 = 1;
	}
	action set_meta_offset_for_key10() {
		meta.tmp_offset_for_lock9 = 2;
	}
	@pragma stage 8
	table set_meta_offset_for_lock9_tbl {
		key = {
			hdr.op_hdr.optype: exact;
			hdr.op_hdr.real_keydepth: exact;
		}
		actions = {
			set_meta_offset_for_key9;
			set_meta_offset_for_key10;
			NoAction;
		}
		default_action = NoAction();
		size = 16;
	}	
	action return_setvalid(egressSpec_t eport) {
		ig_tm_md.ucast_egress_port = eport;
	}
	@pragma stage 9
	table return_setvalid_tbl {
		key =  {
			hdr.op_hdr.optype: exact;
			hdr.ipv4_hdr.srcAddr: lpm;
		}
		actions = {
			return_setvalid;
			NoAction;
		}
		default_action = NoAction();
		size = 4;
	}
	apply{
		// stage 0
		if (!hdr.key1_hdr.isValid()) {
			l2l3_forward_tbl.apply(); // forward traditional packet
		}else{
			hdr.udp_hdr.checksum = 0;
			// stage 0
			access_counter_tbl.apply();
			set_hot_threshold_tbl.apply(); // set inswitch_hdr.hot_threshold
			cache_lookup1_tbl.apply(); // for get and put cached: 16384
			get_last_key_tbl.apply();
			recir_for_error_ingress_tbl.apply();
			
			// stage 1
			cache_lookup3_tbl.apply(); // for lock: 16384
			get_key_for_unlock_tbl.apply();
			check_counter_tbl.apply();
			// if (meta.is_counter == hdr.seq_hdr.sequence_number_from_server) {
			// 	meta.is_counter_equal_to_sequence = 1;
			// }

			// stage 2
			cache_lookup4_tbl.apply(); // for lock: 16384*2
			prepare_for_cachehit_tbl.apply();

			// stage 3
			cache_lookup2_1_tbl.apply(); // for get and put cached: 16384*2
			access_lock_key2_tbl.apply();
			
			
			// stage 4
			// cache_lookup2_2_tbl.apply();
			hash_for_cm12_tbl.apply();
			set_is_cached_tbl.apply();
			access_lock_key3_tbl.apply();
			
			
			// stage 5
			set_val_idx_for_permission_tbl.apply(); // for fetching permission metadata (access inswitch_hdr.val_idx_for_permission)
			hash_for_partition_tbl.apply(); // for hash partition (including startkey of SCANREQ)
			access_lock_key4_tbl.apply();

			// stage 6
			access_lock_key5_tbl.apply();
			hash_partition_tbl.apply(); // update egress_port for normal/speical response packets

			// stage 7
			ipv4_forward_tbl.apply();
			
			// stage 8
			set_meta_offset_for_lock9_tbl.apply();
			

			// stage 9
			return_setvalid_tbl.apply();
			access_lock_key6_tbl.apply();
			access_lock_key7_tbl.apply();
			access_lock_key8_tbl.apply();
			access_lock_key9_tbl.apply();

			// stage 10
			prepare_for_recirculation_tbl.apply();
			is_acquire_lock_tbl.apply();
			
			// stage 11
			ig_port_forward_tbl.apply(); // update key1_hdr.optype (update egress_port for NETCACHE_VALUEUPDATE)
		}
	}
}