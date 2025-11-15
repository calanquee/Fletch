/* Packet Header Types */
typedef bit<8>  pkt_type_t;
const pkt_type_t PKT_TYPE_NORMAL = 1;
const pkt_type_t PKT_TYPE_MIRROR = 2;
header mirror_h {
  	pkt_type_t  pkt_type;
}

// PUT ipv4 in T-PHV

header ethernet_t {
    bit<48> dstAddr;
    bit<48> srcAddr;
    bit<16> etherType;
}

header ipv4_t {
    bit<4> version;
    bit<4> ihl;
    bit<8> diffserv;
    bit<16> totalLen;
    bit<16> identification;
    bit<3> flags;
    bit<13> fragOffset;
    bit<8> ttl;
    bit<8> protocol;
    bit<16> hdrChecksum;
    bit<32> srcAddr;
    bit<32> dstAddr;
}

header udp_t {
    bit<16> srcPort;
    bit<16> dstPort;
    bit<16> hdrlen;
    bit<16> checksum;
}


header key_t{
    bit<32> keyhilo;
    bit<16> keyhihilo;
    bit<16> keyhihihi;
}

header op_t {
	// 4B
    bit<16> optype;
	bit<8> real_keydepth;
    bit<8> keydepth;
}

header vallen_t {
    bit<16> vallen;
}

header val_t {
    bit<32> vallo;
    bit<32> valhi;
}

header shadowtype_t {
    bit<16> shadowtype;
}

header seq_t {
	bit<16> unlock_keyindex;
	bit<16> seq_padding;
}

// NOTE: inswicth_t affects INSWITCH_PREV_BYTES in packet_format.h
header inswitch_t {
	// 32-bit container
	bit<16> hot_threshold;
	bit<32> client_uid_gid; // get value from ingress
	// 32-bit containers
	bit<16> hashval_for_cm1; // at most 64K
	bit<16> hashval_for_cm2; // at most 64K
	bit<16> inswitch_padding; // at most 64K
	
	bit<10> server_sid; // clone to client for cache hit; NOTE: clone_e2e sets eg_intr_md_for_mb.mirror_id w/ 10 bits
	bit<1> is_val1_permission;
	bit<1> is_val2_permission;
	bit<1> is_val3_permission;
	bit<1> is_val4_permission;
	bit<1> is_val5_permission;
	bit<1> padding0;
	
	bit<16> hashval_for_seq; // at most 32K
	
	bit<10> client_sid; // clone to client for cache hit; NOTE: clone_e2e sets eg_intr_md_for_mb.mirror_id w/ 10 bits
	bit<4> val_idx_for_permission; // for fetching permission metadata
	bit<2> padding2;
	// 4B
	
	bit<1> is_cached;
	bit<1> is_sampled;
	// bit<2> need_clone; //Qingxiu: for recirculation decision
	bit<1> need_recirculation; //0 1
	bit<1> is_permission;
	bit<3> padding3; // Qingxiu: for cache lookup table index
	bit<1> is_file; // Qingxiu: use it to update pktlen for cache_pop_inswitch pkt
	
	bit<16> idx_for_latest_deleted_frequency_reg; // Qingxiu: for latest deleted register
	bit<16> bitmap; // index for dynamic allocations
	bit<16> idx; // index for in-switch cache; Qingxiu: must be in the last one and if update in-switch header, plz update INSWITCH_PREV_BYTES
}

header stat_t {
	bit<8> stat;
	bit<16> nodeidx_foreval; // cache hit: 0xFFFF; cache miss: [0, servernum-1]
}

header clone_t {
	// 8B
	bit<16> clonenum_for_pktloss;
	bit<16> client_udpport;
	bit<16> server_udpport;
	bit<10> server_sid; // clone to server for SCANREQ_SPLIT or last cloned NETCACHE_GETREQ_POP
	bit<6> padding;
}

header frequency_t {
	bit<32> frequency;
}
header token_t {
	bit<8> token;
}

struct ingress_metadata {
    bit<32> keyhilo; // for hash_for_cm
    bit<16> keyhihilo;
    bit<16> keyhihihi;
	
	bit<2> tmp_offset_for_lock9;
	bit<1> is_cached_for_lock;
	bit<1> is_wrong_pipeline; // if wrong pipeline, do not lock or unlock
	bit<1> is_acquire_lock_23456;
	bit<1> is_acquire_lock_7;
	bit<1> is_acquire_lock_8;
	bit<1> is_acquire_lock_9;

	bit<8> is_acquire_lock;

	bit<16> tmp_key2_keyhihihi;
	bit<16> tmp_key3_keyhihihi;
	bit<16> tmp_key4_keyhihihi;
	bit<16> tmp_key5_keyhihihi;
	bit<16> tmp_key6_keyhihihi;
	bit<16> tmp_key7_keyhihihi;
	bit<16> tmp_key8_keyhihihi;
	bit<16> tmp_key9_keyhihihi;
}

struct egress_metadata {
	bit<16> hdrlen_delta; // jz: solve tofino bug
	
	MirrorId_t egr_mir_ses; // 10 bits, tofino define
	bit<1> cm1_predicate;
	bit<1> cm2_predicate;
	bit<1> cm3_predicate;
	bit<1> is_hit;
	bit<1> subtract_or_add; // 1 sub 2 add 3/0 noaction
	bit<1> paddings1;

	bit<1> is_latest; // if the entry is latest
	bit<1> is_deleted; // if the entry is deleted
	bit<4> access_val_mode; // 0: not access val_reg; 1: get; 2: set_and_get; 3: reset_and_get	
	bit<1> is_hot;
	bit<1> is_lastclone_for_pktloss;
	
	pkt_type_t  pkt_type; // 8 bits

	bit<32> tmp_permission; // Qingxiu: read permission data from 
}
// Header instances
struct headers {
	ethernet_t ethernet_hdr;
	ipv4_t ipv4_hdr;
	udp_t udp_hdr;
	op_t op_hdr;
	// 4B
	// 8B
	key_t key1_hdr;
	key_t key2_hdr;
	key_t key3_hdr;
	key_t key4_hdr;
	key_t key5_hdr;
	key_t key6_hdr;
	key_t key7_hdr;
	key_t key8_hdr;
	key_t key9_hdr;
	key_t key10_hdr;
	vallen_t vallen_hdr;
	val_t val1_hdr;
	val_t val2_hdr;
	val_t val3_hdr;
	val_t val4_hdr;
	val_t val5_hdr;
	val_t val6_hdr;
	val_t val7_hdr;
	val_t val8_hdr;
	val_t val9_hdr;
	val_t val10_hdr;
	val_t val11_hdr;
	val_t val12_hdr;
	val_t val13_hdr;
	val_t val14_hdr;
	val_t val15_hdr;
	val_t val16_hdr;
	shadowtype_t shadowtype_hdr;
	seq_t seq_hdr;
	inswitch_t inswitch_hdr;
	stat_t stat_hdr;
	clone_t clone_hdr;
	frequency_t frequency_hdr;
	token_t token_hdr;
}

@pa_container_size("ingress","hdr.ethernet_hdr.etherType", 8,8)
@pa_container_size("egress","hdr.ethernet_hdr.etherType", 8,8)
@pa_container_size("ingress","hdr.ipv4_hdr.ttl", 8)
@pa_container_size("egress","hdr.ipv4_hdr.ttl", 8)
@pa_container_size("ingress","hdr.ipv4_hdr.protocol", 8)
@pa_container_size("egress","hdr.ipv4_hdr.protocol", 8)
@pa_container_size("ingress","hdr.ipv4_hdr.diffserv", 8)
@pa_container_size("egress","hdr.ipv4_hdr.diffserv", 8)
@pa_container_size("ingress","hdr.ipv4_hdr.hdrChecksum", 8, 8)
@pa_container_size("egress","hdr.ipv4_hdr.hdrChecksum", 8, 8)
// @pa_container_size("ingress","hdr.udp_hdr.hdrlen", 16)
// @pa_container_size("egress","hdr.udp_hdr.hdrlen", 16)

// @pa_container_size("egress","meta.permission_check_value",16)
@pa_container_size("egress","meta.pkt_type",8)
// @pa_container_size("ingress","meta.keyhihihi", 16)
// @pa_container_size("ingress","hdr.shadowtype_hdr.shadowtype", 16)
// @pa_container_size("egress","hdr.shadowtype_hdr.shadowtype", 16)

@pa_container_size("egress", "hdr.val6_hdr.vallo",32)

// @pa_container_size("ingress","hdr.clone_hdr.clonenum_for_pktloss", 8,8)
@pa_container_size("ingress","hdr.clone_hdr.server_udpport", 8, 8)
@pa_container_size("ingress","hdr.clone_hdr.client_udpport", 8, 8)
// @pa_container_size("egress","hdr.clone_hdr.clonenum_for_pktloss", 8,8)
@pa_container_size("egress","hdr.clone_hdr.server_udpport", 8, 8)
@pa_container_size("egress","hdr.clone_hdr.client_udpport", 8, 8)

// @pa_container_size("ingress", "meta.tmp_offset_for_lock9", "meta.is_acquire_lock", "meta.is_cached_for_lock", "meta.is_wrong_pipeline", "meta.metadata_padding", 8)

// @pa_container_size("ingress","hdr.shadowtype_hdr.shadowtype", 16)
// @pa_container_size("egress","hdr.shadowtype_hdr.shadowtype", 16)
// @pa_container_size("ingress","hdr.inswitch_hdr.hashval_for_seq", 16)
// @pa_container_size("ingress","hdr.inswitch_hdr.idx", 16)
// @pa_container_size("ingress", "hdr.inswitch_hdr.bitmap", 16)
// @pa_container_size("egress","hdr.inswitch_hdr.hashval_for_seq", 16)
// @pa_container_size("egress","hdr.inswitch_hdr.idx", 16)
// @pa_container_size("egress", "hdr.inswitch_hdr.bitmap", 16)
// @pa_container_size("ingress","hdr.key1_hdr.keyhihihi", 16)
// @pa_container_size("egress","hdr.key1_hdr.keyhihihi", 16)
// @pa_container_size("ingress","hdr.key2_hdr.keyhihihi", 16)
// @pa_container_size("egress","hdr.key2_hdr.keyhihihi", 16)
// @pa_container_size("ingress","hdr.key3_hdr.keyhihihi", 16)
// @pa_container_size("egress","hdr.key3_hdr.keyhihihi", 16)
// @pa_container_size("ingress","hdr.key4_hdr.keyhihihi", 16)
// @pa_container_size("egress","hdr.key4_hdr.keyhihihi", 16)
// @pa_container_size("ingress","hdr.key5_hdr.keyhihihi", 16)
// @pa_container_size("egress","hdr.key5_hdr.keyhihihi", 16)
// @pa_container_size("ingress","hdr.key6_hdr.keyhihihi", 16)
// @pa_container_size("egress","hdr.key6_hdr.keyhihihi", 16)
// @pa_container_size("ingress","hdr.key7_hdr.keyhihihi", 16)
// @pa_container_size("egress","hdr.key7_hdr.keyhihihi", 16)
// @pa_container_size("ingress","hdr.key8_hdr.keyhihihi", 16)
// @pa_container_size("egress","hdr.key8_hdr.keyhihihi", 16)
// @pa_container_size("ingress","hdr.key9_hdr.keyhihihi", 16)
// @pa_container_size("egress","hdr.key9_hdr.keyhihihi", 16)
// @pa_container_size("ingress","hdr.key10_hdr.keyhihihi", 16)
// @pa_container_size("egress","hdr.key10_hdr.keyhihihi", 16)

// @pa_container_size("ingress","hdr.key2_hdr.keylolo", 8, 8, 8, 8)
// @pa_container_size("egress","hdr.key2_hdr.keylolo", 8, 8, 8, 8)
// @pa_container_size("ingress","hdr.key2_hdr.keylohi", 8, 8, 8, 8)
// @pa_container_size("egress","hdr.key2_hdr.keylohi", 8, 8, 8, 8)
// @pa_container_size("ingress","hdr.key2_hdr.keyhilo", 8, 8, 8, 8)
// @pa_container_size("egress","hdr.key2_hdr.keyhilo", 8, 8, 8, 8)
// @pa_container_size("ingress","hdr.key3_hdr.keylolo", 8, 8, 8, 8)
// @pa_container_size("egress","hdr.key3_hdr.keylolo", 8, 8, 8, 8)
// @pa_container_size("ingress","hdr.key3_hdr.keylohi", 8, 8, 8, 8)
// @pa_container_size("egress","hdr.key3_hdr.keylohi", 8, 8, 8, 8)
// @pa_container_size("ingress","hdr.key3_hdr.keyhilo", 8, 8, 8, 8)
// @pa_container_size("egress","hdr.key3_hdr.keyhilo", 8, 8, 8, 8)
// @pa_container_size("ingress","hdr.op_hdr.keydepth", 16)
// @pa_container_size("egress","hdr.op_hdr.keydepth", 16)
//@pa_no_overlay("ingress","key1_hdr.drop_ctl")
//@pa_no_overlay("egress","key1_hdr.drop_ctl")
@pa_no_overlay("egress","eg_intr_md_for_dprsr.drop_ctl")
@pa_no_overlay("egress","eg_intr_md.egress_port")
@pa_no_overlay("egress","meta.pkt_type")
// @pa_no_overlay("egress","eg_dprsr_md.mirror_type")
@pa_no_overlay("egress","meta.egr_mir_ses")
@pa_no_overlay("ingress","meta.tmp_key2_keyhihihi")
@pa_no_overlay("ingress","hdr.seq_hdr.unlock_keyindex")
@pa_no_overlay("egress","hdr.seq_hdr.unlock_keyindex")
// @pa_no_overlay("ingress","op_hdr.optype")
// @pa_no_overlay("egress","op_hdr.optype")
// @pa_no_overlay("ingress","op_hdr.keydepth")
// @pa_no_overlay("egress","op_hdr.keydepth")
// @pa_no_overlay("egress","hdr.op_hdr.optype")
@pa_no_overlay("ingress","hdr.inswitch_hdr.is_cached")
@pa_no_overlay("egress","hdr.inswitch_hdr.is_cached")
@pa_no_overlay("ingress","hdr.inswitch_hdr.need_recirculation")
@pa_no_overlay("egress","hdr.inswitch_hdr.need_recirculation")
@pa_no_overlay("ingress","hdr.inswitch_hdr.is_permission")
@pa_no_overlay("egress","hdr.inswitch_hdr.is_permission")
@pa_no_overlay("egress","meta.is_hot")
@pa_no_overlay("egress","meta.hdrlen_delta")
@pa_no_overlay("egress","meta.subtract_or_add")
// @pa_no_overlay("egress","meta.is_report") 
@pa_no_overlay("ingress","hdr.vallen_hdr.vallen")
@pa_no_overlay("egress","hdr.vallen_hdr.vallen")

@pa_no_overlay("egress","meta.is_hit")

@pa_no_overlay("ingress","hdr.shadowtype_hdr.shadowtype")
@pa_no_overlay("egress","hdr.shadowtype_hdr.shadowtype")
@pa_no_overlay("egress","meta.is_latest")
@pa_no_overlay("egress","meta.is_deleted")
@pa_no_overlay("ingress","hdr.inswitch_hdr.client_sid")
@pa_no_overlay("egress","hdr.inswitch_hdr.client_sid")
@pa_no_overlay("egress","meta.is_lastclone_for_pktloss")
@pa_no_overlay("ingress","hdr.clone_hdr.server_sid")
@pa_no_overlay("egress","hdr.clone_hdr.server_sid")
@pa_no_overlay("ingress","hdr.inswitch_hdr.client_uid_gid")
@pa_no_overlay("egress","hdr.inswitch_hdr.client_uid_gid")
@pa_no_overlay("ingress", "hdr.inswitch_hdr.idx_for_latest_deleted_frequency_reg")
@pa_no_overlay("egress", "hdr.inswitch_hdr.idx_for_latest_deleted_frequency_reg")
@pa_no_overlay("ingress","hdr.inswitch_hdr.is_file")
@pa_no_overlay("egress","hdr.inswitch_hdr.is_file")

@pa_no_overlay("ingress","hdr.val1_hdr.vallo")
@pa_no_overlay("egress","hdr.val1_hdr.valhi")
@pa_no_overlay("ingress","hdr.val2_hdr.vallo")
@pa_no_overlay("egress","hdr.val2_hdr.valhi")
@pa_no_overlay("ingress","hdr.val3_hdr.vallo")
@pa_no_overlay("egress","hdr.val3_hdr.valhi")
@pa_no_overlay("ingress","hdr.val4_hdr.vallo")
@pa_no_overlay("egress","hdr.val4_hdr.valhi")
@pa_no_overlay("ingress","hdr.val5_hdr.vallo")
@pa_no_overlay("egress","hdr.val5_hdr.valhi")
@pa_no_overlay("ingress","hdr.frequency_hdr.frequency")
@pa_no_overlay("egress","hdr.frequency_hdr.frequency")


// @pa_no_overlay("ingress","hdr.val6_hdr.vallo")
// @pa_no_overlay("egress","hdr.val6_hdr.valhi")
// @pa_no_overlay("ingress","hdr.val7_hdr.vallo")
// @pa_no_overlay("egress","hdr.val7_hdr.valhi")
// @pa_no_overlay("ingress","hdr.val8_hdr.vallo")
// @pa_no_overlay("egress","hdr.val8_hdr.valhi")
// @pa_no_overlay("ingress","hdr.val9_hdr.vallo")
// @pa_no_overlay("egress","hdr.val9_hdr.valhi")
// @pa_no_overlay("ingress","hdr.val10_hdr.vallo")
// @pa_no_overlay("egress","hdr.val10_hdr.valhi")
// @pa_no_overlay("ingress","hdr.val11_hdr.vallo")
// @pa_no_overlay("egress","hdr.val11_hdr.valhi")
// @pa_no_overlay("ingress","hdr.val12_hdr.vallo")
// @pa_no_overlay("egress","hdr.val12_hdr.valhi")
// @pa_no_overlay("ingress","hdr.val13_hdr.vallo")
// @pa_no_overlay("egress","hdr.val13_hdr.valhi")
// @pa_no_overlay("ingress","hdr.val14_hdr.vallo")
// @pa_no_overlay("egress","hdr.val14_hdr.valhi")
// @pa_no_overlay("ingress","hdr.val15_hdr.vallo")
// @pa_no_overlay("egress","hdr.val15_hdr.valhi")
// @pa_no_overlay("ingress","hdr.val16_hdr.vallo")
// @pa_no_overlay("egress","hdr.val16_hdr.valhi")
