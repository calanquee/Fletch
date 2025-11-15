/* Packet Header Types */

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
//	bit<32> keylolo;
//  bit<32> keylohi;
    bit<32> keyhilo;
    bit<16> keyhihilo;
    bit<16> keyhihihi;
}

header op_t {
    bit<16> optype;
    bit<16> keydepth;
}


// Header instances
struct headers {
	ethernet_t ethernet_hdr;
	ipv4_t ipv4_hdr;
	udp_t udp_hdr;
	// Qingxiu: define two op_t for two keys
	op_t op_hdr;
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

}

struct metadata {
	bit<16>	hashval_for_partition; // at most 32K
	bit<4> cm1_predicate;
	bit<4> cm2_predicate;
	bit<4> cm3_predicate;
	bit<4> cm4_predicate;
	bit<1> is_hot;
    
	bit<1> is_report1;
    
	bit<1> is_report2;
    
	bit<1> is_report3;
    
	bit<1> is_report;
    
	bit<1> is_latest; 
    
	bit<1> is_deleted; // if the entry is deleted

	bit<1> is_lastclone_for_pktloss;

	bit<4> access_val_mode; // 0: not access val_reg; 1: get; 2: set_and_get; 3: reset_and_get

	bit<16> hdrlen; // Qingxiu: for NETFETCH

	bit<2> need_clone; //Qingxiu: for recirculation decision

	bit<2> need_recirculation;

	bit<32> temp_ip_for_recirculation;

	bit<16> temp_port_for_recirculation;

	bit<1> is_permission;

	bit<1> is_new_getreq;
}

