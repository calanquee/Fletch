Register<bit<8>, bit<1>>(2) counter_reg;
RegisterAction<bit<8>, bit<1>, bit<8>>(counter_reg) counter_reg_get_alu = {
	void apply(inout bit<8> register_data, out bit<8> result) {
		result = register_data;
	}
};
RegisterAction<bit<8>, bit<1>, bit<8>>(counter_reg) counter_reg_set_alu = {
	void apply(inout bit<8> register_data) {
		register_data = register_data + 1;
	}
};

action get_counter_server1() {
	meta.is_counter = (bit<16>)counter_reg_get_alu.execute((bit<1>)0);
}
action get_counter_server2() {
	meta.is_counter = (bit<16>)counter_reg_get_alu.execute((bit<1>)1);
}
action update_counter_server1() {
	counter_reg_set_alu.execute((bit<1>)0);
}
action update_counter_server2() {
	counter_reg_set_alu.execute((bit<1>)1);
}
@pragma stage 0
table access_counter_tbl {
	key = {
		hdr.op_hdr.optype: exact;
		hdr.ipv4_hdr.srcAddr: lpm;
	}
	actions = {
		get_counter_server1;
		get_counter_server2;
		update_counter_server1;
		update_counter_server2;
		NoAction;
	}
	default_action = NoAction();
	size = 16;
}

action set_counter_equal() {
	meta.is_counter_equal_to_sequence = 1;
}
action set_counter_not_equal() {
	meta.is_counter_equal_to_sequence = 0;
}

@pragma stage 1
table check_counter_tbl {
	key = {
		hdr.op_hdr.optype: exact;
		meta.is_counter: exact;
		meta.keyhihihi: exact;
	}
	actions = {
		set_counter_equal;
		set_counter_not_equal;
	}
	size = 512;
	default_action = set_counter_not_equal;
}

