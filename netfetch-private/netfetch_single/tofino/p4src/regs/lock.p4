Register<bit<32>, bit<16>>(LOCK_BUCKET_COUNT) reg_for_lock_key2;
RegisterAction<bit<32>, bit<16>, bit<1>>(reg_for_lock_key2) reg_for_lock_key2_alu = {
	void apply(inout bit<32> register_data) {
		register_data = register_data + 1; 
	}
};
RegisterAction<bit<32>, bit<16>, bit<1>>(reg_for_lock_key2) reg_for_unlock_key2_alu = {
	void apply(inout bit<32> register_data) {
		register_data = register_data - 1;
	}
};
RegisterAction<bit<32>, bit<16>, bit<1>>(reg_for_lock_key2) reg_for_read_lock_key2_alu = {
	void apply(inout bit<32> register_data, out bit<1> result) {
		if(register_data == 0){
			result = 1;
		}else{
			result = 0;
		}
	}
};
action lock_op() {
	reg_for_lock_key2_alu.execute(meta.tmp_key2_keyhihihi);
}
action unlock_res() {
	reg_for_unlock_key2_alu.execute(meta.tmp_key2_keyhihihi);
}
action acquire_lock() {
	meta.is_acquire_lock_23456 = reg_for_read_lock_key2_alu.execute(meta.tmp_key2_keyhihihi);
}

@pragma stage 3
table access_lock_tbl {
	key = {
		hdr.op_hdr.optype: exact;
		meta.is_cached_for_lock: exact;
		// hdr.
	}
	actions = {
		lock_op;
		unlock_res;
		acquire_lock;
		NoAction;
		// reset_is_acquire_lock_8_1;
	}
	default_action = NoAction();
	size = 64;
}

