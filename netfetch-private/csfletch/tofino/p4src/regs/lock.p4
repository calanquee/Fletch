Register<bit<16>, bit<16>>(LOCK_BUCKET_COUNT) reg_for_lock_key2;
RegisterAction<bit<16>, bit<16>, bit<1>>(reg_for_lock_key2) reg_for_lock_key2_alu = {
	void apply(inout bit<16> register_data) {
		register_data = register_data + 1; 
	}
};
RegisterAction<bit<16>, bit<16>, bit<1>>(reg_for_lock_key2) reg_for_unlock_key2_alu = {
	void apply(inout bit<16> register_data) {
		register_data = register_data - 1;
	}
};
RegisterAction<bit<16>, bit<16>, bit<1>>(reg_for_lock_key2) reg_for_read_lock_key2_alu = {
	void apply(inout bit<16> register_data, out bit<1> result) {
		if(register_data == 0){
			result = 1;
		}else{
			result = 0;
		}
	}
};
action lock_key2_op() {
	reg_for_lock_key2_alu.execute(meta.tmp_key2_keyhihihi);
}
action unlock_key2_res() {
	reg_for_unlock_key2_alu.execute(meta.tmp_key2_keyhihihi);
}
action acquire_key2_lock() {
	meta.is_acquire_lock_23456 = reg_for_read_lock_key2_alu.execute(meta.tmp_key2_keyhihihi);
}
// action reset_is_acquire_lock_8_1() {
// 	meta.is_acquire_lock_23456 = 0;
// }
@pragma stage 3
table access_lock_key2_tbl {
	key = {
		hdr.op_hdr.optype: exact;
		meta.is_cached_for_lock: exact;
		hdr.op_hdr.keydepth: exact;
		hdr.op_hdr.real_keydepth: exact;
		// meta.is_wrong_pipeline: exact;
	}
	actions = {
		lock_key2_op;
		unlock_key2_res;
		acquire_key2_lock;
		NoAction;
		// reset_is_acquire_lock_8_1;
	}
	default_action = NoAction();
	size = 64;
}

Register<bit<16>, bit<16>>(LOCK_BUCKET_COUNT) reg_for_lock_key3;
RegisterAction<bit<16>, bit<16>, bit<1>>(reg_for_lock_key3) reg_for_lock_key3_alu = {
	void apply(inout bit<16> register_data) {
		register_data = register_data + 1; 
	}
};
RegisterAction<bit<16>, bit<16>, bit<1>>(reg_for_lock_key3) reg_for_unlock_key3_alu = {
	void apply(inout bit<16> register_data) {
		register_data = register_data - 1; 
	}
};
RegisterAction<bit<16>, bit<16>, bit<1>>(reg_for_lock_key3) reg_for_read_lock_key3_alu = {
	void apply(inout bit<16> register_data, out bit<1> result) {
		if(register_data == 0){
			result = 1;
		}else{
			result = 0;
		}
	}
};
action lock_key3_op() {
	reg_for_lock_key3_alu.execute(meta.tmp_key3_keyhihihi);
}
action unlock_key3_res() {
	reg_for_unlock_key3_alu.execute(meta.tmp_key3_keyhihihi);
}
action acquire_key3_lock() {
	meta.is_acquire_lock_23456 = reg_for_read_lock_key3_alu.execute(meta.tmp_key3_keyhihihi);
}
// action reset_is_acquire_lock_8_2() {
// 	meta.is_acquire_lock_23456 = 0;
// }
@pragma stage 4
table access_lock_key3_tbl {
	key = {
		hdr.op_hdr.optype: exact;
		meta.is_cached_for_lock: exact;
		hdr.op_hdr.keydepth: exact;
		hdr.op_hdr.real_keydepth: exact;
		// meta.is_wrong_pipeline: exact;
	}
	actions = {
		lock_key3_op;
		unlock_key3_res;
		acquire_key3_lock;
		NoAction;
		// reset_is_acquire_lock_8_2;
	}
	default_action = NoAction();
	size = 64;
}

Register<bit<16>, bit<16>>(LOCK_BUCKET_COUNT) reg_for_lock_key4;
RegisterAction<bit<16>, bit<16>, bit<1>>(reg_for_lock_key4) reg_for_lock_key4_alu = {
	void apply(inout bit<16> register_data) {
		register_data = register_data + 1; 
	}
};
RegisterAction<bit<16>, bit<16>, bit<1>>(reg_for_lock_key4) reg_for_unlock_key4_alu = {
	void apply(inout bit<16> register_data) {
		register_data = register_data - 1; 
	}
};
RegisterAction<bit<16>, bit<16>, bit<1>>(reg_for_lock_key4) reg_for_read_lock_key4_alu = {
	void apply(inout bit<16> register_data, out bit<1> result) {
		if(register_data == 0){
			result = 1;
		}else{
			result = 0;
		}
	}
};
action lock_key4_op() {
	reg_for_lock_key4_alu.execute(meta.tmp_key4_keyhihihi);
}
action unlock_key4_res() {
	reg_for_unlock_key4_alu.execute(meta.tmp_key4_keyhihihi);
}
action acquire_key4_lock() {
	meta.is_acquire_lock_23456 = reg_for_read_lock_key4_alu.execute(meta.tmp_key4_keyhihihi);
}
// action reset_is_acquire_lock_8_3() {
// 	meta.is_acquire_lock_23456 = 0;
// }
@pragma stage 5
table access_lock_key4_tbl {
	key = {
		hdr.op_hdr.optype: exact;
		meta.is_cached_for_lock: exact;
		hdr.op_hdr.keydepth: exact;
		hdr.op_hdr.real_keydepth: exact;
		// meta.is_wrong_pipeline: exact;
	}
	actions = {
		lock_key4_op;
		unlock_key4_res;
		acquire_key4_lock;
		NoAction;
		// reset_is_acquire_lock_8_3;
	}
	default_action = NoAction();
	size = 64;
}

Register<bit<16>, bit<16>>(LOCK_BUCKET_COUNT) reg_for_lock_key5;
RegisterAction<bit<16>, bit<16>, bit<1>>(reg_for_lock_key5) reg_for_lock_key5_alu = {
	void apply(inout bit<16> register_data) {
		register_data = register_data + 1; 
	}
};
RegisterAction<bit<16>, bit<16>, bit<1>>(reg_for_lock_key5) reg_for_unlock_key5_alu = {
	void apply(inout bit<16> register_data) {
		register_data = register_data - 1; 
	}
};
RegisterAction<bit<16>, bit<16>, bit<1>>(reg_for_lock_key5) reg_for_read_lock_key5_alu = {
	void apply(inout bit<16> register_data, out bit<1> result) {
		if(register_data == 0){
			result = 1;
		}else{
			result = 0;
		}
	}
};
action lock_key5_op() {
	reg_for_lock_key5_alu.execute(meta.tmp_key5_keyhihihi);
}
action unlock_key5_res() {
	reg_for_unlock_key5_alu.execute(meta.tmp_key5_keyhihihi);
}
action acquire_key5_lock() {
	meta.is_acquire_lock_23456 = reg_for_read_lock_key5_alu.execute(meta.tmp_key5_keyhihihi);
}
// action reset_is_acquire_lock_8_4() {
// 	meta.is_acquire_lock_23456 = 0;
// }
@pragma stage 6
table access_lock_key5_tbl {
	key = {
		hdr.op_hdr.optype: exact;
		meta.is_cached_for_lock: exact;
		hdr.op_hdr.keydepth: exact;
		hdr.op_hdr.real_keydepth: exact;
		// meta.is_wrong_pipeline: exact;
	}
	actions = {
		lock_key5_op;
		unlock_key5_res;
		acquire_key5_lock;
		NoAction;
		// reset_is_acquire_lock_8_4;
	}
	default_action = NoAction();
	size = 64;
}

// Key6
Register<bit<16>, bit<16>>(LOCK_BUCKET_COUNT) reg_for_lock_key6;
RegisterAction<bit<16>, bit<16>, bit<1>>(reg_for_lock_key6) reg_for_lock_key6_alu = {
	void apply(inout bit<16> register_data) {
		register_data = register_data + 1; 
	}
};
RegisterAction<bit<16>, bit<16>, bit<1>>(reg_for_lock_key6) reg_for_unlock_key6_alu = {
	void apply(inout bit<16> register_data) {
		register_data = register_data - 1; 
	}
};
RegisterAction<bit<16>, bit<16>, bit<1>>(reg_for_lock_key6) reg_for_read_lock_key6_alu = {
	void apply(inout bit<16> register_data, out bit<1> result) {
		if(register_data == 0){
			result = 1;
		}else{
			result = 0;
		}
	}
};
action lock_key6_op() {
	reg_for_lock_key6_alu.execute(meta.tmp_key6_keyhihihi);
}
action unlock_key6_res() {
	reg_for_unlock_key6_alu.execute(meta.tmp_key6_keyhihihi);
}
action acquire_key6_lock() {
	meta.is_acquire_lock_23456 = reg_for_read_lock_key6_alu.execute(meta.tmp_key6_keyhihihi);
}
// action reset_is_acquire_lock_8_5() {
// 	meta.is_acquire_lock_23456 = 0;
// }
@pragma stage 9
table access_lock_key6_tbl {
	key = {
		hdr.op_hdr.optype: exact;
		meta.is_cached_for_lock: exact;
		hdr.op_hdr.keydepth: exact;
		hdr.op_hdr.real_keydepth: exact;
		// meta.is_wrong_pipeline: exact;
	}
	actions = {
		lock_key6_op;
		unlock_key6_res;
		acquire_key6_lock;
		NoAction;
		// reset_is_acquire_lock_8_5;
	}
	default_action = NoAction();
	size = 64;
}

Register<bit<16>, bit<16>>(LOCK_BUCKET_COUNT) reg_for_lock_key7;
RegisterAction<bit<16>, bit<16>, bit<1>>(reg_for_lock_key7) reg_for_lock_key7_alu = {
	void apply(inout bit<16> register_data) {
		register_data = register_data + 1; 
	}
};
RegisterAction<bit<16>, bit<16>, bit<1>>(reg_for_lock_key7) reg_for_unlock_key7_alu = {
	void apply(inout bit<16> register_data) {
		register_data = register_data - 1; 
	}
};
RegisterAction<bit<16>, bit<16>, bit<1>>(reg_for_lock_key7) reg_for_read_lock_key7_alu = {
	void apply(inout bit<16> register_data, out bit<1> result) {
		if(register_data == 0){
			result = 1;
		}else{
			result = 0;
		}
	}
};
action lock_key7_op() {
	reg_for_lock_key7_alu.execute(meta.tmp_key7_keyhihihi);
}
action unlock_key7_res() {
	reg_for_unlock_key7_alu.execute(meta.tmp_key7_keyhihihi);
}
action acquire_key7_lock() {
	meta.is_acquire_lock_7 = reg_for_read_lock_key7_alu.execute(meta.tmp_key7_keyhihihi);
}
action reset_is_acquire_lock_8_6() {
	meta.is_acquire_lock_7 = 0;
}
@pragma stage 9
table access_lock_key7_tbl {
	key = {
		hdr.op_hdr.optype: exact;
		meta.is_cached_for_lock: exact;
		hdr.op_hdr.keydepth: exact;
		hdr.op_hdr.real_keydepth: exact;
		// meta.is_wrong_pipeline: exact;
	}
	actions = {
		lock_key7_op;
		unlock_key7_res;
		acquire_key7_lock;
		reset_is_acquire_lock_8_6;
	}
	default_action = reset_is_acquire_lock_8_6();
	size = 64;
}


Register<bit<16>, bit<16>>(LOCK_BUCKET_COUNT) reg_for_lock_key8;
RegisterAction<bit<16>, bit<16>, bit<1>>(reg_for_lock_key8) reg_for_lock_key8_alu = {
	void apply(inout bit<16> register_data) {
		register_data = register_data + 1; 
	}
};
RegisterAction<bit<16>, bit<16>, bit<1>>(reg_for_lock_key8) reg_for_unlock_key8_alu = {
	void apply(inout bit<16> register_data) {
		register_data = register_data - 1; 
	}
};
RegisterAction<bit<16>, bit<16>, bit<1>>(reg_for_lock_key8) reg_for_read_lock_key8_alu = {
	void apply(inout bit<16> register_data, out bit<1> result) {
		if(register_data == 0){
			result = 1;
		}else{
			result = 0;
		}
	}
};
action lock_key8_op() {
	reg_for_lock_key8_alu.execute(meta.tmp_key8_keyhihihi);
}
action unlock_key8_res() {
	reg_for_unlock_key8_alu.execute(meta.tmp_key8_keyhihihi);
}
action acquire_key8_lock() {
	meta.is_acquire_lock_8 = reg_for_read_lock_key8_alu.execute(meta.tmp_key8_keyhihihi);
}
action reset_is_acquire_lock_8_7() {
	meta.is_acquire_lock_8 = 0;
}
@pragma stage 9
table access_lock_key8_tbl {
	key = {
		hdr.op_hdr.optype: exact;
		meta.is_cached_for_lock: exact;
		hdr.op_hdr.keydepth: exact;
		hdr.op_hdr.real_keydepth: exact;
		// meta.is_wrong_pipeline: exact;
	}
	actions = {
		lock_key8_op;
		unlock_key8_res;
		acquire_key8_lock;
		reset_is_acquire_lock_8_7;
	}
	default_action = reset_is_acquire_lock_8_7();
	size = 64;
}


Register<bit<16>, bit<16>>(LOCK_BUCKET_COUNT) reg_for_lock_key9;
RegisterAction<bit<16>, bit<16>, bit<1>>(reg_for_lock_key9) reg_for_lock_key9_alu = {
	void apply(inout bit<16> register_data) {
		register_data = register_data + (bit<16>)meta.tmp_offset_for_lock9;
	}
};
RegisterAction<bit<16>, bit<16>, bit<1>>(reg_for_lock_key9) reg_for_unlock_key9_alu = {
	void apply(inout bit<16> register_data) {
		register_data = register_data - (bit<16>)meta.tmp_offset_for_lock9; 
	}
};
RegisterAction<bit<16>, bit<16>, bit<1>>(reg_for_lock_key9) reg_for_read_lock_key9_alu = {
	void apply(inout bit<16> register_data, out bit<1> result) {
		if(register_data == 0){
			result = 1;
		}else{
			result = 0;
		}
	}
};
action lock_key9_op() {
	reg_for_lock_key9_alu.execute(meta.tmp_key9_keyhihihi);
}
action unlock_key9_res() {
	reg_for_unlock_key9_alu.execute(meta.tmp_key9_keyhihihi);
}
action lock_key10_op() {
	reg_for_lock_key9_alu.execute(meta.tmp_key9_keyhihihi);
}
action unlock_key10_res() {
	reg_for_unlock_key9_alu.execute(meta.tmp_key9_keyhihihi);
}
action acquire_key9_lock() {
	meta.is_acquire_lock_9 = reg_for_read_lock_key9_alu.execute(meta.tmp_key9_keyhihihi);
}
action reset_is_acquire_lock_8_8() {
	meta.is_acquire_lock_9 = 0;
}
@pragma stage 9
table access_lock_key9_tbl {
	key = {
		hdr.op_hdr.optype: exact;
		meta.is_cached_for_lock: exact;
		hdr.op_hdr.keydepth: exact;
		hdr.op_hdr.real_keydepth: exact;
		// meta.is_wrong_pipeline: exact;
	}
	actions = {
		lock_key9_op;
		unlock_key9_res;
		lock_key10_op;
		unlock_key10_res;
		acquire_key9_lock;
		reset_is_acquire_lock_8_8;
	}
	default_action = reset_is_acquire_lock_8_8();
	size = 64;
}


