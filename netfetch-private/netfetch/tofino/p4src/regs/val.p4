// NOTE: we do not match optype for val_reg (especialy for val16_reg), as eg_port_forward_tbl may change optype before accessing val16_reg
// NOTE: we also place drop() in the last stage to avoid skipping val16_reg due to immediate drop semantic
Register<bit<16>,bit<32>>(KV_BUCKET_COUNT*3) vallen_reg;
RegisterAction<bit<16>, bit<32>, bit<16>>(vallen_reg) vallen_reg_get_alu = {
	void apply(inout bit<16> register_data, out bit<16> result) {
		result = register_data;
	}
};
RegisterAction<bit<16>, bit<32>, bit<16>>(vallen_reg) vallen_reg_get_set_alu_file = {
	void apply(inout bit<16> register_data, out bit<16> result) { 
		register_data = 40;
        result = register_data;
	}
};
RegisterAction<bit<16>, bit<32>, bit<16>>(vallen_reg) vallen_reg_get_set_alu_dir = {
	void apply(inout bit<16> register_data, out bit<16> result) { 
		register_data = 24;
        result = register_data;
	}
};
RegisterAction<bit<16>, bit<32>, bit<16>>(vallen_reg) vallen_reg_get_set_alu = {
	void apply(inout bit<16> register_data) { 
		register_data = hdr.vallen_hdr.vallen;
	}
};
action get_vallen() {
	// hdr.vallen_hdr.setValid();
	hdr.vallen_hdr.vallen = vallen_reg_get_alu.execute((bit<32>)hdr.inswitch_hdr.idx_for_latest_deleted_frequency_reg);
	meta.access_val_mode = 1; // get val_reg
    hdr.vallen_hdr.setValid();
}
action set_and_get_vallen() {
	vallen_reg_get_set_alu.execute((bit<32>)hdr.inswitch_hdr.idx_for_latest_deleted_frequency_reg);
	meta.access_val_mode = 2; // set_and_get val_reg
    hdr.vallen_hdr.setValid();
}
action set_and_get_vallen_for_file() {
    hdr.vallen_hdr.vallen = vallen_reg_get_set_alu_file.execute((bit<32>)hdr.inswitch_hdr.idx_for_latest_deleted_frequency_reg);
	meta.access_val_mode = 2; // set_and_get val_reg
    hdr.vallen_hdr.setValid();
}
action set_and_get_vallen_for_dir() {
	hdr.vallen_hdr.vallen = vallen_reg_get_set_alu_dir.execute((bit<32>)hdr.inswitch_hdr.idx_for_latest_deleted_frequency_reg);
	meta.access_val_mode = 2; // set_and_get val_reg
    hdr.vallen_hdr.setValid();
}
action reset_and_get_vallen_for_update_cache() {
	meta.access_val_mode =  3; // will update permission later
}

action reset_access_val_mode() {
	meta.access_val_mode = 0; // not access val_reg
}

@pragma stage 1
table update_vallen_tbl {
	key = {
        hdr.op_hdr.optype: exact;
		hdr.inswitch_hdr.is_cached: exact;
		meta.is_latest: exact;
        hdr.inswitch_hdr.is_file: exact;
	}
	actions = {
		get_vallen;
        set_and_get_vallen;
		set_and_get_vallen_for_file;
        set_and_get_vallen_for_dir;
		reset_and_get_vallen_for_update_cache;
		reset_access_val_mode;
	}
	default_action = reset_access_val_mode();
	size = 24;
}

Register<bit<32>,bit<32>>(VAL_BUCKET_COUNT) vallo1_reg;
RegisterAction<bit<32>, bit<32>, bit<32>>(vallo1_reg) vallo1_reg_get_alu = {
    void apply(inout bit<32> register_data, out bit<32> result) {
        result = register_data;
    }
};
RegisterAction<bit<32>, bit<32>, bit<32>>(vallo1_reg) vallo1_reg_set_alu = {
    void apply(inout bit<32> register_data) {
        register_data = hdr.val1_hdr.vallo;
    }
};
RegisterAction<bit<32>, bit<32>, bit<32>>(vallo1_reg) vallo1_reg_set_perm_alu = {
    void apply(inout bit<32> register_data) {
        register_data = meta.tmp_permission;
    }
};


action get_vallo1() {
    hdr.val1_hdr.vallo = vallo1_reg_get_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.val1_hdr.setValid();
}
action set_and_get_vallo1() {
    vallo1_reg_set_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.val1_hdr.setValid();
}
action set_and_get_vallo1_for_permission() {
    vallo1_reg_set_perm_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.val1_hdr.setValid();
}
@pragma stage 2
table update_vallo1_tbl {
    key = {
        meta.access_val_mode: exact; // 1, 2, 3
        hdr.inswitch_hdr.bitmap[0:0]: exact; // 0, 1
        hdr.inswitch_hdr.is_val1_permission: exact; // 0, 1
    }
    actions = {
        get_vallo1;
        set_and_get_vallo1;
        set_and_get_vallo1_for_permission;
        NoAction;
    }
    default_action = NoAction();
    size = 16;
}

Register<bit<32>,bit<32>>(VAL_BUCKET_COUNT) valhi1_reg;
RegisterAction<bit<32>, bit<32>, bit<32>>(valhi1_reg) valhi1_reg_get_alu = {
    void apply(inout bit<32> register_data, out bit<32> result) {
        result = register_data;
    }
};
RegisterAction<bit<32>, bit<32>, bit<32>>(valhi1_reg) valhi1_reg_set_alu = {
    void apply(inout bit<32> register_data) {
        register_data = hdr.val1_hdr.valhi;
    }
};

action get_valhi1() {
    hdr.val1_hdr.valhi = valhi1_reg_get_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
}
action set_and_get_valhi1() {
    valhi1_reg_set_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
}
action xor_valhi1() {
    hdr.val1_hdr.valhi = valhi1_reg_get_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.inswitch_hdr.client_uid_gid = hdr.inswitch_hdr.client_uid_gid ^ hdr.val1_hdr.valhi;
}
@pragma stage 2
table update_valhi1_tbl {
    key = {
        meta.access_val_mode: exact;
        hdr.inswitch_hdr.bitmap[0:0]: exact;
        hdr.inswitch_hdr.val_idx_for_permission: exact;
    }
    actions = {
        get_valhi1;
        set_and_get_valhi1;
        xor_valhi1;
        NoAction;
    }
    default_action = NoAction();
    size = 16;
}

Register<bit<32>,bit<32>>(VAL_BUCKET_COUNT) vallo2_reg;
RegisterAction<bit<32>, bit<32>, bit<32>>(vallo2_reg) vallo2_reg_get_alu = {
    void apply(inout bit<32> register_data, out bit<32> result) {
        result = register_data;
    }
};
RegisterAction<bit<32>, bit<32>, bit<32>>(vallo2_reg) vallo2_reg_set_alu = {
    void apply(inout bit<32> register_data) {
        register_data = hdr.val2_hdr.vallo;
    }
};
RegisterAction<bit<32>, bit<32>, bit<32>>(vallo2_reg) vallo2_reg_set_perm_alu = {
    void apply(inout bit<32> register_data) {
        register_data = meta.tmp_permission;
    }
};

action get_vallo2() {
    hdr.val2_hdr.vallo = vallo2_reg_get_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.val2_hdr.setValid();
}
action set_and_get_vallo2() {
    vallo2_reg_set_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.val2_hdr.setValid();
}
action set_and_get_vallo2_for_permission() {
    vallo2_reg_set_perm_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.val2_hdr.setValid();
}

@pragma stage 3
table update_vallo2_tbl {
    key = {
        meta.access_val_mode: exact;
        hdr.inswitch_hdr.bitmap[1:1]: exact;
        hdr.inswitch_hdr.is_val2_permission: exact;
    }
    actions = {
        get_vallo2;
        set_and_get_vallo2;
        set_and_get_vallo2_for_permission;
        NoAction;
    }
    default_action = NoAction();
    size = 16;
}

Register<bit<32>,bit<32>>(VAL_BUCKET_COUNT) valhi2_reg;
RegisterAction<bit<32>, bit<32>, bit<32>>(valhi2_reg) valhi2_reg_get_alu = {
    void apply(inout bit<32> register_data, out bit<32> result) {
        result = register_data;
    }
};
RegisterAction<bit<32>, bit<32>, bit<32>>(valhi2_reg) valhi2_reg_set_alu = {
    void apply(inout bit<32> register_data) {
        register_data = hdr.val2_hdr.valhi;
    }
};

action get_valhi2() {
    hdr.val2_hdr.valhi = valhi2_reg_get_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
}
action set_and_get_valhi2() {
    valhi2_reg_set_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
}
action xor_valhi2() {
    hdr.val2_hdr.valhi = valhi2_reg_get_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.inswitch_hdr.client_uid_gid = hdr.inswitch_hdr.client_uid_gid ^ hdr.val2_hdr.valhi;
}
@pragma stage 3
table update_valhi2_tbl {
    key = {
        meta.access_val_mode: exact;
        hdr.inswitch_hdr.bitmap[1:1]: exact;
        hdr.inswitch_hdr.val_idx_for_permission: exact;
    }
    actions = {
        get_valhi2;
        set_and_get_valhi2;
        xor_valhi2;
        NoAction;
    }
    default_action = NoAction();
    size = 16;
}

Register<bit<32>,bit<32>>(VAL_BUCKET_COUNT) vallo3_reg;
RegisterAction<bit<32>, bit<32>, bit<32>>(vallo3_reg) vallo3_reg_get_alu = {
    void apply(inout bit<32> register_data, out bit<32> result) {
        result = register_data;
    }
};
RegisterAction<bit<32>, bit<32>, bit<32>>(vallo3_reg) vallo3_reg_set_alu = {
    void apply(inout bit<32> register_data){
        register_data = hdr.val3_hdr.vallo;
    }
};
RegisterAction<bit<32>, bit<32>, bit<32>>(vallo3_reg) vallo3_reg_set_perm_alu = {
    void apply(inout bit<32> register_data) {
        register_data = meta.tmp_permission;
    }
};

action get_vallo3() {
    hdr.val3_hdr.vallo = vallo3_reg_get_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.val3_hdr.setValid();
}
action set_and_get_vallo3() {
    vallo3_reg_set_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.val3_hdr.setValid();
}
action set_and_get_vallo3_for_permission() {
    vallo3_reg_set_perm_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.val3_hdr.setValid();
}
@pragma stage 4
table update_vallo3_tbl {
    key = {
        meta.access_val_mode: exact;
        hdr.inswitch_hdr.bitmap[2:2]: exact;
        hdr.inswitch_hdr.is_val3_permission: exact;
    }
    actions = {
        get_vallo3;
        set_and_get_vallo3;
        set_and_get_vallo3_for_permission;
        NoAction;
    }
    default_action = NoAction();
    size = 16;
}

Register<bit<32>,bit<32>>(VAL_BUCKET_COUNT) valhi3_reg;
RegisterAction<bit<32>, bit<32>, bit<32>>(valhi3_reg) valhi3_reg_get_alu = {
    void apply(inout bit<32> register_data, out bit<32> result) {
        result = register_data;
    }
};
RegisterAction<bit<32>, bit<32>, bit<32>>(valhi3_reg) valhi3_reg_set_alu = {
    void apply(inout bit<32> register_data){
        register_data = hdr.val3_hdr.valhi;
    }
};

action get_valhi3() {
    hdr.val3_hdr.valhi = valhi3_reg_get_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
}
action set_and_get_valhi3() {
    valhi3_reg_set_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
}
action xor_valhi3() {
    hdr.val3_hdr.valhi = valhi3_reg_get_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.inswitch_hdr.client_uid_gid = hdr.inswitch_hdr.client_uid_gid ^ hdr.val3_hdr.valhi;
}
@pragma stage 4
table update_valhi3_tbl {
    key = {
        meta.access_val_mode: exact;
        hdr.inswitch_hdr.bitmap[2:2]: exact;
        hdr.inswitch_hdr.val_idx_for_permission: exact;
    }
    actions = {
        get_valhi3;
        set_and_get_valhi3;
        xor_valhi3;
        NoAction;
    }
    default_action = NoAction();
    size = 16;
}

Register<bit<32>,bit<32>>(VAL_BUCKET_COUNT) vallo4_reg;
RegisterAction<bit<32>, bit<32>, bit<32>>(vallo4_reg) vallo4_reg_get_alu = {
    void apply(inout bit<32> register_data, out bit<32> result) {
        result = register_data;
    }
};
RegisterAction<bit<32>, bit<32>, bit<32>>(vallo4_reg) vallo4_reg_set_alu = {
    void apply(inout bit<32> register_data){
        register_data = hdr.val4_hdr.vallo;
    }
};
RegisterAction<bit<32>, bit<32>, bit<32>>(vallo4_reg) vallo4_reg_set_perm_alu = {
    void apply(inout bit<32> register_data) {
        register_data = meta.tmp_permission;
    }
};

action get_vallo4() {
    hdr.val4_hdr.vallo = vallo4_reg_get_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.val4_hdr.setValid();
}
action set_and_get_vallo4() {
    vallo4_reg_set_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.val4_hdr.setValid();
}
action set_and_get_vallo4_for_permission() {
    vallo4_reg_set_perm_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.val4_hdr.setValid();
}
@pragma stage 5
table update_vallo4_tbl {
    key = {
        meta.access_val_mode: exact;
        hdr.inswitch_hdr.bitmap[3:3]: exact;
        hdr.inswitch_hdr.is_val4_permission: exact;
    }
    actions = {
        get_vallo4;
        set_and_get_vallo4;
        set_and_get_vallo4_for_permission;
        NoAction;
    }
    default_action = NoAction();
    size = 16;
}

Register<bit<32>,bit<32>>(VAL_BUCKET_COUNT) valhi4_reg;
RegisterAction<bit<32>, bit<32>, bit<32>>(valhi4_reg) valhi4_reg_get_alu = {
    void apply(inout bit<32> register_data, out bit<32> result) {
        result = register_data;
    }
};
RegisterAction<bit<32>, bit<32>, bit<32>>(valhi4_reg) valhi4_reg_set_alu = {
    void apply(inout bit<32> register_data){
        register_data = hdr.val4_hdr.valhi;
    }
};

action get_valhi4() {
    hdr.val4_hdr.valhi = valhi4_reg_get_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
}
action set_and_get_valhi4() {
    valhi4_reg_set_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
}
action xor_valhi4() {
    hdr.val4_hdr.valhi = valhi4_reg_get_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.inswitch_hdr.client_uid_gid = hdr.inswitch_hdr.client_uid_gid ^ hdr.val4_hdr.valhi;
}
@pragma stage 5
table update_valhi4_tbl {
    key = {
        meta.access_val_mode: exact;
        hdr.inswitch_hdr.bitmap[3:3]: exact;
        hdr.inswitch_hdr.val_idx_for_permission: exact;
    }
    actions = {
        get_valhi4;
        set_and_get_valhi4;
        xor_valhi4;
        NoAction;
    }
    default_action = NoAction();
    size = 16;
}

Register<bit<32>,bit<32>>(VAL_BUCKET_COUNT) vallo5_reg;
RegisterAction<bit<32>, bit<32>, bit<32>>(vallo5_reg) vallo5_reg_get_alu = {
    void apply(inout bit<32> register_data, out bit<32> result) {
        result = register_data;
    }
};
RegisterAction<bit<32>, bit<32>, bit<32>>(vallo5_reg) vallo5_reg_set_alu = {
    void apply(inout bit<32> register_data){
        register_data = hdr.val5_hdr.vallo;
    }
};
RegisterAction<bit<32>, bit<32>, bit<32>>(vallo5_reg) vallo5_reg_set_perm_alu = {
    void apply(inout bit<32> register_data) {
        register_data = meta.tmp_permission;
    }
};

action get_vallo5() {
    hdr.val5_hdr.vallo = vallo5_reg_get_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.val5_hdr.setValid();
}
action set_and_get_vallo5() {
    vallo5_reg_set_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.val5_hdr.setValid();
}
action set_and_get_vallo5_for_permission() {
    vallo5_reg_set_perm_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.val5_hdr.setValid();
}
@pragma stage 3
table update_vallo5_tbl {
    key = {
        meta.access_val_mode: exact;
        hdr.inswitch_hdr.bitmap[4:4]: exact;
        hdr.inswitch_hdr.is_val5_permission: exact;
    }
    actions = {
        get_vallo5;
        set_and_get_vallo5;
        set_and_get_vallo5_for_permission;
        NoAction;
    }
    default_action = NoAction();
    size = 16;
}

Register<bit<32>,bit<32>>(VAL_BUCKET_COUNT) valhi5_reg;
RegisterAction<bit<32>, bit<32>, bit<32>>(valhi5_reg) valhi5_reg_get_alu = {
    void apply(inout bit<32> register_data, out bit<32> result) {
        result = register_data;
    }
};
RegisterAction<bit<32>, bit<32>, bit<32>>(valhi5_reg) valhi5_reg_set_alu = {
    void apply(inout bit<32> register_data){
        register_data = hdr.val5_hdr.valhi;
    }
};

action get_valhi5() {
    hdr.val5_hdr.valhi = valhi5_reg_get_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
}
action set_and_get_valhi5() {
    valhi5_reg_set_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
}
action xor_valhi5() {
    hdr.val5_hdr.valhi = valhi5_reg_get_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.inswitch_hdr.client_uid_gid = hdr.inswitch_hdr.client_uid_gid ^ hdr.val5_hdr.valhi;
}
@pragma stage 6
table update_valhi5_tbl {
    key = {
        meta.access_val_mode: exact;
        hdr.inswitch_hdr.bitmap[4:4]: exact;
        hdr.inswitch_hdr.val_idx_for_permission: exact;
    }
    actions = {
        get_valhi5;
        set_and_get_valhi5;
        xor_valhi5;
        NoAction;
    }
    default_action = NoAction();
    size = 16;
}
Register<bit<32>,bit<32>>(VAL_BUCKET_COUNT) vallo6_reg;
RegisterAction<bit<32>, bit<32>, bit<32>>(vallo6_reg) vallo6_reg_get_alu = {
    void apply(inout bit<32> register_data, out bit<32> result) {
        result = register_data;
    }
};
RegisterAction<bit<32>, bit<32>, bit<32>>(vallo6_reg) vallo6_reg_set_alu = {
    void apply(inout bit<32> register_data){
        register_data = hdr.val6_hdr.vallo;
    }
};

action get_vallo6() {
    hdr.val6_hdr.vallo = vallo6_reg_get_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.val6_hdr.setValid();
}

action set_and_get_vallo6() {
    vallo6_reg_set_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.val6_hdr.setValid();
}
@pragma stage 7
table update_vallo6_tbl {
    key = {
        meta.access_val_mode: exact;
        hdr.inswitch_hdr.bitmap[5:5]: exact;
    }
    actions = {
        get_vallo6;
        set_and_get_vallo6;
        NoAction;
    }
    default_action = NoAction();
    size = 16;
}
Register<bit<32>,bit<32>>(VAL_BUCKET_COUNT) valhi6_reg;
RegisterAction<bit<32>, bit<32>, bit<32>>(valhi6_reg) valhi6_reg_get_alu = {
    void apply(inout bit<32> register_data, out bit<32> result) {
        result = register_data;
    }
};
RegisterAction<bit<32>, bit<32>, bit<32>>(valhi6_reg) valhi6_reg_set_alu = {
    void apply(inout bit<32> register_data){
        register_data = hdr.val6_hdr.valhi;
    }
};

action get_valhi6() {
    hdr.val6_hdr.valhi = valhi6_reg_get_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
}

action set_and_get_valhi6() {
    valhi6_reg_set_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
}
@pragma stage 7
table update_valhi6_tbl {
    key = {
        meta.access_val_mode: exact;
        hdr.inswitch_hdr.bitmap[5:5]: exact;
    }
    actions = {
        get_valhi6;
        set_and_get_valhi6;
        NoAction;
    }
    default_action = NoAction();
    size = 16;
}
Register<bit<32>,bit<32>>(VAL_BUCKET_COUNT) vallo7_reg;
RegisterAction<bit<32>, bit<32>, bit<32>>(vallo7_reg) vallo7_reg_get_alu = {
    void apply(inout bit<32> register_data, out bit<32> result) {
        result = register_data;
    }
};
RegisterAction<bit<32>, bit<32>, bit<32>>(vallo7_reg) vallo7_reg_set_alu = {
    void apply(inout bit<32> register_data){
        register_data = hdr.val7_hdr.vallo;
    }
};

action get_vallo7() {
    hdr.val7_hdr.vallo = vallo7_reg_get_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.val7_hdr.setValid();
}

action set_and_get_vallo7() {
    vallo7_reg_set_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.val7_hdr.setValid();
}
@pragma stage 11
table update_vallo7_tbl {
    key = {
        meta.access_val_mode: exact;
        hdr.inswitch_hdr.bitmap[6:6]: exact;
    }
    actions = {
        get_vallo7;
        set_and_get_vallo7;
        NoAction;
    }
    default_action = NoAction();
    size = 16;
}
Register<bit<32>,bit<32>>(VAL_BUCKET_COUNT) valhi7_reg;
RegisterAction<bit<32>, bit<32>, bit<32>>(valhi7_reg) valhi7_reg_get_alu = {
    void apply(inout bit<32> register_data, out bit<32> result) {
        result = register_data;
    }
};
RegisterAction<bit<32>, bit<32>, bit<32>>(valhi7_reg) valhi7_reg_set_alu = {
    void apply(inout bit<32> register_data){
        register_data = hdr.val7_hdr.valhi;
    }
};

action get_valhi7() {
    hdr.val7_hdr.valhi = valhi7_reg_get_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
}

action set_and_get_valhi7() {
    valhi7_reg_set_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
}
@pragma stage 11
table update_valhi7_tbl {
    key = {
        meta.access_val_mode: exact;
        hdr.inswitch_hdr.bitmap[6:6]: exact;
    }
    actions = {
        get_valhi7;
        set_and_get_valhi7;
        NoAction;
    }
    default_action = NoAction();
    size = 16;
}
Register<bit<32>,bit<32>>(VAL_BUCKET_COUNT) vallo8_reg;
RegisterAction<bit<32>, bit<32>, bit<32>>(vallo8_reg) vallo8_reg_get_alu = {
    void apply(inout bit<32> register_data, out bit<32> result) {
        result = register_data;
    }
};
RegisterAction<bit<32>, bit<32>, bit<32>>(vallo8_reg) vallo8_reg_set_alu = {
    void apply(inout bit<32> register_data){
        register_data = hdr.val8_hdr.vallo;
    }
};

action get_vallo8() {
    hdr.val8_hdr.vallo = vallo8_reg_get_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.val8_hdr.setValid();
}

action set_and_get_vallo8() {
    vallo8_reg_set_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.val8_hdr.setValid();
}
@pragma stage 11
table update_vallo8_tbl {
    key = {
        meta.access_val_mode: exact;
        hdr.inswitch_hdr.bitmap[7:7]: exact;
    }
    actions = {
        get_vallo8;
        set_and_get_vallo8;
        NoAction;
    }
    default_action = NoAction();
    size = 16;
}
Register<bit<32>,bit<32>>(VAL_BUCKET_COUNT) valhi8_reg;
RegisterAction<bit<32>, bit<32>, bit<32>>(valhi8_reg) valhi8_reg_get_alu = {
    void apply(inout bit<32> register_data, out bit<32> result) {
        result = register_data;
    }
};
RegisterAction<bit<32>, bit<32>, bit<32>>(valhi8_reg) valhi8_reg_set_alu = {
    void apply(inout bit<32> register_data){
        register_data = hdr.val8_hdr.valhi;
    }
};

action get_valhi8() {
    hdr.val8_hdr.valhi = valhi8_reg_get_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
}

action set_and_get_valhi8() {
    valhi8_reg_set_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
}
@pragma stage 11
table update_valhi8_tbl {
    key = {
        meta.access_val_mode: exact;
        hdr.inswitch_hdr.bitmap[7:7]: exact;
    }
    actions = {
        get_valhi8;
        set_and_get_valhi8;
        NoAction;
    }
    default_action = NoAction();
    size = 16;
}
Register<bit<32>,bit<32>>(VAL_BUCKET_COUNT) vallo9_reg;
RegisterAction<bit<32>, bit<32>, bit<32>>(vallo9_reg) vallo9_reg_get_alu = {
    void apply(inout bit<32> register_data, out bit<32> result) {
        result = register_data;
    }
};
RegisterAction<bit<32>, bit<32>, bit<32>>(vallo9_reg) vallo9_reg_set_alu = {
    void apply(inout bit<32> register_data){
        register_data = hdr.val9_hdr.vallo;
    }
};

action get_vallo9() {
    hdr.val9_hdr.vallo = vallo9_reg_get_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.val9_hdr.setValid();
}

action set_and_get_vallo9() {
    vallo9_reg_set_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.val9_hdr.setValid();
}
@pragma stage 2
table update_vallo9_tbl {
    key = {
        meta.access_val_mode: exact;
        hdr.inswitch_hdr.bitmap[8:8]: exact;
    }
    actions = {
        get_vallo9;
        set_and_get_vallo9;
        NoAction;
    }
    default_action = NoAction();
    size = 16;
}
Register<bit<32>,bit<32>>(VAL_BUCKET_COUNT) valhi9_reg;
RegisterAction<bit<32>, bit<32>, bit<32>>(valhi9_reg) valhi9_reg_get_alu = {
    void apply(inout bit<32> register_data, out bit<32> result) {
        result = register_data;
    }
};
RegisterAction<bit<32>, bit<32>, bit<32>>(valhi9_reg) valhi9_reg_set_alu = {
    void apply(inout bit<32> register_data){
        register_data = hdr.val9_hdr.valhi;
    }
};

action get_valhi9() {
    hdr.val9_hdr.valhi = valhi9_reg_get_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
}

action set_and_get_valhi9() {
    valhi9_reg_set_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
}
@pragma stage 2
table update_valhi9_tbl {
    key = {
        meta.access_val_mode: exact;
        hdr.inswitch_hdr.bitmap[8:8]: exact;
    }
    actions = {
        get_valhi9;
        set_and_get_valhi9;
        NoAction;
    }
    default_action = NoAction();
    size = 16;
}
Register<bit<32>,bit<32>>(VAL_BUCKET_COUNT) vallo10_reg;
RegisterAction<bit<32>, bit<32>, bit<32>>(vallo10_reg) vallo10_reg_get_alu = {
    void apply(inout bit<32> register_data, out bit<32> result) {
        result = register_data;
    }
};
RegisterAction<bit<32>, bit<32>, bit<32>>(vallo10_reg) vallo10_reg_set_alu = {
    void apply(inout bit<32> register_data){
        register_data = hdr.val10_hdr.vallo;
    }
};

action get_vallo10() {
    hdr.val10_hdr.vallo = vallo10_reg_get_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.val10_hdr.setValid();
}

action set_and_get_vallo10() {
    vallo10_reg_set_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.val10_hdr.setValid();
}
@pragma stage 7
table update_vallo10_tbl {
    key = {
        meta.access_val_mode: exact;
        hdr.inswitch_hdr.bitmap[9:9]: exact;
    }
    actions = {
        get_vallo10;
        set_and_get_vallo10;
        NoAction;
    }
    default_action = NoAction();
    size = 16;
}
Register<bit<32>,bit<32>>(VAL_BUCKET_COUNT) valhi10_reg;
RegisterAction<bit<32>, bit<32>, bit<32>>(valhi10_reg) valhi10_reg_get_alu = {
    void apply(inout bit<32> register_data, out bit<32> result) {
        result = register_data;
    }
};
RegisterAction<bit<32>, bit<32>, bit<32>>(valhi10_reg) valhi10_reg_set_alu = {
    void apply(inout bit<32> register_data){
        register_data = hdr.val10_hdr.valhi;
    }
};

action get_valhi10() {
    hdr.val10_hdr.valhi = valhi10_reg_get_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
}

action set_and_get_valhi10() {
    valhi10_reg_set_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
}
@pragma stage 7
table update_valhi10_tbl {
    key = {
        meta.access_val_mode: exact;
        hdr.inswitch_hdr.bitmap[9:9]: exact;
    }
    actions = {
        get_valhi10;
        set_and_get_valhi10;
        NoAction;
    }
    default_action = NoAction();
    size = 16;
}
Register<bit<32>,bit<32>>(VAL_BUCKET_COUNT) vallo11_reg;
RegisterAction<bit<32>, bit<32>, bit<32>>(vallo11_reg) vallo11_reg_get_alu = {
    void apply(inout bit<32> register_data, out bit<32> result) {
        result = register_data;
    }
};
RegisterAction<bit<32>, bit<32>, bit<32>>(vallo11_reg) vallo11_reg_set_alu = {
    void apply(inout bit<32> register_data){
        register_data = hdr.val11_hdr.vallo;
    }
};

action get_vallo11() {
    hdr.val11_hdr.vallo = vallo11_reg_get_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.val11_hdr.setValid();
}

action set_and_get_vallo11() {
    vallo11_reg_set_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.val11_hdr.setValid();
}
@pragma stage 8
table update_vallo11_tbl {
    key = {
        meta.access_val_mode: exact;
        hdr.inswitch_hdr.bitmap[10:10]: exact;
    }
    actions = {
        get_vallo11;
        set_and_get_vallo11;
        NoAction;
    }
    default_action = NoAction();
    size = 16;
}
Register<bit<32>,bit<32>>(VAL_BUCKET_COUNT) valhi11_reg;
RegisterAction<bit<32>, bit<32>, bit<32>>(valhi11_reg) valhi11_reg_get_alu = {
    void apply(inout bit<32> register_data, out bit<32> result) {
        result = register_data;
    }
};
RegisterAction<bit<32>, bit<32>, bit<32>>(valhi11_reg) valhi11_reg_set_alu = {
    void apply(inout bit<32> register_data){
        register_data = hdr.val11_hdr.valhi;
    }
};

action get_valhi11() {
    hdr.val11_hdr.valhi = valhi11_reg_get_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
}

action set_and_get_valhi11() {
    valhi11_reg_set_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
}
@pragma stage 8
table update_valhi11_tbl {
    key = {
        meta.access_val_mode: exact;
        hdr.inswitch_hdr.bitmap[10:10]: exact;
    }
    actions = {
        get_valhi11;
        set_and_get_valhi11;
        NoAction;
    }
    default_action = NoAction();
    size = 16;
}
Register<bit<32>,bit<32>>(VAL_BUCKET_COUNT) vallo12_reg;
RegisterAction<bit<32>, bit<32>, bit<32>>(vallo12_reg) vallo12_reg_get_alu = {
    void apply(inout bit<32> register_data, out bit<32> result) {
        result = register_data;
    }
};
RegisterAction<bit<32>, bit<32>, bit<32>>(vallo12_reg) vallo12_reg_set_alu = {
    void apply(inout bit<32> register_data){
        register_data = hdr.val12_hdr.vallo;
    }
};

action get_vallo12() {
    hdr.val12_hdr.vallo = vallo12_reg_get_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.val12_hdr.setValid();
}

action set_and_get_vallo12() {
    vallo12_reg_set_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.val12_hdr.setValid();
}
@pragma stage 8
table update_vallo12_tbl {
    key = {
        meta.access_val_mode: exact;
        hdr.inswitch_hdr.bitmap[11:11]: exact;
    }
    actions = {
        get_vallo12;
        set_and_get_vallo12;
        NoAction;
    }
    default_action = NoAction();
    size = 16;
}
Register<bit<32>,bit<32>>(VAL_BUCKET_COUNT) valhi12_reg;
RegisterAction<bit<32>, bit<32>, bit<32>>(valhi12_reg) valhi12_reg_get_alu = {
    void apply(inout bit<32> register_data, out bit<32> result) {
        result = register_data;
    }
};
RegisterAction<bit<32>, bit<32>, bit<32>>(valhi12_reg) valhi12_reg_set_alu = {
    void apply(inout bit<32> register_data){
        register_data = hdr.val12_hdr.valhi;
    }
};

action get_valhi12() {
    hdr.val12_hdr.valhi = valhi12_reg_get_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
}

action set_and_get_valhi12() {
    valhi12_reg_set_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
}
@pragma stage 8
table update_valhi12_tbl {
    key = {
        meta.access_val_mode: exact;
        hdr.inswitch_hdr.bitmap[11:11]: exact;
    }
    actions = {
        get_valhi12;
        set_and_get_valhi12;
        NoAction;
    }
    default_action = NoAction();
    size = 16;
}
Register<bit<32>,bit<32>>(VAL_BUCKET_COUNT) vallo13_reg;
RegisterAction<bit<32>, bit<32>, bit<32>>(vallo13_reg) vallo13_reg_get_alu = {
    void apply(inout bit<32> register_data, out bit<32> result) {
        result = register_data;
    }
};
RegisterAction<bit<32>, bit<32>, bit<32>>(vallo13_reg) vallo13_reg_set_alu = {
    void apply(inout bit<32> register_data){
        register_data = hdr.val13_hdr.vallo;
    }
};

action get_vallo13() {
    hdr.val13_hdr.vallo = vallo13_reg_get_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.val13_hdr.setValid();
}

action set_and_get_vallo13() {
    vallo13_reg_set_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.val13_hdr.setValid();
}

@pragma stage 5
table update_vallo13_tbl {
    key = {
        meta.access_val_mode: exact;
        hdr.inswitch_hdr.bitmap[12:12]: exact;
    }
    actions = {
        get_vallo13;
        set_and_get_vallo13;
        NoAction;
    }
    default_action = NoAction();
    size = 16;
}
Register<bit<32>,bit<32>>(VAL_BUCKET_COUNT) valhi13_reg;
RegisterAction<bit<32>, bit<32>, bit<32>>(valhi13_reg) valhi13_reg_get_alu = {
    void apply(inout bit<32> register_data, out bit<32> result) {
        result = register_data;
    }
};
RegisterAction<bit<32>, bit<32>, bit<32>>(valhi13_reg) valhi13_reg_set_alu = {
    void apply(inout bit<32> register_data){
        register_data = hdr.val13_hdr.valhi;
    }
};

action get_valhi13() {
    hdr.val13_hdr.valhi = valhi13_reg_get_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
}

action set_and_get_valhi13() {
    valhi13_reg_set_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
}

@pragma stage 4
table update_valhi13_tbl {
    key = {
        meta.access_val_mode: exact;
        hdr.inswitch_hdr.bitmap[12:12]: exact;
    }
    actions = {
        get_valhi13;
        set_and_get_valhi13;
        NoAction;
    }
    default_action = NoAction();
    size = 16;
}
Register<bit<32>,bit<32>>(VAL_BUCKET_COUNT) vallo14_reg;
RegisterAction<bit<32>, bit<32>, bit<32>>(vallo14_reg) vallo14_reg_get_alu = {
    void apply(inout bit<32> register_data, out bit<32> result) {
        result = register_data;
    }
};
RegisterAction<bit<32>, bit<32>, bit<32>>(vallo14_reg) vallo14_reg_set_alu = {
    void apply(inout bit<32> register_data){
        register_data = hdr.val14_hdr.vallo;
    }
};

action get_vallo14() {
    hdr.val14_hdr.vallo = vallo14_reg_get_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.val14_hdr.setValid();
}

action set_and_get_vallo14() {
    vallo14_reg_set_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.val14_hdr.setValid();
}

@pragma stage 6
table update_vallo14_tbl {
    key = {
        meta.access_val_mode: exact;
        hdr.inswitch_hdr.bitmap[13:13]: exact;
    }
    actions = {
        get_vallo14;
        set_and_get_vallo14;
        NoAction;
    }
    default_action = NoAction();
    size = 16;
}
Register<bit<32>,bit<32>>(VAL_BUCKET_COUNT) valhi14_reg;
RegisterAction<bit<32>, bit<32>, bit<32>>(valhi14_reg) valhi14_reg_get_alu = {
    void apply(inout bit<32> register_data, out bit<32> result) {
        result = register_data;
    }
};
RegisterAction<bit<32>, bit<32>, bit<32>>(valhi14_reg) valhi14_reg_set_alu = {
    void apply(inout bit<32> register_data){
        register_data = hdr.val14_hdr.valhi;
    }
};

action get_valhi14() {
    hdr.val14_hdr.valhi = valhi14_reg_get_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
}

action set_and_get_valhi14() {
    valhi14_reg_set_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
}
@pragma stage 6
table update_valhi14_tbl {
    key = {
        meta.access_val_mode: exact;
        hdr.inswitch_hdr.bitmap[13:13]: exact;
    }
    actions = {
        get_valhi14;
        set_and_get_valhi14;
        NoAction;
    }
    default_action = NoAction();
    size = 16;
}
Register<bit<32>,bit<32>>(VAL_BUCKET_COUNT) vallo15_reg;
RegisterAction<bit<32>, bit<32>, bit<32>>(vallo15_reg) vallo15_reg_get_alu = {
    void apply(inout bit<32> register_data, out bit<32> result) {
        result = register_data;
    }
};
RegisterAction<bit<32>, bit<32>, bit<32>>(vallo15_reg) vallo15_reg_set_alu = {
    void apply(inout bit<32> register_data){
        register_data = hdr.val15_hdr.vallo;
    }
};

action get_vallo15() {
    hdr.val15_hdr.vallo = vallo15_reg_get_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.val15_hdr.setValid();
}

action set_and_get_vallo15() {
    vallo15_reg_set_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.val15_hdr.setValid();
}
@pragma stage 10
table update_vallo15_tbl {
    key = {
        meta.access_val_mode: exact;
        hdr.inswitch_hdr.bitmap[14:14]: exact;
    }
    actions = {
        get_vallo15;
        set_and_get_vallo15;
        NoAction;
    }
    default_action = NoAction();
    size = 16;
}
Register<bit<32>,bit<32>>(VAL_BUCKET_COUNT) valhi15_reg;
RegisterAction<bit<32>, bit<32>, bit<32>>(valhi15_reg) valhi15_reg_get_alu = {
    void apply(inout bit<32> register_data, out bit<32> result) {
        result = register_data;
    }
};
RegisterAction<bit<32>, bit<32>, bit<32>>(valhi15_reg) valhi15_reg_set_alu = {
    void apply(inout bit<32> register_data){
        register_data = hdr.val15_hdr.valhi;
    }
};

action get_valhi15() {
    hdr.val15_hdr.valhi = valhi15_reg_get_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
}

action set_and_get_valhi15() {
    valhi15_reg_set_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
}

@pragma stage 10
table update_valhi15_tbl {
    key = {
        meta.access_val_mode: exact;
        hdr.inswitch_hdr.bitmap[14:14]: exact;
    }
    actions = {
        get_valhi15;
        set_and_get_valhi15;
        NoAction;
    }
    default_action = NoAction();
    size = 16;
}
Register<bit<32>,bit<32>>(VAL_BUCKET_COUNT) vallo16_reg;
RegisterAction<bit<32>, bit<32>, bit<32>>(vallo16_reg) vallo16_reg_get_alu = {
    void apply(inout bit<32> register_data, out bit<32> result) {
        result = register_data;
    }
};
RegisterAction<bit<32>, bit<32>, bit<32>>(vallo16_reg) vallo16_reg_set_alu = {
    void apply(inout bit<32> register_data){
        register_data = hdr.val16_hdr.vallo;
    }
};

action get_vallo16() {
    hdr.val16_hdr.vallo = vallo16_reg_get_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.val16_hdr.setValid();
}

action set_and_get_vallo16() {
    vallo16_reg_set_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
    hdr.val16_hdr.setValid();
}
@pragma stage 10
table update_vallo16_tbl {
    key = {
        meta.access_val_mode: exact;
        hdr.inswitch_hdr.bitmap[15:15]: exact;
    }
    actions = {
        get_vallo16;
        set_and_get_vallo16;
        NoAction;
    }
    default_action = NoAction();
    size = 16;
}
Register<bit<32>,bit<32>>(VAL_BUCKET_COUNT) valhi16_reg;
RegisterAction<bit<32>, bit<32>, bit<32>>(valhi16_reg) valhi16_reg_get_alu = {
    void apply(inout bit<32> register_data, out bit<32> result) {
        result = register_data;
    }
};
RegisterAction<bit<32>, bit<32>, bit<32>>(valhi16_reg) valhi16_reg_set_alu = {
    void apply(inout bit<32> register_data) {
        register_data = hdr.val16_hdr.valhi;
    }
};

action get_valhi16() {
    hdr.val16_hdr.valhi = valhi16_reg_get_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
}

action set_and_get_valhi16() {
    valhi16_reg_set_alu.execute((bit<32>)hdr.inswitch_hdr.idx);
}
@pragma stage 10
table update_valhi16_tbl {
    key = {
        meta.access_val_mode: exact;
        hdr.inswitch_hdr.bitmap[15:15]: exact;
    }
    actions = {
        get_valhi16;
        set_and_get_valhi16;
        NoAction;
    }
    default_action = NoAction();
    size = 16;
}
